#!/usr/local/bin/python3
"""
Ajouter des déclarations à des oeuvres cinématographiques sur Wikidata.
"""

import time
from pathlib import Path
import argparse
import pandas as pd
import pywikibot
import re
import pydash
import requests

RAPPORTCHEMIN = Path.cwd() / "rapports"

CTVCHEMIN = Path.cwd() / "donnees"
MAPPING_FILMOID = Path("../mapping/oeuvres-wdtmapping.csv")
MAPPING_PERSONNEID = Path("../mapping/personnes-wdtmapping.csv")
FONCTION_MAP = Path("donnees/fonction_map.csv")


class GeneriqueTriplet:
    """Triplet des trois éléments d'une déclarations RDF"""
    qualDict = pd.read_csv("donnees/qualifications.csv")
    qualDict = qualDict[qualDict["Pred"].notna()]
    qualDict = dict(zip(qualDict["Pred"], qualDict["objrole"]))

    def __init__(
        self, 
        sujqid: str, 
        predquid: str, 
        objquid: str, 
        repo: pywikibot.DataSite,
        qualDict = qualDict
    ):
        self.sujqid = sujqid
        self.predquid = predquid
        self.objquid = objquid

        self.ajoutee = False
        self.referencee = False

        self.fonctqual = qualDict.get(self.predquid, None)

        self.item = pywikibot.ItemPage(repo, self.sujqid)

    def declaration_existante(self) -> pywikibot.Claim | None:
        """Retourner la déclaration Wikidata avec les mêmes sujet-prédicat-objet si une existe."""
        try:
            decls_avec_pred = {
                claim.getTarget().getID(): claim
                for claim in self.item.claims[self.predquid]
            }
            return decls_avec_pred[self.objquid]
            
        except:
            return None
        


def main() -> None:
    # timestr = time.strftime("%Y%m%d-%H%M%S")
    parser = argparse.ArgumentParser()

    parser.add_argument("source", help="Nom du fichier source dans /donnees", type=str)
    parser.add_argument("-l", "--limite", help="Nombre maximum d'URIs Wikidata à modifier", type=int)
    parser.add_argument("-q", "--qid", help="Modifier un seul Qid (exclusif avec --limite)", type=str)

    args = parser.parse_args()

    if not RAPPORTCHEMIN.exists():
        RAPPORTCHEMIN.mkdir()

    gendict = pd.read_csv(FONCTION_MAP)
    gendict = dict(zip(gendict["FonctionID"], gendict["WdtID"]))

    #Charget et nettoyer tous les triplets oeuvre-fonction-personne de CinéTV
    source_chemin = CTVCHEMIN / args.source

    generique_cmtq = pd.read_csv(source_chemin, sep=";")
    generique_cmtq = nettoyerctv(generique_cmtq, gendict)


    #Associer FilmoID et Wikidata URI
    oeuvres_wdt = chargerwdturi(MAPPING_FILMOID)

    generique_cmtq = pd.merge(oeuvres_wdt, generique_cmtq, on="FilmoId")

    #Associer NomID et Wikidata URI
    personnes_wdt = chargerwdtpers(MAPPING_PERSONNEID)

    generique_cmtq = pd.merge(personnes_wdt, generique_cmtq, on="NomID")

    #Associer FonctionID et Wikidata URI
    generique_cmtq["FonctionID"] = generique_cmtq["FonctionID"].apply(lambda x: gendict[x])
    generique_cmtq = generique_cmtq.drop_duplicates()

    generique_cmtq["persQid"] = generique_cmtq["persQid"].apply(simplifierQid)
    generique_cmtq["OeuvreQid"] = generique_cmtq["OeuvreQid"].apply(simplifierQid)

    # TODO: option, créer ce rapport
    # creerrapport(generique_cmtq, "test", timestr)

    # exit()

    repo = creerrepo()

    if args.qid:
        generique_cmtq = generique_cmtq[generique_cmtq["OeuvreQid"] == args.qid]

        triplets = creertriplets(generique_cmtq, repo)

        print(len(triplets))

        verserdonnees(triplets, repo, args.source)

        supprimer_doublons(triplets)

        exit()

    if args.limite: #Limiter le nombre d'URIs à modifier
        generique_cmtq = generique_cmtq.head(args.limite) 

    triplets = creertriplets(generique_cmtq, repo)
    
    start = input(f"Ce script créera un maximum de {len(triplets)} déclarations sur Wikidata. Continuer? [o]ui/[N]on? ")

    if not start.lower().startswith("o"):
        exit()

    verserdonnees(triplets, repo, args.source)

    supprimer_doublons(triplets)

def nettoyerctv(df:pd.DataFrame, gendict: dict) -> pd.DataFrame:
    """Éliminer les lignes suplerflues, renommer des colonnes"""
    df = df[df["FonctionID"].isin(gendict)]

    df = df[df["NomID"].notna()]
    df = df.astype({"NomID": "int64"})

    df = df.rename(columns={'FilmoID': "FilmoId"})
    
    return df


def chargerwdturi(chemin : Path) -> pd.DataFrame:
    #Charger le mapping FilmoID-WdtURI
    outdf = pd.read_csv(chemin)
    outdf = outdf.astype({"FilmoId": str})
    outdf = outdf[outdf["FilmoId"].str.isnumeric()]
    outdf = outdf.astype({"FilmoId": "int64"})
    outdf = outdf.rename(columns={'LienWikidata': "OeuvreQid"})

    return outdf


def chargerwdtpers(chemin : Path) -> pd.DataFrame:
    #Charger le mapping FilmoID-WdtURI
    outdf = pd.read_csv(chemin)
    outdf = outdf.astype({"cmtqID": "int64"})
    outdf = outdf.rename(columns={'cmtqID': "NomID"})

    return outdf

def simplifierQid(qid:str) -> str:
    """Éliminer l'URI avant le QID"""
    return re.search(r"Q.+$", qid).group().strip()

def creerrepo() -> pywikibot.DataSite:
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    return repo    


def ajout_source(decl: pywikibot.Claim, repo: pywikibot.DataSite) -> None:
    """Ajouter une source, Affirmé dans (P248) CinéTV (Q41001657)"""
    ref = pywikibot.Claim(repo, u'P248')
    ref.setTarget(pywikibot.ItemPage(repo, 'Q41001657'))

    decl.addSource(ref, bot=True)


def ajout_declaration(itm: pywikibot.ItemPage, pred: str, cible: str, repo: pywikibot.DataSite)  -> None:
    claim = pywikibot.Claim(repo, pred)
    target = pywikibot.ItemPage(repo, cible)
    claim.setTarget(target)
    
    ajout_source(claim, repo)
    itm.addClaim(claim, bot=True)


def creertriplets(mapping: pd.DataFrame, repo: pywikibot.DataSite) -> list[GeneriqueTriplet]:
    return [
        GeneriqueTriplet(ligne["OeuvreQid"], ligne["FonctionID"], ligne["persQid"], repo)
        for idx, ligne
        in mapping.iterrows()
    ]

def animation_bool(trip: GeneriqueTriplet):
    APIURL = 'https://query.wikidata.org/sparql'
    HEADERS = {
        'User-Agent': 'CMTQBot/0.1 (gsides@cinematheque.qc.ca)'
    }
    
    sparql =f"""
    SELECT *
    WHERE {{
        ?oeuvre wdt:P31/wdt:P279* wd:Q202866.
        BIND (wd:{trip.sujqid} AS ?oeuvre)
    }}
    """

    r = requests.get(
        APIURL,
        params={'format': 'json', 'query': sparql},
        headers=HEADERS
    )

    data = r.json()

    dataframe = pd.DataFrame.from_dict(
        [x for x in pydash.get(data, 'results.bindings')])

    for k in list(dataframe.columns.values):
        def extract_value(row, col):
            return (pydash.get(row[col], 'value'))
        dataframe[k] = dataframe.apply(extract_value, col=k, axis=1)

    return False if dataframe.empty else True

def verserdonnees(triplets: list[GeneriqueTriplet], repo: pywikibot.DataSite, source: str) -> None:
    timestr = time.strftime("%Y%m%d-%H%M%S")
    source = Path(source).stem

    rapport_chemin = RAPPORTCHEMIN / f"generique_modifications-{source}-{timestr}.csv"

    with open(rapport_chemin, "w", encoding="utf-8") as outfile:
        outfile.write(f"oeuvre,relation,objet,triplet_ajoute,source_ajoutee")

    for idx, trip in enumerate(triplets):
        if (idx + 1) % 10 == 0:
            print(f"{idx+1} triplets traités ({round(((idx+1)/len(triplets)*100))}%)")
        
        est_animation = animation_bool(trip)
        if est_animation and trip.predquid == "P161":
            trip.predquid = "P725"

        declaration_existante = trip.declaration_existante()

        # print(trip.sujqid, trip.predquid, trip.objquid)

        if not declaration_existante: #Bon prédicat absent; bon prédicat présent mais pas le bon objet
            ajout_declaration(trip.item, trip.predquid, trip.objquid, repo)
            # print("Bon prédicat absent; bon prédicat présent mais pas le bon objet")
            trip.ajoutee = True
            trip.referencee = True

        else: #Prédicat avec bon objet existe déjà
            declaration_json = declaration_existante.toJSON()
            references = declaration_json.get("references", None)

            if references: #Déclaration sourcée
                reference_cibles = list()

                for reference in references:
                    sources_affirme_dans = pydash.get(reference, "snaks.P248", None)

                    if sources_affirme_dans:
                        for cible in sources_affirme_dans:
                            reference_cibles.append("Q" + str(pydash.get(cible, "datavalue.value.numeric-id", None)))
    
                if "Q41001657" in reference_cibles:
                    # print("Rien ajouté")
                    pass

                else: #Aucune source du bon type, bon type mais pas bonne cible
                    # print("Aucune source du bon type, bon type mais pas bonne cible")
                    ajout_source(declaration_existante, repo)
                    trip.referencee = True
                    
            else: #Déclaration non sourcée
                # print("Déclaration non sourcée")
                ajout_source(declaration_existante, repo)
                trip.referencee = True
        
        if trip.referencee == True:
            with open(rapport_chemin, "a") as outfile:
                outfile.write(f"\n{trip.sujqid},{trip.predquid},{trip.objquid},{trip.ajoutee},{trip.referencee}")


def supprimer_doublons(trips: list[GeneriqueTriplet]):
    """Supprimer des déclarations doublons du type P3092, membre de l'équipe du film, ou P161, distribution."""
    trips = [t for t in trips if t.ajoutee == True]

    print(f"Suppression des doublons parmi les {len(trips)} déclarations ajoutées/modifiées")

    for trip in trips:
        item_claims = trip.item.get()["claims"] 

        if "P3092" in item_claims:
            for claim in trip.item.claims['P3092']: #Finds all statements (P3092, membre de l'équipe du film)
                if claim.getTarget().getID() == trip.objquid: #Looks for claims with same object as triple
                    claimjson = claim.toJSON()

                    for qualif in pydash.get(claimjson, "qualifiers.P3831"): #Finds all qualifications P3831 rôle de l'objet 
                        # print("Q" + str(pydash.get(qualif, "datavalue.value.numeric-id")))
                        if trip.fonctqual == ("Q" + str(pydash.get(qualif, "datavalue.value.numeric-id"))): #Checks that qualification object matches function Pid in added triplet
                            if len(pydash.get(claimjson, "qualifiers.P3831")) > 2: #Ne supprime pas s'il y a plusieurs rôles
                                print(f"Remove {claim.getID()}")
                                trip.item.removeClaims(claim, summary=u"Removing redundant claim, more specific predicate available.")
                            else:
                                print(f"Multiples rôles : {trip.sujqid}, {trip.predquid}, {trip.objquid} : {trip.fonctqual}")
        
        if trip.predquid == "P11108": #triplets avec recorded participant
            if "P161" in trip.item.get()["claims"]: #item a déclarations "distribution"
                for claim in trip.item.claims['P161']:
                    if claim.getTarget().getID() == trip.objquid: #Déclaration "distribution" avec le même objet
                        print(f"Remove {claim.getID()}")
                        trip.item.removeClaims(claim, summary=u"Removing redundant claim, more specific predicate available.")

                        break


if __name__ == "__main__":
    main()