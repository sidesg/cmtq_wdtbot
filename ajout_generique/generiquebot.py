#!/usr/local/bin/python3

"""
Ajouter des déclarations à des oeuvres cinématographiques sur Wikidata.
TODO: 
    * With oeuvre-person-role mapping, ID oeuvres associated multiple times with same person > ID cases where person associated with incorrect property.
    * Load role mapping from csv, add to GeneriqueTriplet
    * Improve duplicate reference handling (don't rely on hash)

"""

import time
from pathlib import Path
import argparse
import pandas as pd
import pywikibot
import re
import pydash

import pprint
import json

RAPPORTCHEMIN = Path.cwd() / "rapports"

CTVCHEMIN = Path.cwd() / "donnees" / "gen_real_qc.csv"
MAPPING_FILMOID = Path("../mapping/oeuvres-wdtmapping.csv")
MAPPING_PERSONNEID = Path("../mapping/personnes-wdtmapping.csv")
FONCTION_MAP = Path("donnees/fonction_map.csv")

timestr = time.strftime("%Y%m%d-%H%M%S")


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

        self.fonctqual = qualDict.get(self.predquid, None)

        self.ajoutee = False

        self.item = pywikibot.ItemPage(repo, self.sujqid)

    def declaration_existante(self) -> pywikibot.Claim:
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
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--limite", help="nombre maximum d'URIs Wikidata à modifier", type=int)
    parser.add_argument("-q", "--qid", help="modifier un seul Qid (exclusif avec --limite)", type=str)

    args = parser.parse_args()

    if not RAPPORTCHEMIN.exists():
        RAPPORTCHEMIN.mkdir()

    gendict = pd.read_csv(FONCTION_MAP)
    gendict = dict(zip(gendict["FonctionID"], gendict["WdtID"]))

    #Charget et nettoyer tous les triplets oeuvre-fonction-personne de CinéTV
    generique_cmtq = pd.read_csv(CTVCHEMIN, sep=";")
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
    # creerrapport(generique_cmtq, "test")

    # exit()

    repo = creerrepo()

    if args.qid:
        generique_cmtq = generique_cmtq[generique_cmtq["OeuvreQid"] == args.qid]

        triplets = creertriplets(generique_cmtq, repo)

        print(len(triplets))

        verserdonnees(triplets, repo)

        supprimer_doublons(triplets)
        
        changelog = creer_changelog(triplets)
    
        creerrapport(changelog, "generique_modifications")

        exit()

    if args.limite: #Limiter le nombre d'URIs à modifier
        generique_cmtq = generique_cmtq.head(args.limite) 

    triplets = creertriplets(generique_cmtq, repo)

    
    start = input(f"Ce script créera {len(triplets)} déclarations sur Wikidata. Continuer? [o]ui/[N]on? ")

    if not start.lower().startswith("o"):
        exit()

    verserdonnees(triplets, repo)

    supprimer_doublons(triplets)

    changelog = creer_changelog(triplets)
    
    creerrapport(changelog, "generique_modifications")


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
    """Ajouter une source, Affirmé dans (P248) CinéTC (Q41001657)"""
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


def verserdonnees(triplets: list[GeneriqueTriplet], repo: pywikibot.DataSite) -> None:
    for trip in triplets:
        print(f"{trip.sujqid}, {trip.predquid}, {trip.objquid}")

        declaration_existante = trip.declaration_existante()

        if not declaration_existante: #Bon prédicat absent; bon prédicat présent mais pas le bon objet
            print(f"Créer triplet: {trip.sujqid}, {trip.predquid}, {trip.objquid}")
            ajout_declaration(trip.item, trip.predquid, trip.objquid, repo)
            trip.ajoutee = True

        else: #Prédicate avec bon objet existe déjà
            references_json = declaration_existante.toJSON().get("references", None)

            if references_json:
                reference_cibles = list()

                for reference in references_json:
                    sources_affirme_dans = pydash.get(reference, "snaks.P248", None)

                    if sources_affirme_dans:
                        for cible in sources_affirme_dans:
                            reference_cibles.append("Q" + str(pydash.get(cible, "datavalue.value.numeric-id", None)))
    
                if "Q41001657" in reference_cibles:
                    print("Rien ajouté")

                else: #Aucune source du bon type, bon type mais pas bonne cible
                    print(f"Ajouter source: {trip.sujqid}, {trip.predquid}, {trip.objquid}")
                    ajout_source(declaration_existante, repo)
                    trip.ajoutee = True
                    
            else: #Déclaration non sourcée
                print(f"Ajouter source: {trip.sujqid}, {trip.predquid}, {trip.objquid}")
                ajout_source(declaration_existante, repo)
                trip.ajoutee = True


def creer_changelog(trips: list[GeneriqueTriplet]) -> pd.DataFrame:
    changelog = pd.DataFrame()

    for trip in trips:
        if trip.ajoutee == True:            
            outdf = pd.DataFrame(data=[{
                "oeuvre": trip.sujqid,
                "relation": trip.predquid,
                "objet": trip.objquid,
                "statut": "modifié"
            }])

        elif trip.ajoutee == False:
            outdf = pd.DataFrame(data=[{
                "oeuvre": trip.sujqid,
                "relation": trip.predquid,
                "objet": trip.objquid,
                "statut": "non modifié"
            }])

        changelog = pd.concat([changelog, outdf])
    
    return changelog

def creerrapport(df: pd.DataFrame, nom:str) -> None:
    """Créer un rapport tabulaire dans le dossier `rapports`"""    
    df.to_excel(RAPPORTCHEMIN / f"{nom}-{timestr}.xlsx", index=False)


def supprimer_doublons(trips: list[GeneriqueTriplet]):
    trips = [t for t in trips if t.ajoutee == True]

    for trip in trips:
        # print(trip.sujqid, trip.predquid, trip.objquid)
        # print(trip.fonctqual)

        item_claims = trip.item.get()["claims"] 

        if "P3092" in item_claims:
            for claim in trip.item.claims['P3092']: #Finds all statements (P3092, membre de l'équipe du film)
                if claim.getTarget().getID() == trip.objquid: #Looks for claims with same object as triple
                    claimjson = claim.toJSON()

                    for qualif in pydash.get(claimjson, "qualifiers.P3831"): #Finds all qualifications P3831 rôle de l'objet 
                        print("Q" + str(pydash.get(qualif, "datavalue.value.numeric-id")))
                        if trip.fonctqual == ("Q" + str(pydash.get(qualif, "datavalue.value.numeric-id"))): #Checks that qualification object matches function Pid in added triplet
                            print(f"Remove {claim.getID()}")
                            trip.item.removeClaims(claim, summary=u"Removing redundant claim")



if __name__ == "__main__":
    main()