import time
from pathlib import Path
import argparse
import pandas as pd
import pywikibot
import re

parser = argparse.ArgumentParser()
parser.add_argument("-l", "--limite", help="nombre maximum d'URIs Wikidata à modifier", type=int)
args = parser.parse_args()

RAPPORTCHEMIN = Path.cwd() / "rapports"

CTVCHEMIN = Path.cwd() / "donnees" / "genre_qc.csv"
GENDICT = {
    28: "P162", #Producteur = producteur ou productrice
    # 29: "P162", #Producteur délégué = producteur ou productrice
    # 48: "P162", #Producteur exécutif = producteur ou productrice
    # 8: "P2515", #Costumes = costumier
    # 31: "P58", #Scénario = scénariste
    # 32: "P58", #Scripte = scénariste
    # 15: "P161", #Interprétation = distribution
    # 19: "P1040" #Montage images = monteur ou monteuse
}


class GeneriqueTriplet:
    def __init__(self, sujqid: str, predquid: str, objquid: str):
        self.sujqid = sujqid
        self.predquid = predquid
        self.objquid = objquid


def main() -> None:
    #Charget et nettoyer tous les triplets oeuvre-fonction-personne de CinéTV
    ctvdf = pd.read_csv(CTVCHEMIN, sep=";")
    ctvdf = nettoyerctv(ctvdf)

    wdt_cmtqId = chargerwdturi()

    ctvdf = pd.merge(wdt_cmtqId, ctvdf, on="FilmoId")
    ctvdf = ctvdf.drop(["FilmoId", "TitreOriginal"], axis=1)

    wdt_persId = chargerwdtpers()

    ctvdf = pd.merge(wdt_persId, ctvdf, on="NomID")
    ctvdf = ctvdf.drop(["NomID", "Nom"], axis=1)

    ctvdf["FonctionID"] = ctvdf["FonctionID"].apply(lambda x: GENDICT[x])
    ctvdf = ctvdf.drop_duplicates()

    ctvdf["persQid"] = ctvdf["persQid"].apply(lambda x: re.search(r"Q.+$", x).group().strip())
    ctvdf["OeuvreQid"] = ctvdf["OeuvreQid"].apply(lambda x: re.search(r"Q.+$", x).group().strip())

    if args.limite: #Limiter le nombre d'URIs à modifier
        ctvdf = ctvdf.head(args.limite) 

    triplets = creertriplets(ctvdf)

    repo = creerrepo()

    changelog = verserdonnees(triplets, repo)
    
    creerrapport(changelog, RAPPORTCHEMIN)


def nettoyerctv(df:pd.DataFrame) -> pd.DataFrame:
    df = df[df["FonctionID"].isin(GENDICT)]

    df.loc[df['NomID'].isna(), 'NomID'] = df['OrganismeID']
    df.loc[df['Nom'].isna(), 'Nom'] = df['dbo_Sujet_Terme']

    df = df[df["NomID"].notna()]
    df = df.astype({"NomID": "int64"})

    df = df.drop(["OrganismeID", "dbo_Sujet_Terme"], axis=1)

    df = df.rename(columns={'FilmoID': "FilmoId"})
    
    return df


def chargerwdturi() -> pd.DataFrame:
    #Charger le mapping FilmoID-WdtURI
    outdf = pd.read_csv("../wdt_cmtqId.csv")
    outdf = outdf[outdf["FilmoId"].str.isnumeric()]
    outdf = outdf.astype({"FilmoId": "int64"})
    outdf = outdf.rename(columns={'LienWikidata': "OeuvreQid"})

    return outdf


def chargerwdtpers() -> pd.DataFrame:
    #Charger le mapping FilmoID-WdtURI
    outdf = pd.read_csv("../wdt_cmtq-pers.csv")
    outdf = outdf.astype({"cmtqID": "int64"})
    outdf = outdf.rename(columns={'cmtqID': "NomID"})

    return outdf


def creerrepo() -> pywikibot.DataSite:
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    return repo    


def ajout_source(decl: pywikibot.Claim, repo: pywikibot.DataSite) -> None:
    ref = pywikibot.Claim(repo, u'P248')
    ref.setTarget(pywikibot.ItemPage(repo, 'Q41001657'))

    decl.addSource(ref, bot=True)


def ajout_declaration(itm: pywikibot.ItemPage, pred: str, cible: str, repo: pywikibot.DataSite)  -> None:
    claim = pywikibot.Claim(repo, pred)
    target = pywikibot.ItemPage(repo, cible)
    claim.setTarget(target)
    
    ajout_source(claim, repo)
    itm.addClaim(claim, bot=True)


def creertriplets(mapping: pd.DataFrame) -> list[GeneriqueTriplet]:
    return [
        GeneriqueTriplet(ligne["OeuvreQid"], ligne["FonctionID"], ligne["persQid"])
        for idx, ligne
        in mapping.iterrows()
    ]


def creerrapport(df: pd.DataFrame, chemin:Path) -> None:
    timestr = time.strftime("%Y%m%d-%H%M%S")

    if not chemin.exists():
        chemin.mkdir()
    
    df.to_excel(chemin / f"geneirique_modifications-{timestr}.xlsx", index=False)


def verserdonnees(triplets: list[GeneriqueTriplet], repo: pywikibot.DataSite) -> pd.DataFrame :
    changelog = pd.DataFrame()
    for trip in triplets:
        modif = False
        item = pywikibot.ItemPage(repo, trip.sujqid)

        try:
            decldict = {
                claim.getTarget().getID(): claim
                for claim in item.claims[trip.predquid]
            }

        except: #Oeuvre n'a pas de déclaration avec pred
            print(f"pas de {trip.predquid} dans {trip.sujqid}")
            ajout_declaration(item, trip.predquid, trip.objquid, repo)
            modif = True

            continue

        if trip.objquid in decldict: #Triplet existe déjà
            try:
                print(f"{trip.objquid} déjà cible de {trip.predquid} dans {trip.sujqid}")
                ajout_source(decldict[trip.objquid], repo)
                modif = True
            
            except:
                print("Déclaration déjà référencée")

                continue

        else: #Pred existe, mais pas avec cet objet
            print(f"{trip.objquid} pas {trip.predquid} dans {trip.sujqid}")
            ajout_declaration(item, trip.predquid, trip.objquid, repo)
            modif = True

        if modif == True:
            outdf = pd.DataFrame(data=[{
                "oeuvre": trip.sujqid,
                "relation": trip.predquid,
                "objet": trip.objquid,
                "statut": "modifié"
            }])

        else:
            outdf = pd.DataFrame(data=[{
                "oeuvre": trip.sujqid,
                "relation": trip.predquid,
                "objet": trip.objquid,
                "statut": "non modifié"
            }])

        changelog = pd.concat([changelog, outdf])
    
    return changelog


if __name__ == "__main__":
    main()