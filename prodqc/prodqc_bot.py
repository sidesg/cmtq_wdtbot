#!/usr/local/bin/python3
"""
Ce bot associe des oeuvres cinématographiques au Québec en tant que lieu de création.
Le bot dépend d'un export de CinéTV, PRODS_QC, qui contient des oeuvres identifées comme
ayant été produites au Québec.
"""

import re
import time
from pathlib import Path
import argparse
import pandas as pd
import pywikibot
import pydash

# timestr = time.strftime("%Y%m%d-%H%M%S")


PRODS_QC = Path.cwd() / "cinetv_prodqc"
MAPPING_FILMOID = Path("../mapping/oeuvres-wdtmapping.csv")

FIMLOCOLS = [
    "Numéro séquentiel", 
    "Titre original"
]
CANADAURI = "http://www.wikidata.org/entity/Q16"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fichier", help="charger un fichier source de CinéTV plutôt que le dossier 'rapports' dans son ensemble", type=str)
    parser.add_argument("-l", "--limite", help="nombre maximum d'URIs Wikidata à modifier", type=int)
    parser.add_argument("-q", "--qid", help="modifier un seul Qid (exclusif avec --limite)", type=str)

    args = parser.parse_args()

    repo = creerrepo()

    if args.qid:
        qid = pd.DataFrame(columns=["LienWikidata"], data=[args.qid])

        ajoutdeclarations(qid, repo)

        exit()      

    qcproddf = chargerctv(PRODS_QC, args.fichier)
    qcproddf = nettoyerctv(qcproddf)

    wdt_cmtqId = chargerwdturi(MAPPING_FILMOID)

    qcprodWdt = pd.merge(wdt_cmtqId, qcproddf, on="FilmoId")

    if args.limite: #Limiter le nombre d'URIs à modifier
        qcprodWdt = qcprodWdt.head(args.limite) 

    start = input(f"Ce script modifiera {len(qcprodWdt)} notices sur Wikidata. Continuer? [o]ui/[N]on? ")

    if not start.lower().startswith("o"):
        exit()

    qcprodWdt["LienWikidata"] = qcprodWdt["LienWikidata"].apply(lambda x: re.search(r"Q.+$", x).group().strip())

    ajoutdeclarations(qcprodWdt, repo)


def chargerctv(chemin : Path, fichier: str) -> pd.DataFrame:
    """Charger les données de l'export CinéTV"""

    if fichier:
        outdf = pd.read_csv(
            chemin / fichier,
            usecols=FIMLOCOLS,
            sep="\t",
            encoding="cp1252"
        )

    else:
        outdf = pd.DataFrame()

        file_list = [f for f in chemin.iterdir() if f.suffix == ".tsv"] 

        outdf = pd.DataFrame()

        #TODO: make flexible to accomodate csv and xlsx
        for file_path in file_list:
            indf = pd.read_csv(
                file_path,
                usecols=FIMLOCOLS,
                sep="\t",
                encoding="cp1252"
            )
        
            outdf = pd.concat([outdf, indf])

    return outdf

def nettoyerctv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df = df.rename(columns={'Numéro séquentiel': "FilmoId"})
    df = df[df["FilmoId"].notnull()]

    df["FilmoId"] = df["FilmoId"].astype(str)
    df = df[df["FilmoId"].str.isnumeric()]
    df = df.astype({"FilmoId": "int64"})

    return df

def chargerwdturi(chemin : Path) -> pd.DataFrame:
    #Charger le mapping FilmoID-WdtURI
    outdf = pd.read_csv(chemin)
    outdf = outdf[outdf["FilmoId"].str.isnumeric()]
    outdf = outdf.astype({"FilmoId": "int64"})

    return outdf

def creerrepo() -> pywikibot.DataSite: #Connecter à Wikidata
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    return repo

def ajout_qualification(decl: pywikibot.Claim, repo: pywikibot.DataSite) -> None:
    qualifier = pywikibot.Claim(repo, u'P131')
    target = pywikibot.ItemPage(repo, "Q176")
    qualifier.setTarget(target)

    decl.addQualifier(qualifier, bot=True)
    ajout_source(decl, repo=repo)

def ajout_declaration(itm: pywikibot.ItemPage, repo: pywikibot.DataSite) -> None:
    claim = pywikibot.Claim(repo, u'P495')
    target = pywikibot.ItemPage(repo, 'Q16')
    claim.setTarget(target)

    ajout_qualification(claim, repo=repo)
    
    itm.addClaim(claim, bot=True)

def ajout_source(decl: pywikibot.Claim, repo: pywikibot.DataSite) -> None:
    ref = pywikibot.Claim(repo, u'P248')
    ref.setTarget(pywikibot.ItemPage(repo, 'Q41001657'))

    decl.addSource(ref, bot=True)

def ajoutdeclarations(mapping: pd.DataFrame, repo: pywikibot.DataSite) -> None:
    timestr = time.strftime("%Y%m%d-%H%M%S")

    rapport_chemin = f"rapports/prodqc_rapport-{timestr}.csv"

    with open(rapport_chemin, "w", encoding="utf-8") as outfile:
        outfile.write(f"qid,modification")   

    for idx, row in mapping.iterrows():
        changed = False
        qid = row["LienWikidata"]

        item = pywikibot.ItemPage(repo, qid)

        item_dict = item.get()  # Get the item dictionary
        clm_dict = item_dict["claims"]  # Get the claim dictionary

        if "P495" in clm_dict: #Si l'URI a "pays d'origine" parmi ses déclarations
            canada_claims = [
                claim
                for claim in item.claims['P495']
                if claim.getTarget().concept_uri() == CANADAURI
            ]

            if len(canada_claims) > 0:
                for claim in canada_claims:
                    if 'P131' not in claim.qualifiers: #Si la déclaration n'a pas de qualification "localisation administrative"
                        ajout_qualification(claim, repo)
                        changed = True

                    else: #Oui qualif "localisation administrative"
                        claimjson = claim.toJSON()

                        qcquals = [
                            qualif
                            for qualif in pydash.get(claimjson, "qualifiers.P131")
                            if "Q" + str(pydash.get(qualif, "datavalue.value.numeric-id")) == "Q176"
                        ]

                        if len(qcquals) == 0:
                            ajout_qualification(claim, repo)
                            changed = True

                        else:
                            continue

                if len(canada_claims) > 1:
                    print(f"Multiples 'pays d'origine', {qid}.")
  

            elif len(canada_claims) == 0: #Oui "pays d'origine", mais cible != Canada
                ajout_declaration(item, repo)
                changed = True

        else: #Aucune déclaration "pays d'origine"
            ajout_declaration(item, repo)
            changed = True

    
        if changed == True:
            with open(rapport_chemin, "a") as outfile:
                outfile.write(f"\n{qid},{changed}")
  


if __name__ == "__main__":
    main()