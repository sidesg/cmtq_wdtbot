#!/usr/local/bin/python3

"""
Ce bot associe des oeuvres cinématographiques au Québec en tant que lieu de création.
Le bot dépend d'un export de CinéTV, QCPRODS, qui contient des oeuvres identifées comme
ayant été produites au Québec.
"""

import re
import time
from pathlib import Path
import argparse
import pandas as pd
import pywikibot

timestr = time.strftime("%Y%m%d-%H%M%S")

parser = argparse.ArgumentParser()
parser.add_argument("-l", "--limite", help="nombre maximum d'URIs Wikidata à modifier", type=int)
args = parser.parse_args()

QCPRODS = Path.cwd() / "cinetv_prodqc"
FIMLOCOLS = [
    "Numéro séquentiel", 
    "Titre original"
]
CANADAURI = "http://www.wikidata.org/entity/Q16"

def main():
    qcproddf = chargerctv()
    qcproddf = nettoyerctv(qcproddf)

    wdt_cmtqId = chargerwdturi()

    qcprodWdt = pd.merge(wdt_cmtqId, qcproddf, on="FilmoId")

    print(f'{len(qcprodWdt)} oeuvres à modifier sur Wikidata.')
    
    if args.limite:
        qcprodWdt = qcprodWdt.head(args.limite) #Limiter le nombre d'URIs à modifier

    start = input(f"Ce script modifiera {len(qcprodWdt)} notices sur Wikidata. Continuer? [o]ui/[N]on? ")

    if not start.lower().startswith("o"):
        exit()

    qcprodWdt["LienWikidata"] = qcprodWdt["LienWikidata"].apply(lambda x: re.search(r"Q.+$", x).group().strip())

    repo = creerrepo()

    ajoutdeclarations(qcprodWdt, repo)


def chargerctv() -> pd.DataFrame:
    #Charger les oeuvres produites au QC à partir d'export(s) CinéTV
    file_list = [f for f in QCPRODS.iterdir() if f.suffix == ".tsv"] 

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
    df = df[df["FilmoId"].str.isnumeric()]
    df = df.astype({"FilmoId": "int64"})

    return df

def chargerwdturi() -> pd.DataFrame:
    #Charger le mapping FilmoID-WdtURI
    outdf = pd.read_csv("../wdt_cmtqId.csv")
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
    ajout_source(decl)

def ajout_declaration(itm: pywikibot.ItemPage, repo: pywikibot.DataSite) -> None:
    claim = pywikibot.Claim(repo, u'P495')
    target = pywikibot.ItemPage(repo, 'Q16')
    claim.setTarget(target)

    ajout_qualification(claim)
    
    itm.addClaim(claim, bot=True)

def ajout_source(decl: pywikibot.Claim, repo: pywikibot.DataSite) -> None:
    ref = pywikibot.Claim(repo, u'P248')
    ref.setTarget(pywikibot.ItemPage(repo, 'Q41001657'))

    decl.addSource(ref, bot=True)

def ajoutdeclarations(mapping: pd.DataFrame, repo: pywikibot.DataSite) -> None:
    err_quids = []
    modif_qids = []

    rapportdf = pd.DataFrame()

    for idx, row in mapping.iterrows():
        changed = False
        qid = row["LienWikidata"]
        filmoid = row["FilmoId"]

        try:
            item = pywikibot.ItemPage(repo, qid)

            item_dict = item.get()  # Get the item dictionary
            clm_dict = item_dict["claims"]  # Get the claim dictionary

            if "P495" in clm_dict: #Si l'URI a "pays d'origine" parmi ses déclarations
                pays_cibles = [claim.getTarget().concept_uri() for claim in item.claims['P495']]
                if CANADAURI in pays_cibles:
                    for claim in item.claims['P495']: 
                        if claim.getTarget().concept_uri() == CANADAURI:# and 'P131' not in claim.qualifiers:
                            if 'P131' not in claim.qualifiers: #Si la déclaration n'a pas de qualification "localisation administrative"
                                ajout_qualification(claim, repo)
                                changed = True
                            else:
                                try:
                                    ajout_source(claim, repo)
                                    changed = True
                                except:
                                    continue                                                       
                else:
                    ajout_declaration(item, repo)
                    changed = True

            else:
                ajout_declaration(item, repo)
                changed = True
    
        except:
            print(f"Erreur, qid {qid}")
    
        if changed == False:
            err_quids.append(qid)
            outdf = pd.DataFrame(data=[{
                "qid": qid,
                "filmoid": filmoid,
                "statut": "non modifié"
            }])
        elif changed == True:
            modif_qids.append(qid)
            outdf = pd.DataFrame(data=[{
                "qid": qid,
                "filmoid": filmoid,
                "statut": "modifié"
            }])
        rapportdf = pd.concat([rapportdf, outdf])

    print(f"""{len(modif_qids)} oeuvres modifiées.
    Qids non modifiés : {', '.join(err_quids)}""")

    rapportdf.to_excel(f"rapports/prodqc_rapport-{timestr}.xlsx", index=False)


if __name__ == "__main__":
    main()