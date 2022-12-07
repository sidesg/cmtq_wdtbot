"""
Ce bot associe des oeuvres cinématographiques au Québec en tant que lieu de création.
Le bot dépend d'un export de CinéTV, QCPRODS, qui contient des oeuvres identifées comme
ayant été produites au Québec.
"""

import re
import time
from pathlib import Path

import pandas as pd
import pywikibot

timestr = time.strftime("%Y%m%d-%H%M%S")

QCPRODS = Path.cwd() / "cinetv_prodqc"
FIMLOCOLS = [
    "Numéro séquentiel", 
    "Titre original"
]
CANADAURI = "http://www.wikidata.org/entity/Q16"

#Charger les oeuvres produites au QC à partir d'export(s) CinéTV
file_list = [f for f in QCPRODS.glob('**/*') if f.is_file()]

qcproddf = pd.DataFrame()

#TODO: make flexible to accomodate csv and xlsx
for file_path in file_list:
    indf = pd.read_csv(
        file_path,
        usecols=FIMLOCOLS,
        sep="\t",
        encoding="cp1252"
    )
    
    qcproddf = pd.concat([qcproddf, indf])

qcproddf = qcproddf.drop_duplicates()
qcproddf.rename(columns={'Numéro séquentiel': "FilmoId"}, inplace=True)
qcproddf = qcproddf[qcproddf["FilmoId"].notnull()]
qcproddf = qcproddf[qcproddf["FilmoId"].str.isnumeric()]
qcproddf = qcproddf.astype({"FilmoId": "int64"})

#Charger le mapping FilmoID-WdtURI
wdt_cmtqId = pd.read_csv("../wdt_cmtqId.csv")
wdt_cmtqId = wdt_cmtqId[wdt_cmtqId["FilmoId"].str.isnumeric()]
wdt_cmtqId = wdt_cmtqId.astype({"FilmoId": "int64"})

qcprodWdt = pd.merge(wdt_cmtqId, qcproddf, on="FilmoId")

print(f'{len(qcprodWdt)} oeuvres à modifier sur Wikidata.')

start = input("Ce script pourrait effectuer des modifications à grande échelle. Continuer? [o]ui/[N]on? ")

if not start.lower().startswith("o"):
  exit()


qcprodWdt = qcprodWdt.head(100)
qcprodWdt["LienWikidata"] = qcprodWdt["LienWikidata"].apply(lambda x: re.search(r"Q.+$", x).group().strip())

#Connecter à Wikidata
site = pywikibot.Site("wikidata", "wikidata")
repo = site.data_repository()


def ajout_qualification(decl, repo=repo):
        qualifier = pywikibot.Claim(repo, 'P131')
        target = pywikibot.ItemPage(repo, "Q176")
        qualifier.setTarget(target)

        decl.addQualifier(qualifier, bot=True)

def ajout_declaration(itm, repo=repo):
    claim = pywikibot.Claim(repo, 'P495')
    target = pywikibot.ItemPage(repo, 'Q16')
    claim.setTarget(target)

    ajout_qualification(claim)
    
    itm.addClaim(claim, bot=True)


err_quids = []
modif_qids = []

rapportdf = pd.DataFrame()

for idx, row in qcprodWdt.iterrows():
    changed = False
    qid = row["LienWikidata"]
    filmoid = row["FilmoId"]

    try:
        item = pywikibot.ItemPage(repo, qid)

        item_dict = item.get()  # Get the item dictionary
        clm_dict = item_dict["claims"]  # Get the claim dictionary

        if "P495" in clm_dict:
            pays_cibles = [claim.getTarget().concept_uri() for claim in item.claims['P495']]
            if CANADAURI in pays_cibles:
                for claim in item.claims['P495']: 
                    if claim.getTarget().concept_uri() == CANADAURI and 'P131' not in claim.qualifiers:
                        ajout_qualification(claim)
                        changed = True
            else:
                ajout_declaration(item)
                changed = True

        else:
            ajout_declaration(item)
            changed = True
    
    except:
        print(f"Qid {qid} inexistant")
    
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
Qids non modifiés : {err_quids}""")

rapportdf.to_excel(f"rapports/prodqc_rapport-{timestr}.xlsx", index=False)