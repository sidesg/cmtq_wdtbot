import time
from pathlib import Path
import argparse
import pandas as pd
import pywikibot
import re

timestr = time.strftime("%Y%m%d-%H%M%S")

parser = argparse.ArgumentParser()
parser.add_argument("-l", "--limite", help="nombre maximum d'URIs Wikidata à modifier", type=int)
args = parser.parse_args()

CTVCHEMIN = Path.cwd() / "donnees" / "genre_qc.csv"
GENDICT = {
    28: "P162", 
    29: "P162", 
    48: "P162"
    }

def main():
    ctvdf = pd.read_csv(CTVCHEMIN, sep=";")
    ctvdf = ctvdf[ctvdf["FonctionID"].isin(GENDICT)]
    ctvdf = nettoyerctv(ctvdf)

    wdt_cmtqId = chargerwdturi()

    ctvdf = pd.merge(wdt_cmtqId, ctvdf, on="FilmoId")
    ctvdf = ctvdf.drop(["FilmoId", "TitreOriginal"], axis=1)

    wdt_persId = chargerwdtpers()

    ctvdf = pd.merge(wdt_persId, ctvdf, on="NomID")
    ctvdf = ctvdf.drop(["NomID", "Nom"], axis=1)
    ctvdf["FonctionID"] = ctvdf["FonctionID"].apply(lambda x: GENDICT[x])

    ctvdf["persQid"] = ctvdf["persQid"].apply(lambda x: re.search(r"Q.+$", x).group().strip())
    ctvdf["OeuvreQid"] = ctvdf["OeuvreQid"].apply(lambda x: re.search(r"Q.+$", x).group().strip())

    if args.limite:
        ctvdf = ctvdf.head(args.limite) #Limiter le nombre d'URIs à modifier

    repo = creerrepo()

    ajoutdeclarations(ctvdf, repo)


    # print(type(pywikibot.ItemPage(repo, "Q85815526")))

    # exit()

    # print(ctvdf.head())
    # print(len(ctvdf))

def nettoyerctv(df):
    #TODO: merge NomID-OrganismeID, Nom-Terme
    mask = df['NomID'].isna()
    column_name = 'NomID'
    df.loc[mask, column_name] = df['OrganismeID']

    mask = df['Nom'].isna()
    column_name = 'Nom'
    df.loc[mask, column_name] = df['dbo_Sujet_Terme']

    df = df[df["NomID"].notna()]
    df = df.astype({"NomID": "int64"})

    df = df.drop(["OrganismeID", "dbo_Sujet_Terme"], axis=1)

    df = df.rename(columns={'FilmoID': "FilmoId"})
    
    return df

def chargerwdturi():
    #Charger le mapping FilmoID-WdtURI
    outdf = pd.read_csv("../wdt_cmtqId.csv")
    outdf = outdf[outdf["FilmoId"].str.isnumeric()]
    outdf = outdf.astype({"FilmoId": "int64"})
    outdf = outdf.rename(columns={'LienWikidata': "OeuvreQid"})

    return outdf

def chargerwdtpers():
    #Charger le mapping FilmoID-WdtURI
    outdf = pd.read_csv("../wdt_cmtq-pers.csv")
    outdf = outdf.astype({"cmtqID": "int64"})
    outdf = outdf.rename(columns={'cmtqID': "NomID"})

    return outdf

def creerrepo():
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    return repo    

def ajout_source(decl, repo):
    ref = pywikibot.Claim(repo, u'P248')
    ref.setTarget(pywikibot.ItemPage(repo, 'Q41001657'))

    decl.addSource(ref, bot=True)

def ajout_declaration(itm, pred:str, cible:str, repo):
    claim = pywikibot.Claim(repo, pred)
    target = pywikibot.ItemPage(repo, cible)
    claim.setTarget(target)
    
    itm.addClaim(claim, bot=True)

def ajoutdeclarations(mapping, repo):
    err_quids = []
    modif_qids = []

    rapportdf = pd.DataFrame()

    for oqid in mapping["OeuvreQid"].unique():
        changed = False

        item = pywikibot.ItemPage(repo, oqid)

        oeuvredf = mapping[mapping["OeuvreQid"] == oqid]
        
        for predqid in oeuvredf["FonctionID"].unique():
            fonctdf = oeuvredf[oeuvredf["FonctionID"] == predqid]
            fonctcibles = fonctdf["persQid"]

            for persqid in fonctcibles:
                try:
                    wdt_decldict = {
                    claim.getTarget().getID(): claim
                    for claim in item.claims[predqid]
                    }

                except: #Oeuvre n'a pas de déclaration avec pred
                    #Terminal
                    print(f"pas de {predqid} dans {oqid}")
                    ajout_declaration(item, predqid, predqid, repo)

                    continue

                if persqid in wdt_decldict: 
                    ajout_source(wdt_decldict[persqid], repo)
                    changed = True

                else:
                    #Terminal
                    #Ajouter autre occurence du pred avec nouv cible
                    print(f"{persqid} pas {predqid} dans {oqid}")

                if changed == False:
                    err_quids.append(oqid)
                    outdf = pd.DataFrame(data=[{
                        "oeuvre_qid": oqid,
                        "fonct_qid": predqid,
                        "personne_qid": persqid,
                        "statut": "non modifié"
                    }])
                    
                elif changed == True:
                    modif_qids.append(oqid)
                    outdf = pd.DataFrame(data=[{
                        "oeuvre_qid": oqid,
                        "fonct_qid": predqid,
                        "personne_qid": persqid,
                        "statut": "non modifié"
                    }])

                rapportdf = pd.concat([rapportdf, outdf])

    rapportdf.to_excel(f"rapports/prodqc_rapport-{timestr}.xlsx", index=False)



if __name__ == "__main__":
    main()