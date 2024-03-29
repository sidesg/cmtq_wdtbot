#!/usr/local/bin/python3

import pandas as pd
import requests
import pydash
import time

APIURL = 'https://query.wikidata.org/sparql'
HEADERS = {
    'User-Agent': 'CMTQBot/0.1 (gsides@cinematheque.qc.ca)'
}

def main():
    timestr = time.strftime("%Y%m%d-%H%M%S")
    exportpth = f'exports/personnes_donnees-{timestr}.csv'

    #charger mapdict
    mapdf = pd.read_csv("qid_map.csv")
    mapdict = dict(zip(mapdf["qid"], mapdf["etiquette"]))
    
    print("Récupération des données sur Wikidata")
    exportdf = importpersonnes()

    print("Création du tableau d'export")
    exportdf["isnis"] = exportdf["isnis"].apply(lambda x : x.split(", "))
    exportdf["viafs"] = exportdf["viafs"].apply(lambda x : x.split(", "))
    exportdf["banqids"] = exportdf["banqids"].apply(lambda x : x.split(", "))
    exportdf["imdbids"] = exportdf["imdbids"].apply(lambda x : x.split(", "))
    exportdf["genres"] = exportdf["genres"].apply(lambda x : x.split(", "))
    exportdf["citoyens"] = exportdf["citoyens"].apply(lambda x : x.split(", "))
    exportdf["naisses"] = exportdf["naisses"].apply(lambda x : x.split(", "))
    exportdf["morts"] = exportdf["morts"].apply(lambda x : x.split(", "))

    exportdf = exportdf.applymap(lambda x : dicttransform(x, mapdict))
    exportdf = exportdf.applymap(cleanlists)

    exportdf.to_csv(exportpth, index=False)
    

def pull_clean(requete: str) -> pd.DataFrame:
    """
    This function submits the SPARQL query, and cleans the resulting data.
    """

    r = requests.get(
        APIURL,
        params={'format': 'json', 'query': requete},
        headers=HEADERS
    )

    print(r.status_code)

    data = r.json()

    # Diviser le json en X colonnes selon les X clés 1er niveau (variables SPARQL)
    dataframe = pd.DataFrame.from_dict(
        [x for x in pydash.get(data, 'results.bindings')]
    )

    # Remplacer la valeur des cellules par la valeur de la clé "value"
    for k in list(dataframe.columns.values):
        def extract_value(row, col):
            return (pydash.get(row[col], 'value'))
        dataframe[k] = dataframe.apply(extract_value, col=k, axis=1)

    return dataframe


def importpersonnes() -> pd.DataFrame:
    """
    Cette fonction produit un tableau organisé par Wikidata Qid en fonction 
    de la requête SPARQL qui décrit les personnes avec un identifiant de la CQ.
    """
    requete = """
        SELECT DISTINCT ?LienWikidataLabel ?LienWikidata ?PersID ?statutd
            (GROUP_CONCAT(DISTINCT ?isni; SEPARATOR=", ") AS ?isnis)
            (GROUP_CONCAT(DISTINCT ?viaf; SEPARATOR=", ") AS ?viafs)
            (GROUP_CONCAT(DISTINCT ?banqid; SEPARATOR=", ") AS ?banqids)
            (GROUP_CONCAT(DISTINCT ?imdbid; SEPARATOR=", ") AS ?imdbids)
            (GROUP_CONCAT(DISTINCT ?genre; SEPARATOR=", ") AS ?genres)
            (GROUP_CONCAT(DISTINCT ?citoyen; SEPARATOR=", ") AS ?citoyens)
            (GROUP_CONCAT(DISTINCT ?naisse; SEPARATOR=", ") AS ?naisses)
            (GROUP_CONCAT(DISTINCT ?mort; SEPARATOR=", ") AS ?morts)
        WHERE {
            ?LienWikidata wdt:P8971 ?PersID;
                wdt:P31 wd:Q5.
            OPTIONAL {?LienWikidata wdt:P7763 ?statutd .}
            OPTIONAL {?LienWikidata wdt:P213 ?isni .}
            OPTIONAL {?LienWikidata wdt:P214 ?viaf .}
            OPTIONAL {?LienWikidata wdt:P3280 ?banqid .}
            OPTIONAL {?LienWikidata wdt:P4985 ?imdbid .}
            OPTIONAL {?LienWikidata wdt:P21 ?genre .}
            OPTIONAL {?LienWikidata wdt:P27 ?citoyen .}
            OPTIONAL {?LienWikidata wdt:P569 ?naisse .}
            OPTIONAL {?LienWikidata wdt:P570 ?mort .}
            SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
        }
        GROUP BY ?LienWikidata ?LienWikidataLabel ?PersID ?statutd
        ORDER BY ?LienWikidata
    """

    return pull_clean(requete)

def dicttransform(cell: str | list, refdict: dict) -> str | list:
    if isinstance(cell, list):
        return [
            refdict.get(item, item)
            for item in cell
        ]

    else:
        return refdict.get(cell, cell)

    
def cleanlists(cell):
    if isinstance(cell, list) and len(cell) == 1 and cell[0] == "":
        return None

    else:
        return cell


if __name__ == "__main__":
    main()
