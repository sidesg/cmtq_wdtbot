#!/usr/local/bin/python3

"""
Créer le fichier CSV qui mappe les identifiants de la Cinémathèque aux URIs Wikidata
pour les entités précisées dans l'argument "entite"
"""

import pandas as pd
import requests
import pydash
import argparse
import yaml

parser = argparse.ArgumentParser()
parser.add_argument("entite", help="sélectionne l'entité dont le mapping est importé depuis Wikidata. Valeurs possibles : oevres, personnes.", type=str)

APIURL = 'https://query.wikidata.org/sparql'
HEADERS = {
    'User-Agent': 'CMTQBot/0.1 (gsides@cinematheque.qc.ca)'
}

def pull_clean(sparql: str) -> pd.DataFrame:
    """
    This function submits the SPARQL query, and cleans the resulting data.
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
    return (dataframe)


def main() -> None:
    args = parser.parse_args()

    with open("requetes.yaml", "r") as infile:
        entdict = yaml.safe_load(infile)

    if args.entite not in entdict:
        print(f"Valeur de l'argument 'entite', {args.entite}, non reconnue.")
        exit()

    requete = entdict[args.entite]["requete"]
    rapport_nom = entdict[args.entite]["rapport_nom"]

    wdt_cmtqId = pull_clean(requete)

    wdt_cmtqId.to_csv(f"{rapport_nom}-wdtmapping.csv", index=False)


if __name__ == "__main__":
    main()
