import pandas as pd
import requests
import pydash

APIURL = 'https://query.wikidata.org/sparql'
HEADERS = {
    'User-Agent': 'CMTQBot/0.1 (gsides@cinematheque.qc.ca)'
}

requete = """
    SELECT DISTINCT ?LienWikidata ?LienWikidataLabel ?FilmoId
    WHERE {
    ?LienWikidata wdt:P4276 ?FilmoId;
        wdt:P31/wdt:P279* wd:Q2431196.
    SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
    }
    """


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


def main():
    wdt_cmtqId = pull_clean(requete)

    wdt_cmtqId.to_csv("wdt_cmtqID.csv", index=False)


if __name__ == "__main__":
    main()
