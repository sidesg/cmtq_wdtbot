#!/usr/local/bin/python3

import pywikibot
import polars as pl
import pandas as pd

def resume_df(df: pl.DataFrame) -> None:
    print(len(df))
    print(df.head())

def creer_metiersdict(chemin: str="donnees/mapping_metiers.csv") -> dict:
    df = pl.read_csv(chemin).filter(pl.col('qid').is_not_null())

    return dict(zip(
        df.select(pl.col("terme")).to_series(),
        df.select(pl.col("qid")).to_series()
    ))


def ajout_source(decl: pywikibot.Claim, repo: pywikibot.DataSite) -> None:
    """Ajouter une source, Affirmé dans (P248) CinéTV (Q41001657)"""
    ref = pywikibot.Claim(repo, u'P248')
    ref.setTarget(pywikibot.ItemPage(repo, 'Q41001657'))

    decl.addSource(ref, bot=True)

def create_item(repo: pywikibot.DataSite, row: pd.Series, nomcomp: str) -> str:
    new_item = pywikibot.ItemPage(repo)
    new_item.editLabels(
        labels= {
            "fr": nomcomp,
            "en": nomcomp
        }, 
        summary="Setting labels"
    )

    if pd.notna(row["voirnom"]):
        aliases = {
            "fr": row["voirnom"],
            "en": row["voirnom"]
        }
        new_item.editAliases(aliases, summary="Setting new aliases.")

    notes = row["Notes"] if pd.notna(row["Notes"]) else "Personne liée au cinéma québécois"

    new_item.editDescriptions({"fr": notes}, summary="Setting new descriptions.")

    nature = pywikibot.Claim(repo, "P31")
    ntarget = pywikibot.ItemPage(repo, "Q5")
    nature.setTarget(ntarget)
    new_item.addClaim(nature, bot=True)

    cmtqid = pywikibot.Claim(repo, "P8971")
    cmtqid.setTarget(str(row["cmtqID"]))
    new_item.addClaim(cmtqid, bot=True)
    ajout_source(cmtqid, repo)

    occupations = set(row["foncts"])
    for occ in occupations:
        claim = pywikibot.Claim(repo, "P106")
        target = pywikibot.ItemPage(repo, occ)
        claim.setTarget(target)
        new_item.addClaim(claim)
        ajout_source(claim, repo)
    
    return new_item.getID()


def main(donnees: str, minoccs: int, qlimit: int) -> None:
    datadf = pd.read_csv(donnees)
    nouvelles_personnes = datadf[datadf["occurrences"] > minoccs]
    nouvelles_personnes = nouvelles_personnes[nouvelles_personnes["effectue"].isnull()]
    nouvelles_personnes = nouvelles_personnes[nouvelles_personnes["qid"].isnull()]

    nouvelles_personnes["voirnom"] = nouvelles_personnes["voirnom"].str.split("|")
    nouvelles_personnes["voirid"] = nouvelles_personnes["voirid"].str.split("|")

    metiers_dict = creer_metiersdict()
    nouvelles_personnes["foncts"] = nouvelles_personnes["foncts"].str.split(", ")

    nouvelles_personnes["foncts"] = nouvelles_personnes["foncts"].apply(
        lambda x : [metiers_dict.get(f, None) for f in x if metiers_dict.get(f, None)]
    )

    nouvelles_personnes = nouvelles_personnes.head(qlimit)

    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    for idx, row in nouvelles_personnes.iterrows():
        nomcomp = str(row["Prenom"]) + " " + str(row["Nom"]) if row["Prenom"] else str(row["Nom"])

        new_item_id = create_item(repo, row, nomcomp)

        datadf.loc[datadf["cmtqID"] == row["cmtqID"], "effectue"] = "X"
        datadf.loc[datadf["cmtqID"] == row["cmtqID"], "qid"] = new_item_id

        print(f"{nomcomp} : {new_item_id}")

        datadf.to_csv("donnees/personnes_amapper-test.csv", index=False)


if __name__ == "__main__":
    donnees = "donnees/personnes_amapper-test.csv"
    minoccs = 40
    qlimit = 25

    main(donnees, minoccs, qlimit)