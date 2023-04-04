#!/usr/local/bin/python3

import pywikibot
import polars as pl

def main(donnees, minoccs) -> None:
    nouvelles_personnes = (
        pl.scan_csv(donnees)
        .filter(pl.col("occurrences") > minoccs)
        .filter(pl.col("effectue").is_null())
        .filter(pl.col("qid").is_null())
        .collect()
    )

    #Traduire "foncts" en occupation qids
    metiers_dict = creer_metiersdict()
    foncts = nouvelles_personnes.select(pl.col("foncts").str.split(", ")).to_series()
    foncts = foncts.apply(
        lambda x : [metiers_dict.get(f, None) for f in x if metiers_dict.get(f, None)]
    )
    nouvelles_personnes.replace("foncts", foncts)

    #Créer entités WDT
    nouvelles_personnes = nouvelles_personnes.head(5)

    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    for row in nouvelles_personnes.rows(named=True):
        nomcomp = row["Prenom"] + " " + row["Nom"] if row["Prenom"] else row["Nom"]
        
        # assert type(row) == dict

        new_item_id = create_item(repo, row, nomcomp)
        
        print(f"{nomcomp} : {new_item_id}")



def resume_df(df: pl.DataFrame):
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

def create_item(repo: pywikibot.DataSite, row: dict, nomcomp: str) -> str:
    new_item = pywikibot.ItemPage(repo)
    new_item.editLabels(
        labels= {
            "fr": nomcomp,
            "en": nomcomp
        }, 
        summary="Setting labels"
    )

    #TODO: Ajouter AKA

    notes = row["Notes"] if row["Notes"] else "Personne liée au cinéma québécois"
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


if __name__ == "__main__":
    donnees = "donnees/personnes_amapper.csv"
    minoccs = 40

    main(donnees, minoccs)