# Générique bot

Robot qui ajoute des déclarations sur les membres du générique d'oeuvres audiovisuelles produites au Québec.

Le mapping entre les fonctions (rôles) dans CinéTV et les prédicats Wikidata se trouve dans `donnees/fonction_map.csv`.

Arguments
* `--limite X` (`-l`) pour limiter les modifications aux X premiers URIs. Par exemple, `generiquebot.py -l 25` limitera les modifications aux 25 premiers URIs.
* `--qid` (`-q`) : Modifier un seul URI sur Wikidata avec toutes les déclarations avec cette oeuvre comme sujet identifiées dans CinéTV.