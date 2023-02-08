# Prodqc_bot

Ce bot a besoin de deux fichiers supplémentaires (1) un export de CinéTV d'oeuvres produites au Québec et (2) un mapping entre les identifiants d'oeuvres de la Cinémathèque et les identifiants de Wikidata. Le script pour produire ce deuxième fichier se trouve dans le dossier `../mapping`.

Arguments
* `--fichier` (`-f`) : Sélectionner le fichier dans `/cinetv_prodqc` à charger comme source des données. Si cet argument est absent, tous les fichiers du dossier sont chargés.
* `--limite` (`-l`) : Limiter les modifications aux X premiers URIs. Par exemple, `prodqc_bot.py -l 25` limitera les modifications aux 25 premiers URIs.
* `--qid` (`-q`) : Modifier un seul URI sur Wikidata (attention! avec cet argument, le bot ne vérifie pas que l'oeuvre figure sur la liste des oeuvres québécoises dans CinéTV).
