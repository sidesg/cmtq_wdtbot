# CMTQ Wikidata Bots
Ce projet contient des bots qui modifient Wikidata à partir des données de la Cinémathèque québécoise. Chaque sous-dossier contient un bot qui fonctionne indépendamment des autres.

* `ajout_generique` : Ajouter des déclarations aux oeuvres identifiant des membres du générique.
* `ajoutFilmoId` : Créer des nouveaux mappings entre les identifiants d'oeuvres de la Cinémathèque et les identifiants Wikidata. (non fonctionnel)
* `mapping` : Produire les documents qui mappent ID Cinémathèque à ID Wikidata, dont se servent les autres scripts.
* `prodqc` : Ajouter des déclarations qui identifient des oeuvres cinématographiques comme ayant été produites au Québec.
* `recup_personnes` : Importer des informations concernant les personnes sur Wikidata ayant un identifiant de la Cinémathèque. (fonctionnalité limitée)
