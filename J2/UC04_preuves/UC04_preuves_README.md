Toutes les sorties ont été produites dans la VM Ubuntu le 17/11/2026, via l'interface NiFi ouverte dans un onglet de mon navigateur Safari.

## configuration/

'statu_nifi.txt' : NiFi démarré : 'Status: Up'

'about_nifi.png' : Version de NiFi : 2.12.0

'preparation_dossiers.txt' : Création des dossiers /data/nifi, copie de store.dat, et création de /retail-lake/raw/nifi.

'config_getfile.png' : Propriétés de GetFile.

'config.puthdfs.png' : Propriétés de PutHDFS.

'flux_getfile_puthdfs.png' : Flux GetFile vers PutHDFS, avec la relation 'failure' reliée au processeur LogAttribute.

## execution/

'flux_execute.png' : Compteurs après l'exécution : 1 FlowFile lu par GetFile et reçu par PutHDFS, aucun échec.

'verification_hdfs.txt' : store.dat est bien présent dans /retail-lake/raw/nifi.


## erreurs/

'ajout_modules_hadoop.txt' : Correction suite à l'observation de l'abscence de PutHDFS dans les processeurs : téléchargement des deux modules Hadoop et chargement par NiFi.

'puthdfs_disponible.png' : PutHDFS disponible dans la liste des processeurs après la correction.

'erreur_dossier_inexistant.png' : Test d'erreur contrôlé : GetFile est invalide à cause d'un dossier source inexistant.

## erreurs/

'evenement_provenance.png' : Caractéristiques de l'évènement SEND.

'transit_uri.png' : Destination HDFS de l'évènement.

'attributs.png' : Attributs du FlowFile.