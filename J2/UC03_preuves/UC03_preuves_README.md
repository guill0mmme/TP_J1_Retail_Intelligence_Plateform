Toutes les sorties ont été produites dans la VM Ubuntu le 17/11/2026. Chaque fichier commence par la date d'exécution.

## environnement/

'verification_environnement.txt' : Suite aux commandes de vérifications dans le TP, affiche la version du système, de Python, de Java, de Hadoop, l'état de HDFS (1 DataNode actif) et l'état de NiFi.

'configuration_hadoop.txt' : Chemin d'installation, Java 17 utilisé par Hadoop, aucune erreur dans les journaux.

## hdfs/

'commandes_hdfs.txt' : Commandes HDFS de base : espace disponible, liste, création de /retail-lake.

'depot_store_sales.txt' : Dépôt de source_sales.dat avec la commande 'hdfs dfs -put', même taille en octets dans HDFS et en local.

'verification_stockage.txt' : Contrôles du stockage, affichage de l'arborescence complète et des taiilles, puis lecture du fichier .dat

## data_lake/

'arborescence_datalake.txt' : Création des zones Raw, Bronze, Silver, Gold, Quality et Logs, et liste des 10 répertoires

'inspection_store_sales.txt' : Inspection du fichier avant le dépôt : taille, format, premières lignes et nombre de lignes.