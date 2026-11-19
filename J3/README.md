Les scripts lisent les données brutes dans HDFS, les nettoient (Silver),
puis calculent les ventes par mois (Gold).
Les résultats sont en Parquet.


## Prérequis et versions

VM Ubuntu 24.04 (arm64, Multipass), dépôt partagé dans '/home/ubuntu/projet'.
HDFS démarré ('start-dfs.sh'), avec 'store_sales.dat' et 'date_dim.dat' dans
'/retail-lake/raw/tpcxbb/initial/'.

Python 3.12.3, PySpark 4.3.0, Java 21, Hadoop 3.5.0, Mode 'local[*]'.


## Preuves

Chaque fichier de 'preuves/' est une sortie de commande, datée sur sa première ligne.

'environnement/' : versions, test Spark, chemin HDFS, écriture test, ressources de la VM.
'exploration/' : dépôt de 'dat_dim.dat', export HTML du notebook (pour observer les schémas, les valeurs atypiques, etc.).
'transformations/' : exécution de 'nettoyage.py' et 'transformations.py' (jointure, agrégation).
'silver_gold/' : fichiers Parquet Silver et Gold dans HDFS.
'parquet/' : relecture de la copie Parquet : schéma conservé.
'qualite/' : 6 contrôles qualité, tous OK.