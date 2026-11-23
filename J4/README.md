## Environnement

VM : Ubuntu 24.04.5 LTS arm64 (Multipass, sur Mac M3 Pro)
Java : OpenJDK 21.0.13
Python : 3.12.3
Hadoop (HDFS) : 3.5.0
PySpark : 4.3.0
JupyterLab : 4.6.6
pandas : 2.3.3
matplotlib : 3.11.2

L'environnement virtuel est le même que pour le TP 3.


## Reproduire l'UC-08

Ouvrir 'notebooks/analyses_kpi.ipynb' avec le noyau "Python - Retail TP4", puis exécuter toutes les cellules.
Les données sont lues dans HDFS ('/retail-lake/sliver/store-sales' et '/retail-lake/gold/ventes_mensuelles').
Le notebook régénère les deux PNG et les CSV de 'preuves/uc08/'.


## Réaliser l'UC-07

Actions faites dans le portail Azure :

1. Abonnement Azure for Students, sans carte bancaire.
2. Budget de 5$ par mois, alerte à 50%.
3. Groupe de ressources 'rg-retail-intelligence'.
4. Compte 'stretailgm112026' : StorageV2, Standard, LRS, Germany West Central, HNS activé.
5. Conteneur privé 'retail-intelligence', répertoires 'bronze/tpcxbb', 'silver/store_sales', 'silver/date_dim', 'gold/ventes_mensuelles'?
6. Dépôt du Parquet Gold du TP 3 dans 'gold/ventes_mensuelles.
7. Suppression du groupe de ressources après les captures.


## Preuves

# uc/08
'kpi_mensuels.csv' : les 60 mois avec CA net, quantité, transactions, panier moyen et évolution du CA. Sert aussi de résultat à la permière analyse.
'statistiques_lignes.csv' : statistiques par ligne de ticket : montant et quantité.
'statistiques_tickets.csv' : statistiques par ticket : montant et nombre de lignes.
'analyse2_magasins.csv' : CA net, tickets et panier moyen des 12 magasins.

# uc/07
'abonnement.png' : abonnement Azure for Students actif.
'budget.png' : budget de 5$ par mois, créé avant toute ressource.
'groupe_ressources.png' : groupe de ressources 'rg-retail-intelligence'.
'erreur_region.png' : France Central refusée pa la stratégie de l'abonnement.
'compte_stockage_hns.png' : compte de stockage avec l'espace de noms hiérarchique activé (ADLS Gen2).
'conteneur_repertoires.png' : conteneur 'retail-intelligence' avec les répertoires 'bronze', 'silver' et 'gold'.
'depot_gold.png' : fichier Parquet du Gold déposé (3,42 Kio).
'acl_gold.png' : ACL de '/gold' : propriétaire '$superuser', preuve des droits par répertoire du HNS.
