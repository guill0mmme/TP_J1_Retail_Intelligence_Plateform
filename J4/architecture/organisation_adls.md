## Ce qui a été déployé

Abonnement : Azure for Students (sans carte bancaire, budge de 5$ par mois avec une alerte à 50% de la consommation)

Groupe de ressources : 'rg-retail-intelligence'

Compte de stockage : 'stretailgm112026'

Région : Germany West Central (France Central erfusée par la stratégie de l'abonnement)

Conteneur : 'retail-intelligence', accès privé

Fichier déposé : Gold 'ventes_mensuelles' du Jour 3 (Parquet, 3503 octets)

Les répertoires ont été créés et le fichier a été déposé depuis le portail Azure.


## Arborescence

Un seul conteneur, organisé en répertoires par zones. Elle reprend celle de notre Data Lake HDFS '/retail-lake'.

retail-intelligence/
├── bronze/tpcxbb/              (déployé, vide)
├── silver/store_sales/         (déployé, vide)
├── silver/date_dim/            (déployé, vide)
└── gold/ventes_mensuelles/     (déployé, 1 fichier Parquet)


## Description des zones

Bronze : Conserver les sources telles quelles / Fichiers '.dat' TPCx-BB d'origine.

Silver : Données fiabilisées et typées / Parquet de 'store-sales' et 'date_dim'.

Gold : Données métier prêtes pour l'analyse / Parquet des agrégations ('ventes_mensuelles').


## Rôle du HNS

Sans HNS, un stockage Blob n'a pas de vrais répertoires.

Cela nous permet de gérer les permissions : on peut poser des ACL sur chaque répertoire et chaque fichier. On peut ainsi ouvrir 'gold/' en lecture aux analystes sans leur donner accès à 'bronze'.


## Limites et points de vigilance

- Les répertoires ont été créés avec la clé d'accès du compte. Le propriétaire des ACL est donc '$superuser'.
- Seul le fichier Gold a été déposé. Les autres zones sont créées mais vides.