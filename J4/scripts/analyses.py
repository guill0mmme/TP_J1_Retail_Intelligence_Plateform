#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TP4-Retail-Intelligence")
    .master("local[*]")
    .getOrCreate()
)

print("Version Spark :", spark.version)


# ## Chargement des jeux de données (Silver et Gold, HDFS)

# In[2]:


chemin = "hdfs://localhost:9000/retail-lake/silver/store_sales"

df_ventes = spark.read.parquet(chemin)

df_ventes.show(5)
df_ventes.printSchema()

print("Nombre de lignes :", df_ventes.count())


# In[3]:


chemin_gold = "hdfs://localhost:9000/retail-lake/gold/ventes_mensuelles"

df_gold = spark.read.parquet(chemin_gold)

df_gold.show(5)
df_gold.printSchema()

print("Nombre de lignes :", df_gold.count())


# ## Calcul des KPI (dictionnaire dans UC-01)

# In[4]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

fenetre = Window.orderBy("annee", "mois")

df_kpi = (
    df_gold
    .withColumn("ca_mois_precedent", F.lag("ca_net").over(fenetre))
    .withColumn(
        "evolution_ca_pct",
        F.round(
            (F.col("ca_net") - F.col("ca_mois_precedent"))
            / F.col("ca_mois_precedent") * 100,
            2
        )
    )
    .select("annee", "mois", "ca_net", "quantite",
            "nb_transactions", "panier_moyen", "evolution_ca_pct")
    .orderBy("annee", "mois")
)

df_kpi.show(60, truncate=False)


# ## Statistiques descriptives (Silver store_sales-

# In[5]:


# Niveau ligne
df_ventes.select("ss_net_paid", "ss_quantity").summary().show()
df_ventes.agg(F.sum("ss_net_paid"), F.sum("ss_quantity")).show()

# Niveau ticket : on regroupe les lignes de chaque ticket
df_tickets = (
    df_ventes
    .groupBy("ss_ticket_number")
    .agg(
        F.sum("ss_net_paid").alias("montant_ticket"),
        F.count("*").alias("lignes_par_ticket")
    )
)

df_tickets.select("montant_ticket", "lignes_par_ticket").summary().show()


# In[6]:


# Contrôle : les totaux du Gold doivent égaler ceux du Silver
df_gold.agg(F.sum("ca_net"), F.sum("quantite"), F.sum("nb_transactions")).show()


# ## Analyse 1 : évolution mensuelle du CA net (Q2, Q5)
# Filtre : décembre 2005 retiré (ventes jusqu'au 13/12 seulement).

# In[7]:


ventes_par_mois = (
    df_gold
    .filter(~((F.col("annee") == 2005) & (F.col("mois") == 12)))
    .orderBy("annee", "mois")
    .select(
        F.format_string("%d-%02d", "annee", "mois").alias("mois"),
        (F.col("ca_net") / 1000000).cast("double").alias("ca_net_millions")
    )
)

pdf_mois = ventes_par_mois.toPandas()
print(len(pdf_mois), "mois")


# In[8]:


import matplotlib.pyplot as plt
from pathlib import Path

Path("../visualisations").mkdir(parents=True, exist_ok=True)

plt.figure(figsize=(10,5))

plt.plot(pdf_mois["mois"], pdf_mois["ca_net_millions"], marker="o")
plt.title("Évolution mensuelle du CA net des magasins (janvier 2001 à novembre 2005)")
plt.xlabel("Mois")
plt.ylabel("CA net (en millions)")
plt.xticks(pdf_mois["mois"][::6], rotation=45)
plt.ylim(bottom=0)

plt.grid(True)
plt.tight_layout()
plt.savefig("../visualisations/visualisation_1.png", dpi=150, bbox_inches="tight")

plt.show()


# **Source** : Gold ventes_mensuelles (HDFS, Jour 3). On prend en compte seulement les ventes en magasins, pas les ventes web.
# Le jeu de données TPCx-BB est synthétique. Le mois de décembre 2005 est retiré du graphique car incomplet, et enfin la devise du CA est inconnue.
# 
# **Résultat** : on voit que le CA reste à peu près stable, et ne descend jamais en-dessous de 17.5M. Aucun tendance à la hausse ou à la baisse n'apparaît.
# 
# **Interprétation** : on voit que les creux du graphique tombent chaque année en février. On pourrait croire à un lien de causalité, mais nous reviendrons sur cela dans les limites.
# 
# **Limites** : le nombre de ticket suit le nombre de jours du mois, la baisse en février vient donc de la durée du mois. De plus, les données présentées sont synthétiques, les ventes web et les retours sont absents.

# ## Analyse 2 : comparaison des magasins
# Période : toutes les ventes du Silver (de janvier 2001 au 13/12/2005), identique pour les 12 magasins

# In[9]:


ventes_par_magasin = (
    df_ventes
    .groupBy("ss_store_sk")
    .agg(
        F.sum("ss_net_paid").alias("ca_net"),
        F.countDistinct("ss_ticket_number").alias("nb_transactions")
    )
    .withColumn("panier_moyen", F.round(F.col("ca_net") / F.col("nb_transactions"), 2))
    .withColumn("ca_net_millions", F.round(F.col("ca_net") / 1000000, 2))
    .orderBy(F.desc("ca_net"))
)

ventes_par_magasin.show()

pdf_ventes = ventes_par_magasin.toPandas()


# In[10]:


plt.figure(figsize=(10, 5))

plt.bar(
    pdf_ventes["ss_store_sk"].astype(str),
    pdf_ventes["ca_net_millions"]
)

plt.title("CA net par magasin (janvier 2001 au 13 décembre 2005)")
plt.xlabel("Identifiant du magasin (ss_store_sk)")
plt.ylabel("CA net (en millions)")
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig("../visualisations/visualisation_2.png", dpi=150, bbox_inches="tight")
plt.show()


# **Source** : Silver store_sales (HDFS, Jour 3), regroupé par ss_store_sk.
# Le jeu de données TPCx-BB est synthétique et la devise du CA est inconnue.
# 
# **Résultat** : le magasin 7 est en tête, avec un CA d'environ 98 millions, le magasin 2 est dernier. L'écart reste assez faible visuellement.
# 
# **Interprétation** : comme ont peut le voir dans tableau de la cellule précédente, le magasin 7 est celui qui possède le plus de tickets (7645), mais un panier moyen en dessous de la médiane (mediane ~= 12 955). Son avance vient donc de son nombre élebé de transactions.
# 
# **Limites** : il nous manque pas mal d'informations sur les magasins pour aider à la décision : on n'a pas l'emplacement, la surface ou les horaires d'ouverture des magasins par exemple.

# ## Export des résultats (preuves/uc08)

# In[13]:


Path("../preuves/uc08").mkdir(parents=True, exist_ok=True)

df_kpi.toPandas().to_csv("../preuves/uc08/01_kpi_mensuels.csv", index=False)

df_ventes.select("ss_net_paid", "ss_quantity").summary().toPandas().to_csv("../preuves/uc08/02_statistiques_lignes.csv", index=False)

df_tickets.select("montant_ticket", "lignes_par_ticket").summary().toPandas().to_csv("../preuves/uc08/03_statistiques_tickets.csv", index=False)

pdf_ventes.to_csv("../preuves/uc08/04_analyse2_magasins.csv", index=False)


# In[ ]:




