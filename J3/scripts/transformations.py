from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Parametres et chemins
HDFS = "hdfs://localhost:9000"
SILVER_VENTES = HDFS + "/retail-lake/silver/store_sales"
SILVER_DATES = HDFS + "/retail-lake/silver/date_dim"
GOLD_VENTES_MOIS = HDFS + "/retail-lake/gold/ventes_mensuelles"


def joindre_dates(ventes, dates):
    dates_utiles = dates.select("d_date_sk", "d_date", "d_year", "d_moy")
    ventes_dates = ventes.join(
        dates_utiles,
        ventes["ss_sold_date_sk"] == dates_utiles["d_date_sk"],
        how="left"
    )
    print("Lignes avant jointure :", ventes.count())
    print("Lignes apres jointure :", ventes_dates.count())
    print("Ventes sans date correspondante :",
          ventes_dates.filter(F.col("d_date_sk").isNull()).count())
    # Règle identifiant invalide : les ventes sans date sont mises a l'ecart
    return ventes_dates.filter(F.col("d_date_sk").isNotNull())


def ventes_par_mois(ventes_dates):
    return (
        ventes_dates
        .groupBy("d_year", "d_moy")
        .agg(
            F.sum("ss_net_paid").cast("decimal(15,2)").alias("ca_net"),
            F.sum("ss_quantity").alias("quantite"),
            F.countDistinct("ss_ticket_number").alias("nb_transactions"),
            F.count("*").alias("nb_lignes")
        )
        .withColumn("panier_moyen",
                    F.round(F.col("ca_net") / F.col("nb_transactions"), 2))
        .withColumnRenamed("d_year", "annee")
        .withColumnRenamed("d_moy", "mois")
        .orderBy("annee", "mois")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("TP3-Transformations")
        .master("local[*]")
        .getOrCreate()
    )

    ventes = spark.read.parquet(SILVER_VENTES)
    dates = spark.read.parquet(SILVER_DATES)

    # Jointure vérifiée puis agrégation
    ventes_dates = joindre_dates(ventes, dates)
    gold = ventes_par_mois(ventes_dates)

    # Overwrite du répertoire gold
    gold.write.mode("overwrite").parquet(GOLD_VENTES_MOIS)

    # Relecture et vérification
    gold_relecture = spark.read.parquet(GOLD_VENTES_MOIS)
    gold_relecture.printSchema()
    print("Lignes gold relues :", gold_relecture.count())
    gold_relecture.orderBy("annee", "mois").show(60, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()