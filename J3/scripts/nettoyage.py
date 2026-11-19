from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Paramètres et chemins
HDFS = "hdfs://localhost:9000"
SOURCE_VENTES = HDFS + "/retail-lake/raw/tpcxbb/initial/store_sales.dat"
SOURCE_DATES = HDFS + "/retail-lake/raw/tpcxbb/initial/date_dim.dat"
SILVER_VENTES = HDFS + "/retail-lake/silver/store_sales"
SILVER_DATES = HDFS + "/retail-lake/silver/date_dim"

# Noms des colonnes : specification TPCx-BB 1.7.0
COLONNES_VENTES = [
    "ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk",
    "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk",
    "ss_ticket_number", "ss_quantity", "ss_wholesale_cost", "ss_list_price",
    "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price",
    "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax",
    "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"
]
COLONNES_DATES = [
    "d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_week_seq",
    "d_quarter_seq", "d_year", "d_dow", "d_moy", "d_dom", "d_qoy",
    "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_day_name",
    "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday",
    "d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq",
    "d_current_day", "d_current_week", "d_current_month",
    "d_current_quarter", "d_current_year"
]

# On définit les types à la main
ENTIERS_VENTES = COLONNES_VENTES[0:11]    # identifiants + quantite
MONTANTS_VENTES = COLONNES_VENTES[11:23]  # montants
ENTIERS_DATES = [
    "d_date_sk", "d_month_seq", "d_week_seq", "d_quarter_seq", "d_year",
    "d_dow", "d_moy", "d_dom", "d_qoy", "d_fy_year", "d_fy_quarter_seq",
    "d_fy_week_seq", "d_first_dom", "d_last_dom", "d_same_day_ly",
    "d_same_day_lq"
]


def lire_source(spark, chemin, colonnes):
    df = (
        spark.read
        .option("header", False)
        .option("sep", "|")
        .csv(chemin)
    )
    return df.toDF(*colonnes)


def convertir(df, entiers, montants, dates):
    for nom_colonne in entiers:
        df = df.withColumn(nom_colonne, F.col(nom_colonne).cast("integer"))
    for nom_colonne in montants:
        df = df.withColumn(nom_colonne, F.col(nom_colonne).cast("double"))
    for nom_colonne in dates:
        df = df.withColumn(nom_colonne, F.col(nom_colonne).cast("date"))
    return df


def controler_nulls(df_avant, df_apres, nom):
    for nom_colonne in df_apres.columns:
        avant = df_avant.filter(F.col(nom_colonne).isNull()).count()
        apres = df_apres.filter(F.col(nom_colonne).isNull()).count()
        if avant > 0 or apres > 0:
            print(nom_colonne, ":", avant, "->", apres)


def ecrire_silver(df, source, chemin):
    df = (
        df.withColumn("source_fichier", F.lit(source))
        .withColumn("date_traitement", F.current_timestamp())
    )
    df.write.mode("overwrite").parquet(chemin)


def verifier_silver(spark, chemin, nb_attendu):
    df = spark.read.parquet(chemin)
    df.printSchema()
    print("lignes relues :", df.count(), "/ lignes attendues :", nb_attendu)


def main():
    spark = (
        SparkSession.builder
        .appName("TP3-Nettoyage")
        .master("local[*]")
        .getOrCreate()
    )

    ## Pour store_sales
    brut_ventes = lire_source(spark, SOURCE_VENTES, COLONNES_VENTES)
    ventes = convertir(brut_ventes, ENTIERS_VENTES, MONTANTS_VENTES, [])
    nb_ventes = brut_ventes.count()

    # Règle sur le type : la conversion ne doit pas produire de valeurs nulles
    controler_nulls(brut_ventes, ventes, "store_sales")

    # Règle doublon : aucune ligne identique
    print("store_sales - lignes en double ->", nb_ventes - ventes.dropDuplicates().count())

    # Règles sur les valeurs atypiques : les signaler sans supprimer
    print("store_sales - prix de vente nul :", ventes.filter(F.col("ss_sales_price") == 0).count())
    print("store_sales - net_profit négatif :", ventes.filter(F.col("ss_net_profit") < 0).count())

    ecrire_silver(ventes, SOURCE_VENTES, SILVER_VENTES)
    verifier_silver(spark, SILVER_VENTES, nb_ventes)

    ## Pour date_dim
    brut_dates = lire_source(spark, SOURCE_DATES, COLONNES_DATES)
    dates = convertir(brut_dates, ENTIERS_DATES, [], ["d_date"])
    nb_dates = brut_dates.count()
    print("date_dim - lignes lues :", nb_dates)

    controler_nulls(brut_dates, dates, "date_dim")
    print("date_dim - lignes en double ->", nb_dates - dates.dropDuplicates().count())

    ecrire_silver(dates, SOURCE_DATES, SILVER_DATES)
    verifier_silver(spark, SILVER_DATES, nb_dates)

    spark.stop()


if __name__ == "__main__":
    main()