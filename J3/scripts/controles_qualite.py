from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Paramètres et chemins
HDFS = "hdfs://localhost:9000"
SIlVER_VENTES = HDFS + "/retail-lake/silver/store_sales"
SILVER_DATES = HDFS + "/retail-lake/silver/date_dim"
GOLD_VENTES_MOIS = HDFS + "/retail-lake/gold/ventes_mensuelles"

# KPI
CHAMPS_REQUIS = ["ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk",
                 "ss_ticket_number", "ss_quantity", "ss_net_paid"]

# Types attendus après nettoyage
TYPES_ATTENDUS_VENTES = {"ss_sold_date_sk": "int", "ss_ticket_number": "int",
                         "ss_quantity": "int", "ss_ext_sales_price": "double",
                         "ss_coupon_amt": "double", "ss_net_paid": "double"}
TYPES_ATTENDUS_DATES = {"d_date_sk": "int", "d_date": "date", "d_year": "int",
                        "d_moy": "int"}


def afficher(point, regle, resultat, statut):
    print(point, "|", regle, "|", resultat, "|", statut)


def main():
    spark = (
        SparkSession.builder
        .appName("TP3-Controles-Qualite")
        .master("local[*]")
        .getOrCreate()
    )

    ventes = spark.read.parquet(SIlVER_VENTES)
    dates = spark.read.parquet(SILVER_DATES)
    gold = spark.read.parquet(GOLD_VENTES_MOIS)
    nb_ventes = ventes.count()

    print("Contrôle | Règle | Résultat | Statut")

    # 1. Complétude
    nb_nulls = 0
    for nom_colonne in CHAMPS_REQUIS:
        nb_nulls += ventes.filter(F.col(nom_colonne).isNull()).count()
    afficher("Complétude", "Champs requis non nuls (7 colonnes)",
             str(nb_nulls) + " valeur(s) nulle(s)", "OK" if nb_nulls == 0 else "KO")

    # 2. Unicité
    doublons_dates = dates.count() - dates.select("d_date_sk").distinct().count()
    afficher("Unicité / clé candidate", "d_date_sk unique (Silver date_dim)",
             str(doublons_dates) + " doublon(s)", "OK" if doublons_dates == 0 else "KO")    

    # 3. Validité des types
    types_ventes = dict(ventes.dtypes)
    types_dates = dict(dates.dtypes)
    ecarts = [c for c, t in TYPES_ATTENDUS_VENTES.items() if types_ventes.get(c) != t]
    ecarts += [c for c, t in TYPES_ATTENDUS_DATES.items() if types_dates.get(c) != t]
    afficher("Validité des types", "Types attendus apres nettoyage",
             str(len(ecarts)) + " ecart(s) de type", "OK" if not ecarts else "KO")

    # 4. Cohérence
    ecarts_net_paid = ventes.filter(
        F.abs(F.col("ss_net_paid") - (F.col("ss_ext_sales_price") - F.col("ss_coupon_amt"))) > 0.01
    ).count()
    afficher("Cohérence", "net_paid = ext_sales_price - coupon_amt (à 0,01 près)",
             str(ecarts_net_paid) + " écart(s)", "OK" if ecarts_net_paid == 0 else "KO")

    # 5. Volumes
    total_gold = gold.agg(F.sum("nb_lignes")).first()[0]
    afficher("Volumes", "Somme nb_lignes Gold = lignes Silver",
             str(total_gold) + " / " + str(nb_ventes), "OK" if total_gold == nb_ventes else "KO")

    # 6. Intégrité référentielle
    orphelins_dates = ventes.join(
        dates, ventes["ss_sold_date_sk"] == dates["d_date_sk"], how="left_anti"
    ).count()
    afficher("Intégrité", "Toute date de vente existe dans date_dim",
             str(orphelins_dates) + " vente(s) orpheline(s)", "OK" if orphelins_dates == 0 else "KO")

    spark.stop()


if __name__ == "__main__":
    main()