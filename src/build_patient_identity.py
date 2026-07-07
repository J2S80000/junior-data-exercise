from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import os


RESOURCES_DIR = "resources"
OUTPUT_DIR = "output"


def read_csv(spark, path):
    return (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )

def empty_to_null(col_name):
    return F.when(
        F.col(col_name).isNull()
        | (F.trim(F.col(col_name)) == "")
        | (F.upper(F.trim(F.col(col_name))) == "NULL"),
        F.lit(None),
    ).otherwise(F.trim(F.col(col_name)))


def normalize_date(df, source_col, target_col):
    if source_col not in df.columns:
        return df.withColumn(target_col, F.lit(None).cast("date"))

    return df.withColumn(
        target_col,
        F.coalesce(
            F.to_date(F.col(source_col), "yyyy-MM-dd"),
            F.to_date(F.col(source_col), "dd/MM/yyyy"),
            F.to_date(F.col(source_col), "dd-MM-yyyy"),
            F.to_date(F.col(source_col), "yyyy/MM/dd"),
        ),
    )


def add_ipp_canonique(df, mapping_ipp, ipp_col="ipp"):
    return (
        df.join(
            mapping_ipp,
            df[ipp_col] == mapping_ipp["ipp_source"],
            "left",
        )
        .withColumn(
            "ipp_canonique",
            F.coalesce(F.col("ipp_canonique"), F.col(ipp_col)),
        )
        .drop("ipp_source")
    )


def build_mapping_ipp(identifiants):
    identifiants_nettoyes = (
        identifiants
        .withColumn("statut_nettoye", F.upper(F.trim(F.col("statut"))))
        .withColumn(
            "statut_nettoye",
            F.translate(
                F.col("statut_nettoye"),
                "ÉÈÊË",
                "EEEE",
            ),
        )
    )

    mapping_ipp = (
        identifiants_nettoyes
        .withColumn(
            "ipp_canonique",
            F.when(
                (F.col("statut_nettoye") == "DEPRECIE")
                & F.col("ipp_principal").isNotNull()
                & (F.trim(F.col("ipp_principal")) != ""),
                F.col("ipp_principal"),
            ).otherwise(F.col("ipp")),
        )
        .select(
            F.col("ipp").alias("ipp_source"),
            "ipp_canonique",
            "statut_nettoye",
        )
    )

    return mapping_ipp


def normalize_patients(patients_avec_ipp_canonique):
    df = patients_avec_ipp_canonique

    if "nom_naissance" in df.columns:
        df = df.withColumn(
            "nom_naissance_norm",
            F.upper(F.trim(F.col("nom_naissance"))),
        )
    else:
        df = df.withColumn("nom_naissance_norm", F.lit(None))

    if "prenoms" in df.columns:
        df = df.withColumn(
            "prenoms_norm",
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(F.col("prenoms"), '\\[', ""),
                        '\\]',
                        "",
                    ),
                    '"',
                    "",
                )
            ),
        )
    else:
        df = df.withColumn("prenoms_norm", F.lit(None))

    if "date_naissance" in df.columns:
        df = normalize_date(df, "date_naissance", "date_naissance_norm")
    else:
        df = df.withColumn("date_naissance_norm", F.lit(None).cast("date"))

    if "date_deces" in df.columns:
        df = normalize_date(df, "date_deces", "date_deces_norm")
    else:
        df = df.withColumn("date_deces_norm", F.lit(None).cast("date"))

    if "sexe" in df.columns:
        df = (
            df.withColumn("sexe_nettoye", F.upper(F.trim(F.col("sexe"))))
            .withColumn(
                "gender_fhir",
                F.when(
                    F.col("sexe_nettoye").isin("M", "H", "HOMME", "MALE", "1"),
                    F.lit("male"),
                )
                .when(
                    F.col("sexe_nettoye").isin("F", "FEMME", "FEMALE", "2"),
                    F.lit("female"),
                )
                .otherwise(F.lit("unknown")),
            )
        )
    else:
        df = df.withColumn("gender_fhir", F.lit("unknown"))

    return df


def consolidate_one_row_per_patient(patients_normalises):
    """
    Objectif : une seule ligne par patient réel.
    On utilise ipp_canonique comme identifiant patient consolidé.
    Si plusieurs lignes existent, on privilégie la ligne active.
    """

    date_fin_col ="date_fin_validite"

    df = patients_normalises

    
    df = df.withColumn(
        "priorite_validite",
        F.when(empty_to_null(date_fin_col).isNull(), F.lit(0)).otherwise(F.lit(1)),
    )
    

    if "statut_nettoye" in df.columns:
        df = df.withColumn(
            "priorite_statut",
            F.when(F.col("statut_nettoye") == "ACTIF", F.lit(0)).otherwise(F.lit(1)),
        )
    else:
        df = df.withColumn("priorite_statut", F.lit(0))

    window_patient = (
        Window
        .partitionBy("ipp_canonique")
        .orderBy(
            F.col("priorite_statut").asc(),
            F.col("priorite_validite").asc(),
            F.col("ipp").asc(),
        )
    )

    patients_consolides = (
        df.withColumn("rang", F.row_number().over(window_patient))
        .filter(F.col("rang") == 1)
        .drop("rang", "priorite_statut", "priorite_validite")
    )

    return patients_consolides


def prepare_adresses(adresses, mapping_ipp):
    adresses_avec_ipp = add_ipp_canonique(adresses, mapping_ipp, "ipp")

    date_fin_col = "date_fin"

    
    adresses_avec_ipp = adresses_avec_ipp.filter(empty_to_null(date_fin_col).isNull())

    adresses_avec_ipp = adresses_avec_ipp.dropDuplicates(["ipp_canonique"])

    colonnes_adresse = [
        c for c in adresses_avec_ipp.columns
        if c not in ["ipp", "ipp_canonique", "ipp_source", "statut_nettoye"]
    ]

    if colonnes_adresse:
        return adresses_avec_ipp.select(
            "ipp_canonique",
            F.to_json(F.struct(*[F.col(c) for c in colonnes_adresse])).alias("adresse_json"),
        )

    return adresses_avec_ipp.select(
        "ipp_canonique",
        F.lit(None).alias("adresse_json"),
    )

def prepare_opposition(opposition, mapping_ipp):
    opposition_avec_ipp = add_ipp_canonique(opposition, mapping_ipp, "ipp")

    # Si la colonne opposition n'existe pas, on renvoie des valeurs nulles propres
    if "opposition" not in opposition_avec_ipp.columns:
        return opposition_avec_ipp.select(
            "ipp_canonique",
            F.lit(None).cast("boolean").alias("opposition_recherche"),
            F.lit(None).alias("opposition_recherche_raw"),
            F.lit(None).cast("date").alias("date_recueil_opposition"),
        ).dropDuplicates(["ipp_canonique"])

    opposition_nettoyee = (
        opposition_avec_ipp
        .withColumn("opposition_recherche_raw", F.trim(F.col("opposition")))
        .withColumn("opposition_norm", F.upper(F.trim(F.col("opposition"))))
        .withColumn(
            "opposition_norm",
            F.translate(
                F.col("opposition_norm"),
                "ÉÈÊËÀÂÄÔÖÙÛÜÎÏÇ",
                "EEEEAAAOOUUUIIC",
            ),
        )
        .withColumn(
            "opposition_recherche",
            F.when(
                F.col("opposition_norm").isin("O", "OUI", "TRUE", "1", "OPPOSE"),
                F.lit(True),
            )
            .when(
                F.col("opposition_norm").isin("N", "NON", "FALSE", "0"),
                F.lit(False),
            )
            .otherwise(F.lit(None).cast("boolean")),
        )
    )

    if "date_recueil" in opposition_nettoyee.columns:
        opposition_nettoyee = normalize_date(
            opposition_nettoyee,
            "date_recueil",
            "date_recueil_opposition",
        )
    else:
        opposition_nettoyee = opposition_nettoyee.withColumn(
            "date_recueil_opposition",
            F.lit(None).cast("date"),
        )

    opposition_finale = (
        opposition_nettoyee
        .select(
            "ipp_canonique",
            "opposition_recherche",
            "opposition_recherche_raw",
            "date_recueil_opposition",
        )
        .dropDuplicates(["ipp_canonique"])
    )

    return opposition_finale

def build_fhir_patient_json(df):
    """
    FHIR simplifié.
    Le but est de produire une colonne JSON sérialisable par une API.
    Ce n'est pas une validation FHIR complète.
    """

    return df.withColumn(
        "patient_fhir_json",
        F.to_json(
            F.struct(
                F.lit("Patient").alias("resourceType"),
                F.col("ipp_canonique").alias("id"),
                F.array(
                    F.struct(
                        F.lit("official").alias("use"),
                        F.col("ipp_canonique").alias("value"),
                    )
                ).alias("identifier"),
                F.array(
                    F.struct(
                        F.lit("official").alias("use"),
                        F.col("nom_naissance_norm").alias("family"),
                        F.array(F.col("prenoms_norm")).alias("given"),
                    )
                ).alias("name"),
                F.col("gender_fhir").alias("gender"),
                F.date_format(F.col("date_naissance_norm"), "yyyy-MM-dd").alias("birthDate"),
                F.when(
                    F.col("date_deces_norm").isNotNull(),
                    F.date_format(F.col("date_deces_norm"), "yyyy-MM-dd"),
                ).alias("deceasedDateTime"),
            )
        ),
    )


def main():
    spark = SparkSession.builder.appName("BuildPatientIdentityFHIR").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    patients_path = os.path.join(RESOURCES_DIR, "patients.csv")
    identifiants_path = os.path.join(RESOURCES_DIR, "identifiants_ipp.csv")
    adresses_path = os.path.join(RESOURCES_DIR, "adresses.csv")
    opposition_path = os.path.join(RESOURCES_DIR, "opposition_recherche.csv")

    # 1. Lecture des sources
    patients = read_csv(spark, patients_path)
    identifiants = read_csv(spark, identifiants_path)
    adresses = read_csv(spark, adresses_path)
    opposition = read_csv(spark, opposition_path)

    print("\n=== Schéma patients ===")
    patients.printSchema()

    print("\n=== Schéma identifiants ===")
    identifiants.printSchema()

    print("\n=== Schéma adresses ===")
    adresses.printSchema()

    print("\n=== Schéma opposition ===")
    opposition.printSchema()

    # 2. Construction du mapping IPP source -> IPP canonique
    mapping_ipp = build_mapping_ipp(identifiants)

    print("\n=== Mapping IPP ===")
    mapping_ipp.show(truncate=False)

    # 3. Ajout de l'IPP canonique aux patients
    patients_avec_ipp_canonique = add_ipp_canonique(patients, mapping_ipp, "ipp")

    print("\n=== Patients avec IPP canonique - étape intermédiaire ===")
    patients_avec_ipp_canonique.select(
        "ipp",
        "ipp_canonique",
        "statut_nettoye",
        "nom_naissance",
        "prenoms",
        "date_naissance",
        "sexe",
    ).show(truncate=False)

    # 4. Normalisation des champs principaux
    patients_normalises = normalize_patients(patients_avec_ipp_canonique)

    # 5. Consolidation : une seule ligne par patient réel
    patients_consolides = consolidate_one_row_per_patient(patients_normalises)

    print("\n=== Patients consolidés : une ligne par ipp_canonique ===")
    patients_consolides.select(
        "ipp_canonique",
        "nom_naissance_norm",
        "prenoms_norm",
        "date_naissance_norm",
        "gender_fhir",
    ).show(truncate=False)

    # 6. Préparation des enrichissements
    adresses_preparees = prepare_adresses(adresses, mapping_ipp)
    opposition_preparee = prepare_opposition(opposition, mapping_ipp)

    # 7. Enrichissement patient avec adresse et opposition recherche
    patients_enrichis = (
        patients_consolides
        .join(adresses_preparees, on="ipp_canonique", how="left")
        .join(opposition_preparee, on="ipp_canonique", how="left")
    )

    # 8. Construction d'une colonne JSON Patient FHIR simplifiée
    patients_fhir = build_fhir_patient_json(patients_enrichis)

    # 9. Sélection finale
    patients_final = patients_fhir.select(
    "ipp_canonique",
    "nom_naissance_norm",
    "prenoms_norm",
    "date_naissance_norm",
    "gender_fhir",
    "adresse_json",
    "opposition_recherche",
    "opposition_recherche_raw",
    "date_recueil_opposition",
    "patient_fhir_json",
    )

    print("\n=== Échantillon final ===")
    patients_final.show(truncate=False)

    # 10. Export local
    # Les transformations sont faites avec Spark.
    # toPandas est utilisé uniquement pour contourner les contraintes d'écriture Spark/Hadoop sous Windows.
    patients_final.toPandas().to_csv(
        os.path.join(OUTPUT_DIR, "patients_fhir_consolides.csv"),
        index=False,
        encoding="utf-8",
    )

    print("\nExport terminé : output/patients_fhir_consolides.csv")

    spark.stop()


if __name__ == "__main__":
    main()