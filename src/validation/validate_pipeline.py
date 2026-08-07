import os

from trino.dbapi import connect


def get_connection():
    return connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=8080,
        user="airflow",
        catalog="lakehouse",
        schema="dbt_dev",
    )


def get_layer_counts(connection):
    query = """
        SELECT
            (
                SELECT count(*)
                FROM bronze.crime_reports
            ) AS bronze_count,

            (
                SELECT count(*)
                FROM silver.crime_reports
            ) AS silver_count,

            (
                SELECT sum(incident_count)
                FROM gold.monthly_crime_summary
            ) AS spark_gold_count,

            (
                SELECT sum(incident_count)
                FROM dbt_dev.fct_crime_incident
            ) AS dbt_fact_count,

            (
                SELECT sum(incident_count)
                FROM dbt_dev.mart_monthly_crime_trends
            ) AS dbt_mart_count
    """

    cursor = connection.cursor()
    cursor.execute(query)
    counts = cursor.fetchone()
    cursor.close()

    return counts


def get_gold_differences(connection):
    query = """
        SELECT
            (
                SELECT count(*)
                FROM (
                    SELECT
                        occ_month,
                        crime_type,
                        council_district,
                        incident_count,
                        known_clearance_count,
                        cleared_count,
                        family_violence_count
                    FROM gold.monthly_crime_summary

                    EXCEPT ALL

                    SELECT
                        occ_month,
                        crime_type,
                        council_district,
                        incident_count,
                        known_clearance_count,
                        cleared_count,
                        family_violence_count
                    FROM dbt_dev.mart_monthly_crime_trends
                ) AS spark_only
            ) AS spark_only_count,

            (
                SELECT count(*)
                FROM (
                    SELECT
                        occ_month,
                        crime_type,
                        council_district,
                        incident_count,
                        known_clearance_count,
                        cleared_count,
                        family_violence_count
                    FROM dbt_dev.mart_monthly_crime_trends

                    EXCEPT ALL

                    SELECT
                        occ_month,
                        crime_type,
                        council_district,
                        incident_count,
                        known_clearance_count,
                        cleared_count,
                        family_violence_count
                    FROM gold.monthly_crime_summary
                ) AS dbt_only
            ) AS dbt_only_count
    """

    cursor = connection.cursor()
    cursor.execute(query)
    differences = cursor.fetchone()
    cursor.close()

    return differences


def validate_layer_counts(counts):
    layer_counts = {
        "Bronze": counts[0],
        "Silver": counts[1],
        "Spark Gold": counts[2],
        "dbt Fact": counts[3],
        "dbt Mart": counts[4],
    }

    bronze_count = layer_counts["Bronze"]

    if bronze_count == 0:
        raise ValueError(
            "Bronze contains no records."
        )

    mismatched_layers = {
        layer: count
        for layer, count in layer_counts.items()
        if count != bronze_count
    }

    if mismatched_layers:
        raise ValueError(
            "Layer counts do not match: "
            f"{layer_counts}"
        )

    print("Layer count validation passed.")


def validate_gold_outputs(differences):
    spark_only_count = differences[0]
    dbt_only_count = differences[1]

    if (
        spark_only_count > 0
        or dbt_only_count > 0
    ):
        raise ValueError(
            "Spark Gold and dbt mart differ: "
            f"Spark-only={spark_only_count}, "
            f"dbt-only={dbt_only_count}"
        )

    print("Gold output validation passed.")


def main():
    connection = get_connection()

    try:
        differences = get_gold_differences(
            connection
        )
        counts = get_layer_counts(connection)
    finally:
        connection.close()

    print(f"Bronze: {counts[0]}")
    print(f"Silver: {counts[1]}")
    print(f"Spark Gold: {counts[2]}")
    print(f"dbt Fact: {counts[3]}")
    print(f"dbt Mart: {counts[4]}")

    validate_layer_counts(counts)

    print(
        "Spark-only Gold rows: "
        f"{differences[0]}"
    )
    print(
        "dbt-only Gold rows: "
        f"{differences[1]}"
    )

    validate_gold_outputs(differences)


if __name__ == "__main__":
    main()
