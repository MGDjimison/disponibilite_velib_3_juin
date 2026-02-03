import dagster as dg
import pandas as pd
from disponibilite_velib_etl.defs.utils import add_reverse_geocoding, get_department
from dagster_duckdb import DuckDBResource


START_FILE_PATH = "src/disponibilite_velib_etl/defs/data/velib-disponibilite-paris-data.csv"
TRANSFORMED_FILE_PATH = "src/disponibilite_velib_etl/defs/data/transformed-velib-data.csv"


@dg.asset(group_name="velib_etl", description="Transform raw Velib data")
def transformed_velib(context: dg.AssetExecutionContext) -> str:
    df = pd.read_csv(START_FILE_PATH, sep=";")

    # convert columns to snake_case
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # set index with existing columns
    df.set_index("identifiant_station", inplace=True)

    # convert human readable boolean to actual boolean
    names = {"OUI": True, "NON": False}
    bool_cols = [
        "station_en_fonctionnement",
        "retour_vélib_possible",
        "borne_de_paiement_disponible",
    ]
    df[bool_cols] = df[bool_cols].replace(names)

    # convert column to datetime
    df["actualisation_de_la_donnée"] = pd.to_datetime(
        df["actualisation_de_la_donnée"], utc=True
    )

    # remove useless column (full of nan)
    df = df.drop("station_opening_hours", axis=1)

    # rename columns
    df = df.rename(
        columns={
            "nom_communes_équipées": "commune",
            "code_insee_communes_équipées": "code_insee",
            "coordonnées_géographiques": "coordonnées",
        }
    )

    # add "departement" column based on "code_insee" column
    df["departement"] = df["code_insee"].apply(get_department).astype("category")

    # cast groups to reduce memory usage
    int16_cols = [
        "capacité_de_la_station",
        "nombre_bornettes_libres",
        "nombre_total_vélos_disponibles",
        "vélos_mécaniques_disponibles",
        "vélos_électriques_disponibles",
    ]
    df[int16_cols] = df[int16_cols].astype("int16")
    df["code_insee"] = df["code_insee"].astype("int32")

    df.to_csv(TRANSFORMED_FILE_PATH)
    context.log.info(f"Transformed data saved to {TRANSFORMED_FILE_PATH}")

    return TRANSFORMED_FILE_PATH


@dg.asset(deps=[transformed_velib], group_name="velib_etl", description="Create column from 'coordinates' column using reverse geocoding")
def reverse_geocode(context: dg.AssetExecutionContext) -> str:
    df = add_reverse_geocoding(TRANSFORMED_FILE_PATH)
    df.to_csv(TRANSFORMED_FILE_PATH, sep=";")
    context.log.info(f"Reverse geocoded data saved to {TRANSFORMED_FILE_PATH}")

    return TRANSFORMED_FILE_PATH


@dg.asset(deps=[reverse_geocode], group_name="velib_etl", description="Load final Velib data into DuckDB")
def final_velib(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    reverse_geocode: str,
) -> None:
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
                CREATE TABLE IF NOT EXISTS velib AS
                SELECT * FROM read_csv_auto('{reverse_geocode}', sep=';')
            """
        )
    context.log.info("Data loaded into DuckDB table 'velib' successfully.")


