import duckdb
import pandas as pd
from prefect import task, flow

from lib import reverse_geocode_cached, get_department


@task(log_prints=True)
def extract_data():
    df = pd.read_csv(
        "data/velib-disponibilite-en-temps-reel-paris-data.csv", delimiter=";"
    )
    return df


@task(log_prints=True)
def transform_data(df: pd.DataFrame):
    # convert column names to snake case
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # set index with existing column
    df = df.set_index("identifiant_station")

    # convert human-readable columns to boolean
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

    # Reverse geocoding using OpenCage API
    # For this part, you need to sign up on https://opencagedata.com/

    # split coordinates once
    df[["lat", "lon"]] = df["coordonnées"].str.split(",", expand=True)

    # reverse geocode with caching
    df["localisation"] = df.apply(
        lambda row: reverse_geocode_cached(row["lat"], row["lon"]), axis=1
    )

    df = df.drop(["lat", "lon"], axis=1)

    return df


@task(log_prints=True)
def load_data(df: pd.DataFrame):
    """Create duckdb db and table with given dataframe"""
    con = duckdb.connect("data/disponibilite_velib.duckdb")
    con.sql("CREATE TABLE velib AS SELECT * FROM df")


@flow(log_prints=True)
def run_etl():
    velib_df = extract_data()
    print(velib_df.info(memory_usage="deep"))
    velib_df = transform_data(velib_df)
    print(velib_df.info(memory_usage="deep"))
    load_data(velib_df)
    print("ETL Completed")


if __name__ == "__main__":
    run_etl.serve(name="disponibilite-velib")
    con = duckdb.connect("data/disponibilite_velib.duckdb")
    df = con.sql("SELECT * FROM velib LIMIT 5").df()
    print(df)
