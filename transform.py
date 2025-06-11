import pandas

from dataframe import get_velib_data


def get_transformed_data():
    df = get_velib_data()
    # convert column names to snake case
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # convert human-readable columns to boolean
    names = {
        "OUI": True,
        "NON": False
    }
    df["station_en_fonctionnement"] = df["station_en_fonctionnement"].map(names)
    df["retour_vélib_possible"] = df["retour_vélib_possible"].map(names)
    df["borne_de_paiement_disponible"] = df["borne_de_paiement_disponible"].map(names)

    # convert string to datetime
    df["actualisation_de_la_donnée"] = pandas.to_datetime(df["actualisation_de_la_donnée"])

    # remove useless column (full of nan)
    df = df.drop("station_opening_hours", axis=1)

    # rename columns
    df = df.rename(columns={"nom_communes_équipées": "commune", "code_insee_communes_équipées": "code_postal"})

    return df


def get_velib_in_paris():
    df = get_transformed_data()
    paris_velib_df = df[
        (df["commune"] == "Paris") &
        (df["station_en_fonctionnement"] == True)
    ]
    return paris_velib_df


def get_station_by_commune():
    df = get_transformed_data()
    station_by_commune_df = df["commune"].value_counts()
    return station_by_commune_df


def get_station_by_departement():
    df = get_transformed_data()
    df["departement"] = add_department()
    station_by_departement_df = df["departement"].value_counts()
    return station_by_departement_df


def add_department():
    df = get_transformed_data()
    # convert "code_postal" to string and extract two first characters
    df["departement"] = df["code_postal"].apply(lambda val: str(val)[:2])
    department = {
        "75": "Paris",
        "77": "Seine-et-Marne",
        "78": "Yvelines",
        "91": "Essonne",
        "92": "Hauts-de-Seine",
        "93": "Seine-Saint-Denis",
        "94": "Val-De-Marne",
        "95": "Val-D'Oise"
    }
    # convert "code_postal" to its corresponding name
    df["departement"] = df["departement"].map(department)
    return df["departement"]