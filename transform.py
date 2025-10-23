import pandas
from geopy import Nominatim

from extract import get_velib_data


def get_transformed_data():
    df = get_velib_data()
    # convert column names to snake case
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df = df.set_index("identifiant_station")

    # convert human-readable columns to boolean
    names = {"OUI": True, "NON": False}
    df["station_en_fonctionnement"] = df["station_en_fonctionnement"].map(names)
    df["retour_vélib_possible"] = df["retour_vélib_possible"].map(names)
    df["borne_de_paiement_disponible"] = df["borne_de_paiement_disponible"].map(names)

    # convert column to human-friendly string
    df["actualisation_de_la_donnée"] = pandas.to_datetime(
        df["actualisation_de_la_donnée"], utc=True
    )
    df["actualisation_de_la_donnée"] = df["actualisation_de_la_donnée"].apply(
        lambda x: x.strftime("%Y-%m-%d %X")
    )

    # remove useless column (full of nan)
    df = df.drop("station_opening_hours", axis=1)

    # rename columns
    df = df.rename(
        columns={
            "nom_communes_équipées": "commune",
            "code_insee_communes_équipées": "code_insee",
        }
    )

    return df


def add_department():
    df = get_transformed_data()
    # convert "code_postal" to string and extract two first characters
    df["departement"] = df["code_insee"].apply(lambda val: str(val)[:2])
    department = {
        "75": "Paris",
        "77": "Seine-et-Marne",
        "78": "Yvelines",
        "91": "Essonne",
        "92": "Hauts-de-Seine",
        "93": "Seine-Saint-Denis",
        "94": "Val-De-Marne",
        "95": "Val-D'Oise",
    }
    # convert "code_insee" to its corresponding name
    df["departement"] = df["departement"].map(department)
    return df["departement"]


def get_address(coordinate: str):
    geolocator = Nominatim(user_agent="velib")
    location = geolocator.reverse(coordinate)
    address = location.address.split(",")
    address = address[:3]
    address = "".join([char for char in address])
    return address
