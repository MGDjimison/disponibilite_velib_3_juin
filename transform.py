import pandas as pd
import os
from dotenv import load_dotenv
from tqdm import tqdm

from opencage.geocoder import OpenCageGeocode
from opencage.geocoder import InvalidInputError, RateLimitExceededError


def get_transformed_data(df: pd.DataFrame):
    # convert column names to snake case
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # set index with existing column
    df = df.set_index("identifiant_station")

    # convert human-readable columns to boolean
    names = {"OUI": True, "NON": False}
    df["station_en_fonctionnement"] = df["station_en_fonctionnement"].map(names)
    df["retour_vélib_possible"] = df["retour_vélib_possible"].map(names)
    df["borne_de_paiement_disponible"] = df["borne_de_paiement_disponible"].map(names)

    # convert column to human-friendly string
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
            "coordonnées_géographiques": "coords",
        }
    )

    # add "departement" column based on "code_insee" column
    df["departement"] = df["code_insee"].apply(lambda code: get_department(code))

    # change dtypes to reduce memory usage
    df["departement"] = df["departement"].astype("category")
    df["capacité_de_la_station"] = df["capacité_de_la_station"].astype("int16")
    df["nombre_bornettes_libres"] = df["nombre_bornettes_libres"].astype("int16")
    df["nombre_total_vélos_disponibles"] = df["nombre_total_vélos_disponibles"].astype(
        "int16"
    )
    df["vélos_mécaniques_disponibles"] = df["vélos_mécaniques_disponibles"].astype(
        "int16"
    )
    df["vélos_électriques_disponibles"] = df["vélos_électriques_disponibles"].astype(
        "int16"
    )
    df["code_insee"] = df["code_insee"].astype("int32")

    # Reverse geocoding using OpenCage API
    # For this part, you need to sign up on https://opencagedata.com/
    load_dotenv()
    key = os.getenv("OPENCAGE_API_KEY")
    geocoder = OpenCageGeocode(key)
    coordinates = list(df["coords"])
    locations = []

    for coord in tqdm(coordinates):
        coord = coord.split(",")
        coord = {"latitude": coord[0], "longitude": coord[1]}

        try:
            results = geocoder.reverse_geocode(
                coord["latitude"],
                coord["longitude"],
                language="fr",
                no_annotations="1",
            )
            if results and len(results):
                location = results[0]["formatted"]
                locations.append(location)
                # print(results[0]['formatted'])
                # 11 Rue Sauteyron, 33800 Bordeaux, Frankreich
        except RateLimitExceededError as ex:
            # You have used the requests available on your plan.
            print(ex)
        except InvalidInputError as ex:
            # this happens for example with invalid unicode in the input data
            print(ex)

    df["localisation"] = locations

    return df


def get_department(postal_code: int):
    postal_code = str(postal_code)[:2]
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

    # get department value for postal_code
    return department[postal_code]
