import pandas
from geopy import Nominatim
import pandas as pd
from geopy.extra.rate_limiter import RateLimiter


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
            "coordonnées_géographiques": "coords"
        }
    )

    # add "departement" column based on "code_insee" column
    df["departement"] = df["code_insee"].apply(lambda code: get_department(code))
    # Initialize the geocoder
    geolocator = Nominatim(user_agent="myGeocoder")
    # Create a rate limiter
    geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)
    # convert 'coords' from string to a tuple of floats
    df["coords"] = df["coords"].apply(lambda x: tuple(map(float, x.strip("()").split(","))))
    # Add 'localisation' column to dataframe by applying geocode to 'coords' column
    df["localisation"] = df["coords"].apply(geocode)

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
