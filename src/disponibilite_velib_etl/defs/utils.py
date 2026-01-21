import dagster as dg
import pandas as pd
from functools import lru_cache
from opencage.geocoder import OpenCageGeocode, InvalidInputError, RateLimitExceededError
from concurrent.futures import ThreadPoolExecutor


opencage_api_key = dg.EnvVar("OPENCAGE_API_KEY").get_value()
geocoder = OpenCageGeocode(opencage_api_key)


@lru_cache(maxsize=None)
def reverse_geocode_cached(lat: str, lon: str) -> str:
    try:
        results = geocoder.reverse_geocode(lat, lon, language="fr", no_annotations=1)
        if results and len(results):
            return results[0]["formatted"]
    except RateLimitExceededError as ex:
        # You have used the requests available on your plan.
        print(ex)
    except InvalidInputError as ex:
        # this happens for example with invalid unicode in the input data
        print(ex)
    
    return None


def reverse_from_row(row: dict):
    return reverse_geocode_cached(row["lat"], row["lon"])


def add_reverse_geocoding(file_path: str):
    df = pd.read_csv(file_path)
    df[["lat", "lon"]] = df["coordonnées"].str.split(",", expand=True)

    with ThreadPoolExecutor(max_workers=3) as executor:
        df["localisation"] = list(
            executor.map(reverse_from_row, df.to_dict("records"))
        )
        executor.shutdown(wait=True)

    df = df.drop(["lat", "lon"], axis=1)

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