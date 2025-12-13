import os
from dotenv import load_dotenv
from functools import lru_cache
from opencage.geocoder import OpenCageGeocode
from opencage.geocoder import InvalidInputError, RateLimitExceededError


load_dotenv()
key = os.getenv("OPENCAGE_API_KEY")
geocoder = OpenCageGeocode(key)


@lru_cache(maxsize=None)
def reverse_geocode_cached(lat: str, lon: str):
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

