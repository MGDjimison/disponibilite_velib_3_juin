import time

from transform import (
    get_transformed_data,
    get_address
)


if __name__ == "__main__":
    cleaned_df = get_transformed_data()
    # print(cleaned_df.head())
    for val in cleaned_df["coordonnées_géographiques"]:
        address = get_address(val)
        print(address)
        time.sleep(2)
