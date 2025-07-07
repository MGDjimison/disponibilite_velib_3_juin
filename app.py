from transform import (
    get_transformed_data,
)


if __name__ == "__main__":
    cleaned_df = get_transformed_data()
    # print(cleaned_df.head())
    for val in cleaned_df["actualisation_de_la_donnée"]:
        print(type(val), val)
        break
