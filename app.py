from transform import get_transformed_data
from load import create_velib_table

if __name__ == "__main__":
    df = get_transformed_data()
    print(df[["code_insee", "departement"]])

    create_velib_table()
