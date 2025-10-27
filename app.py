from transform import get_transformed_data
from extract import get_velib_data

if __name__ == "__main__":
    velib_df = get_velib_data()
    print(velib_df)
    velib_df = get_transformed_data(velib_df)
    print(velib_df)
