from transform import get_transformed_data
from extract import get_velib_data
from load import create_db_table


if __name__ == "__main__":
    velib_df = get_velib_data()
    velib_df = get_transformed_data(velib_df)
    print(velib_df)
    create_db_table(df=velib_df, table_name="velib")

