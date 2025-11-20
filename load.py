import sqlite3
import pandas as pd


def create_db_table(df: pd.DataFrame, table_name: str):
    connection = sqlite3.connect("data/disponibilite_velib.db")
    df.to_sql(name=table_name, con=connection, if_exists="replace")
