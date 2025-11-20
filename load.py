import sqlite3
import pandas as pd


def get_connection():
    conn = sqlite3.connect("data/disponibilite_velib.db")

    return conn


def get_cursor():
    connection = get_connection()
    cursor = connection.cursor()

    return cursor


def create_db_table(df: pd.DataFrame, table_name: str):
    con = get_connection()
    df.to_sql(name=table_name, con=con, if_exists="replace")
