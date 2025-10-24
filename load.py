from transform import get_transformed_data
import sqlite3


def get_connection():
    conn = sqlite3.connect("data/disponibilite_velib.db")

    return conn


def get_cursor():
    connection = get_connection()
    cursor = connection.cursor()

    return cursor


def create_velib_table():
    con = get_connection()
    df = get_transformed_data()
    df.to_sql("velib", con=con, if_exists="replace")
