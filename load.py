import pandas as pd
import duckdb


def load_data(df: pd.DataFrame):
    """Create duckdb db and table with given dataframe"""
    con = duckdb.connect("data/disponibilite_velib.db")
    con.sql("CREATE TABLE velib AS SELECT * FROM df")
