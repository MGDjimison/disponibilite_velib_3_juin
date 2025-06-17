import pandas as pd


def get_velib_data():
    df = pd.read_csv(
        "data/velib-disponibilite-en-temps-reel-paris-data.csv", delimiter=";"
    )
    return df
