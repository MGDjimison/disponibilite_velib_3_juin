import pandas as pd


def extract_data():
    df = pd.read_csv(
        "data/velib-disponibilite-en-temps-reel-paris-data.csv", delimiter=";"
    )
    return df
