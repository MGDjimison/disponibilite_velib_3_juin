import pandas

from extract import get_velib_data


def get_transformed_data():
    df = get_velib_data()
    # convert column names to snake case
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # convert human-readable columns to boolean
    names = {"OUI": True, "NON": False}
    df["station_en_fonctionnement"] = df["station_en_fonctionnement"].map(names)
    df["retour_vélib_possible"] = df["retour_vélib_possible"].map(names)
    df["borne_de_paiement_disponible"] = df["borne_de_paiement_disponible"].map(names)

    # convert string to datetime
    df["actualisation_de_la_donnée"] = pandas.to_datetime(
        df["actualisation_de_la_donnée"]
    )

    # remove useless column (full of nan)
    df = df.drop("station_opening_hours", axis=1)

    # rename columns
    df = df.rename(
        columns={
            "identifiant_station": "id_station",
            "nom_communes_équipées": "commune",
            "code_insee_communes_équipées": "code_insee",
        }
    )

    return df


def get_numerical_columns():
    # except id and code_postal
    transformed_velib_df = get_transformed_data()
    # select column which dtype is "int64"
    df = transformed_velib_df.select_dtypes(include="int64")
    # drop unwanted column
    final_df = df.drop(["identifiant_station", "code_postal"], axis=1)
    return final_df


def get_average_of_columns_by_department():
    transformed_velib_df = get_transformed_data()
    transformed_velib_df["departement"] = add_department()
    # get numerical columns dataframe
    numerical_columns_df = get_numerical_columns()
    # get names of numerical columns
    numerical_columns = numerical_columns_df.columns

    numerical_columns_df["departement"] = transformed_velib_df["departement"]
    # print(numerical_columns_df)
    data_groupby_department_df = numerical_columns_df.groupby("departement")[
        numerical_columns
    ].mean()
    # round all columns
    data_groupby_department_df = data_groupby_department_df.round()

    return data_groupby_department_df


def get_average_of_columns_by_city():
    transformed_velib_df = get_transformed_data()
    # get numerical columns dataframe
    numerical_columns_df = get_numerical_columns()
    # get names of numerical columns
    numerical_columns = numerical_columns_df.columns

    numerical_columns_df["commune"] = transformed_velib_df["commune"]
    # print(numerical_columns_df)
    data_groupby_city_df = numerical_columns_df.groupby("commune")[
        numerical_columns
    ].mean()
    # round all columns
    data_groupby_city_df = data_groupby_city_df.round()

    return data_groupby_city_df


def get_velib_in_paris():
    df = get_transformed_data()
    paris_velib_df = df[(df["commune"] == "Paris") & (df["station_en_fonctionnement"])]
    return paris_velib_df


def get_station_by_city():
    df = get_transformed_data()
    station_by_commune_df = df["commune"].value_counts()
    return station_by_commune_df


def get_station_by_department():
    df = get_transformed_data()
    df["departement"] = add_department()
    station_by_departement_df = df["departement"].value_counts()
    return station_by_departement_df


def add_department():
    df = get_transformed_data()
    # convert "code_postal" to string and extract two first characters
    df["departement"] = df["code_postal"].apply(lambda val: str(val)[:2])
    department = {
        "75": "Paris",
        "77": "Seine-et-Marne",
        "78": "Yvelines",
        "91": "Essonne",
        "92": "Hauts-de-Seine",
        "93": "Seine-Saint-Denis",
        "94": "Val-De-Marne",
        "95": "Val-D'Oise",
    }
    # convert "code_postal" to its corresponding name
    df["departement"] = df["departement"].map(department)
    return df["departement"]
