from transform import get_velib_in_paris, get_station_by_city, get_station_by_department


def load_number_station_by_city():
    df = get_station_by_city()
    return df.to_csv("data/number_station_by_commune.csv", index=True)


def load_number_station_by_department():
    df = get_station_by_department()
    return df.to_csv("data/number_station_by_department.csv", index=True)


def load_velib_in_paris():
    df = get_velib_in_paris()
    return df.to_csv("data/velib_in_paris.csv", index=False)
