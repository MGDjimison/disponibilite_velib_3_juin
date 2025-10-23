from load import create_velib_table, get_cursor

if __name__ == "__main__":
    create_velib_table()
    cur = get_cursor()
    res = cur.execute("SELECT nom_station FROM velib")
    print(res.fetchone())


