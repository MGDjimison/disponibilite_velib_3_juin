from transform import get_velib_in_paris, get_count_on_boolean_column, get_boolean_columns

paris_df = get_velib_in_paris()
# print(paris_df.dtypes)
print(get_boolean_columns())
print(get_count_on_boolean_column())

