from transform import (
    get_transformed_data,
    add_department,
    get_average_of_columns_by_departement,
    get_average_of_columns_by_commune,
)

transformed_velib_df = get_transformed_data()
print("commune" in transformed_velib_df.columns)
transformed_velib_df["departement"] = add_department()

average_numerical_data_by_departement_df = get_average_of_columns_by_departement()
# print(average_numerical_data_by_departement_df)

average_numerical_data_by_commune_df = get_average_of_columns_by_commune()
print(average_numerical_data_by_commune_df)
