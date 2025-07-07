from transform import (
    add_department,
    get_transformed_data,
)

if __name__ == "__main__":
    cleaned_df = get_transformed_data()
    cleaned_df["departement"] = add_department()
    print(cleaned_df.head())
