from dagster_duckdb import DuckDBResource

import dagster as dg

database_resource = DuckDBResource(database="src/disponibilite_velib_etl/defs/data/velib_duckdb.db")


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
        }
    )