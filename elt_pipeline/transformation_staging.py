from prefect import task
import duckdb

@task
def run_ddl() -> None:
    # Invokes any ddl on top of raw data
    with duckdb.connect("../.duckdb") as db:
        with open('../sql_ddl/weather_api_latest_forecast.sql') as f:
            sql = f.read()
            db.execute(sql)

if __name__ == "__main__":
    # Run the task function (bypassing Prefect orchestration)
    run_ddl.fn()
