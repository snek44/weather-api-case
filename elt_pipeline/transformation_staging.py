from prefect import task
import duckdb

@task
def run_ddl() -> None:
    filename = "weather_api_latest_forecast.sql"
    # Invokes any ddl on top of raw data
    with duckdb.connect("../.duckdb") as db:
        with open("../sql_ddl/" + filename) as f:
            sql = f.read()
            print("Executing sql file: " + filename)
            db.execute(sql)

if __name__ == "__main__":
    # Run the task function (bypassing Prefect orchestration)
    run_ddl.fn()
