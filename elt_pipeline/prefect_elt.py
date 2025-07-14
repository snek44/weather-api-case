from prefect import flow
from dlt_weather_api_to_duckdb import dlt_load_weather_api
from transformation_staging import run_ddl

@flow
def prefect_elt():
    # This flow is responsible for extracting data from all our sources, and to run any downstream tasks

    ## Run extract-load tasks
    dlt_load_weather_api(endpoint="current")
    dlt_load_weather_api(endpoint="forecast")

    ## Run any downstream transformations
    run_ddl()
    
if __name__ == "__main__":
    # If run locally, schedule every 15 minutes
    prefect_elt.serve(name="local-deployment",cron="*/15 * * * *")