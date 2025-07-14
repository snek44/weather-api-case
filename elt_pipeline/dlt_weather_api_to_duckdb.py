import sys
from typing import Any
from prefect import task

import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources
)

@dlt.source
def dlt_source_weather_api(api_key=dlt.secrets.value, endpoint="current") -> Any:
    # Loads current and forecast data, based on endpoint parameter
    # Supported endpoints: "current", "forecast"

    # Create a REST API configuration for WeatherAPI
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.weatherapi.com/v1/",
            # WeatherAPI uses an API key for authentication as part of the query params:
            # https://www.weatherapi.com/docs/
            "auth": (
                {
                    "type": "api_key",
                    "name": "key",
                    "api_key": api_key,
                    "location": "query"
                }
            ),
            "paginator": {
                "type": "single_page",
            },
        },
        # List all cities from which to load weather data
        "resources": [
            {
                "name": endpoint + "_london",
                "write_disposition": "append",
                "endpoint": {
                    "path": endpoint + ".json",
                    "method": "GET",
                    "params": {
                        "q": "London",
                        "days": 3,
                        "aqi": "no",
                        "alerts": "no",
                        "lang": "en"
                    },
                }
            },
            {
                "name": endpoint + "_prague",
                "write_disposition": "append",
                "endpoint": {
                    "path": endpoint + ".json",
                    "method": "GET",
                    "params": {
                        "q": "Prague",
                        "days": 3,
                        "aqi": "no",
                        "alerts": "no",
                        "lang": "en"
                    },
                }
            }
        ]
    }

    yield from rest_api_resources(config)


@task
def dlt_load_weather_api(endpoint="current") -> None:
    # Invokes the dlt pipeline to load data from the WeatherAPI into DuckDB
    pipeline = dlt.pipeline(
        pipeline_name="weather_api_to_duckdb",
        destination=dlt.destinations.duckdb(".duckdb"),
        dataset_name="dlt_weather_api",
    )

    load_info = pipeline.run(dlt_source_weather_api(endpoint=endpoint))
    print(load_info)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in {"current", "forecast"}:
        endpoint = sys.argv[1]
    else:
        print("Usage: python dlt_weather_api_to_duckdb.py [current|forecast]")
        sys.exit(1)

    # Run the task function (bypassing Prefect orchestration)
    dlt_load_weather_api.fn(endpoint=endpoint)
