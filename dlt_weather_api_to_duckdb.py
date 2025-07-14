from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources
)

@dlt.source
def dlt_source_weather_api(api_key=dlt.secrets.value) -> Any:
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
        "resources": [
            # List all endpoints to be processed
            {
                "name": "forecast",
                "write_disposition": "append",
                "endpoint": {
                    "path": "forecast.json",
                    "method": "GET",
                    "params": {
                        "q": "London",
                        "days": 3,
                        "aqi": "no",
                        "alerts": "no",
                        "lang": "en"
                    },
                },
            },
            {
                "name": "current",
                "write_disposition": "append",
                "endpoint": {
                    "path": "current.json",
                    "method": "GET",
                    "params": {
                        "q": "London",
                        "days": 3,
                        "aqi": "no",
                        "alerts": "no",
                        "lang": "en"
                    },
                },
            },
        ]
    }

    yield from rest_api_resources(config)


def dlt_load_weather_api() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="weather_api_to_duckdb",
        destination=dlt.destinations.duckdb(".duckdb"),
        dataset_name="dlt_weather_api",
    )

    load_info = pipeline.run(dlt_source_weather_api())
    print(load_info)

if __name__ == "__main__":
    dlt_load_weather_api()
