# weather-api-case
ELT pipeline storing data into local DuckDB from WeatherAPI.com endpoints.
This setup doesn't require any DB installations or container management, just clone the git repo and run commands listed below.
All used components are Python-based/compatible and require only Python 3.9 or higher to be installed.

dlt: https://dlthub.com/docs/reference/installation
DuckDB: https://duckdb.org/docs/stable/clients/python/overview.html
Prefect: https://docs.prefect.io/v3/get-started/install
Jupyter: https://docs.jupyter.org/en/stable/install/notebook-classic.html

# How to install and run
## Initiate Python venv and install dependencies
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```


