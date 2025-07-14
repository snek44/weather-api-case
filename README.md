# weather-api-case
ELT pipeline storing data into local DuckDB from WeatherAPI.com endpoints.
## Achitecture overview
The chosen architecture follows the ELT principle
- loading raw data into a target data store and keeping all history in raw format, only with normalizations applied where needed (e.g. JSON unnesting).
- once the data is loaded, donwstream transformations are executed (the transformation part would be done with frameworks like dbt, but is out of scope for this exercise and pure SQL files are used)

To load the data, a Python pacakge *dlt* (data load tool) is used:
- it is a Python library that standardizes loading from various sources, with many built-in features like schema/data type inferring and data normalization
- it support multiple DBs as targets, and can be used to work with simple stores like DuckDB in Dev environment, and a production-grade data store (e.g. BigQuery) in production

For orchestration, *Prefect* was chosen as it nicely complements *dlt* with its Python-native code.

This setup doesn't require any DB installations or container management, just clone the git repo and run commands listed below within the Python virtual environment.
All used components only require Python 3.9 or higher to be installed.

## Links to Docs of used tools/libraries:

dlt: https://dlthub.com/docs/reference/installation

DuckDB: https://duckdb.org/docs/stable/clients/python/overview.html

Prefect: https://docs.prefect.io/v3/get-started/install

Jupyter: https://docs.jupyter.org/en/stable/install/notebook-classic.html

## How to install and run
### Initiate Python venv and install dependencies
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Once the environment is initialized, you can run the dlt pipeline either manually or through Prefect schedule with the commands below.

As DuckDB doesn't allow concurrent processes, stop the Schedule if you want to query the DuckDB, or expect potential failed loads (if you keep your DuckDB connection open in Jupyter / another DB IDE).

### Trigger dlt pipeline manually
To trigger the dlt pipeline manually and store data in DuckDB, run the following command.

The input parameter is either 'current' of 'forecast'
```bash
cd elt_pipeline
python dlt_weather_api_to_duckdb.py current
```
To trigger the tranformation pipeline manually, run the following command:
```bash
cd elt_pipeline
python transformation_staging.py
```

### Schedule dlt pipeline with Prefect
To schedule the pipeline with Prefect, first start the Prefect server in the background.
```bash
cd elt_pipeline
prefect server start > ./logs/prefect_server.log 2>&1 &
```
Once started, you should be able to access the UI at: http://127.0.0.1:4200

Then, deploy the Prefect Flow:
```bash
python prefect_elt.py > ./logs/prefect_run_output.log 2>&1 &
```
The Flow is set to be run every 15 minutes in the prefect_elt.py main function.

You should now be able to see the execution runs in the Prefect UI.

### Stop Prefect processes
To stop the schedule, kill the Prefect Flow process:

Find the task ID:
```bash
ps aux | grep "python prefect_elt.py"
```
Kill the ID (replace with correct ID):
```bash
kill 10415
```

To stop the Prefect Server, kill the Prefect Server:

Find the task ID:
```bash
ps aux | grep "prefect server start"
```
Kill the ID (replace with correct ID):
```bash
kill 10157
```

### Open Jupyter notebook to analyze data
Start Jupyter notebook
```bash
cd ../jupyter_files
jupyter notebook
```

Run the query in the *read_latest_weather_forecast.ipynb*  file.
