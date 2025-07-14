CREATE OR REPLACE VIEW staging.weather_api_latest_forecast AS
(
/* 
 * In the raw data produced by dlt, forecasts for the same time_epoch are appended with each load
 * Based on _dlt logs insert timestamps, we can sort the forecasts to retrieve the latest one
*/
WITH forecasts_ordered AS
(
	SELECT 
		flh.time_epoch
		, 'London' AS city
		, flh.temp_c 
		, flh.feelslike_c 
		, flh.condition__text
		, flh.chance_of_rain 
		, ROW_NUMBER() OVER(PARTITION BY flh.time_epoch ORDER BY dl.inserted_at DESC) AS forecast_rn
	FROM dlt_weather_api.forecast_london AS fl 
	INNER JOIN dlt_weather_api.forecast_london__hour AS flh 
		ON fl."_dlt_id" = flh."_dlt_parent_id" 
	INNER JOIN dlt_weather_api."_dlt_loads" AS dl 
		ON fl."_dlt_load_id" = dl.load_id
		
	UNION ALL 
	
	SELECT 
		flh.time_epoch 
		, 'Prague' AS city
		, flh.temp_c 
		, flh.feelslike_c 
		, flh.condition__text
		, flh.chance_of_rain 
		, ROW_NUMBER() OVER(PARTITION BY flh.time_epoch ORDER BY dl.inserted_at DESC) AS forecast_rn
	FROM dlt_weather_api.forecast_prague AS fl 
	INNER JOIN dlt_weather_api.forecast_prague__hour AS flh 
		ON fl."_dlt_id" = flh."_dlt_parent_id" 
	INNER JOIN dlt_weather_api."_dlt_loads" AS dl 
		ON fl."_dlt_load_id" = dl.load_id 
)

SELECT
	to_timestamp(time_epoch) AT TIME ZONE 'UTC' AS forecast_time_utc -- convert epoch to utc timestamp
	, city
	, temp_c
	, feelslike_c
	, condition__text
	, chance_of_rain
FROM forecasts_ordered
WHERE forecast_rn = 1 -- get the latest forecast for each hour
	AND forecast_time_utc <= NOW() AT TIME ZONE 'UTC' + INTERVAL 2 DAY -- filter for forecasts up to 2 days in the future
ORDER BY 1 DESC, 2 DESC
);