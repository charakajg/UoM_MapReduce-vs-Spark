DROP TABLE IF EXISTS flightdelays;
CREATE EXTERNAL TABLE flightdelays (id int, year int, month int, day_of_month int, day_of_week int, dep_time int, crs_dep_time int, arr_time int, crs_arr_time int, unique_carrier string, flight_num string, tail_num string, actual_elapsed_time int, crs_elapsed_time int, air_time int, arr_delay int, dep_delay int, origin string, dest string, distance int, taxi_in int, taxi_out int, cancelled int, cancellation_code string, diverted int, carrier_delay int, weather_delay int, nas_delay int, security_delay int, late_aircraft_delay int)
    row format delimited fields terminated by ','
    LOCATION 's3://bda-test-data-1/demo/input'
    TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM flightdelays LIMIT 10;

SELECT year, avg(arr_delay) FROM flightdelays group by year;

INSERT OVERWRITE DIRECTORY 's3://bda-test-data-1/demo/mapreduce_output/average_arrival_delay_by_year/'
    row format delimited fields terminated by ','
    SELECT year, avg(arr_delay) FROM flightdelays group by year;

SELECT year, sum(carrier_delay) FROM flightdelays group by year;
SELECT year, sum(nas_delay) FROM flightdelays group by year;
SELECT year, sum(weather_delay) FROM flightdelays group by year;
SELECT year, sum(late_aircraft_delay) FROM flightdelays group by year;
SELECT year, sum(security_delay) FROM flightdelays group by year;
