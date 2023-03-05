DROP TABLE IF EXISTS flightdelays;
CREATE EXTERNAL TABLE flightdelays (id int, year int, month int, day_of_month int, day_of_week int, dep_time int, crs_dep_time int, arr_time int, crs_arr_time int, unique_carrier string, flight_num string, tail_num string, actual_elapsed_time int, crs_elapsed_time int, air_time int, arr_delay int, dep_delay int, origin string, dest string, distance int, taxi_in int, taxi_out int, cancelled int, cancellation_code string, diverted int, carrier_delay int, weather_delay int, nas_delay int, security_delay int, late_aircraft_delay int)
    row format delimited fields terminated by ','
    LOCATION 's3://bda-test-data-1/input/'
    TBLPROPERTIES ("skip.header.line.count"="1");
INSERT OVERWRITE DIRECTORY 's3://bda-test-data-1/mapreduce_output/total_late_aircraft_delay_per_year/'
    row format delimited fields terminated by ','
    SELECT year, sum(late_aircraft_delay) FROM flightdelays group by year;
