USE DATABASE final;
USE SCHEMA final.public;

--Create joined test table
CREATE OR REPLACE TABLE final.public.sparktbl
USING template (
    SELECT array_agg(object_construct(*))
    FROM TABLE(
        infer_schema(
          location=>'@project_stage/sparktbl.parquet',  
          file_format=>'parquet_format'
        )
    )
);

--Load data 
COPY INTO final.public.sparktbl
FROM @project_stage/sparktbl.parquet
file_format = parquet_format
match_by_column_name = case_insensitive;


--Create refined table
CREATE OR REPLACE TABLE final.public.flightwx AS (
SELECT s.*,
       EXTRACT(year FROM dep_time_utc) AS year,
       EXTRACT(month FROM dep_time_utc) AS month,
       EXTRACT(day FROM dep_time_utc) AS day,
       EXTRACT(dayofweek FROM dep_time_utc) AS dow,
       DAYNAME(dep_time_utc) AS dayname,
       CASE WHEN EXTRACT(dayofweek FROM dep_time_utc) IN (0,6) THEN 1
            ELSE 0 END AS is_weekend,
       CASE WHEN EXTRACT(dayofweek FROM dep_time_utc) NOT IN (0,6) THEN 1
            ELSE 0 END AS is_weekday,
       CASE 
        WHEN EXTRACT(month FROM dep_time_utc) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(month FROM dep_time_utc) IN (6,7,8) THEN 'Summer'
        WHEN EXTRACT(month FROM dep_time_utc) IN (9,10,11) THEN 'Autumn'
        WHEN EXTRACT(month FROM dep_time_utc) IN (12,1,2) THEN 'Winter' END AS season    
FROM final.public.sparktbl s
);

--Create aggregated view
CREATE OR REPLACE VIEW final.public.measures_by_locale AS (
WITH agg AS (
SELECT d.origin,
       d.dest,
       d.airport_name_origin,
       d.airport_name_dest,
       d.op_carrier,
       d.op_carrier_name,
       d.longitude_origin,
       d.latitude_origin,
       d.latitude_dest,
       d.longitude_dest,
       AVG(d.dep_delay) AS avg_dep_delay,
       COUNT(1) AS total_flight_cnt,
       SUM(CASE WHEN d.dep_delay > 15 THEN 1 ELSE 0 END) AS delayed_cnt, 
       SUM(d.cancelled) AS cancelled_cnt,
       SUM(d.diverted) AS diverted_cnt,
       SUM(CASE WHEN d.dep_delay > 15 THEN 1 ELSE 0 END) / COUNT(1) AS delay_ratio,
       SUM(d.cancelled)/COUNT(1) AS cancelled_ratio,
       SUM(d.diverted)/COUNT(1) AS diverted_ratio,
FROM final.public.flightwx d
GROUP BY GROUPING SETS (
(),
(origin, airport_name_origin, longitude_origin, latitude_origin),
(dest, airport_name_dest, longitude_dest, latitude_dest),
(op_carrier, op_carrier_name),
(op_carrier, op_carrier_name, origin, airport_name_origin, longitude_origin, latitude_origin),
(op_carrier, op_carrier_name, origin, airport_name_origin, longitude_origin, latitude_origin, dest, airport_name_dest, longitude_dest, latitude_dest)))

SELECT NVL(origin, 'ALL') AS origin,
       NVL(dest, 'ALL') AS dest,
       NVL(airport_name_origin, 'ALL') AS airport_name_origin,
       NVL(airport_name_dest, 'ALL') AS airport_name_dest,
       NVL(op_carrier_name, 'ALL') AS op_carrier_name,
       NVL(op_carrier, 'ALL') AS op_carrier,
       longitude_origin AS longitude_origin,
       latitude_origin AS latitude_origin,
       longitude_dest AS longitude_dest,
       latitude_dest AS latitude_dest,
       total_flight_cnt, delayed_cnt, delay_ratio, 
       cancelled_cnt, cancelled_ratio, diverted_cnt, diverted_ratio
FROM agg);

CREATE OR REPLACE VIEW final.public.measures_by_time AS (
SELECT d.year,
       d.month,
       d.day,
       d.dayname,
       d.season,
       AVG(d.dep_delay) AS avg_dep_delay,
       COUNT(1) AS total_flight_cnt,
       SUM(CASE WHEN d.dep_delay > 15 THEN 1 ELSE 0 END) AS delayed_cnt, 
       SUM(CASE WHEN d.dep_delay > 15 THEN 1 ELSE 0 END) / COUNT(1) AS delay_ratio,
       SUM(d.cancelled) AS cancelled_cnt,
       SUM(d.cancelled)/COUNT(1) AS cancelled_ratio,
       SUM(d.diverted) AS diverted_cnt,
       SUM(d.diverted)/COUNT(1) AS diverted_ratio,
FROM final.public.flightwx d
GROUP BY GROUPING SETS ((season),(dayname),(day),(month),(year)));
