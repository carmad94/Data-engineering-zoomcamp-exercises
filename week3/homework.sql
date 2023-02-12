# create or replace external table
CREATE OR REPLACE EXTERNAL TABLE `level-agent-375808.fhv_trip_dataset.external_fhv_trip_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://fhv_trip_data_level_agent/2019/*.csv.gz']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `level-agent-375808.fhv_trip_dataset.fhv_trip_2019_non_partitioned` AS
SELECT * FROM `level-agent-375808.fhv_trip_dataset.external_fhv_trip_2019`;

-- query for question 1
Select count(1) from `level-agent-375808.fhv_trip_dataset.external_fhv_trip_2019`
-- result: 43,244,696

-- query for question 2
Select count(1) from `level-agent-375808.fhv_trip_dataset.external_fhv_trip_2019`
group by Affiliated_base_number
--process 0B

Select count(1) from `level-agent-375808.fhv_trip_dataset.fhv_trip_2019_non_partitioned`
group by Affiliated_base_number
--process 317.94 MB

# answer: 0 MB for the External Table and 317.94MB for the BQ Table

# query for question 3
Select count(1)
 from `level-agent-375808.fhv_trip_dataset.fhv_trip_2019_non_partitioned`
where PUlocationID is null and
DOlocationID is null
# result: 717748

Select distinct(Affiliated_base_number) from `level-agent-375808.fhv_trip_dataset.fhv_trip_2019_non_partitioned`
where DATE(pickup_datetime) BETWEEN "2019-03-01" AND "2019-04-01"
#647.87 MB

CREATE OR REPLACE TABLE `level-agent-375808.fhv_trip_dataset.fhv_trip_2019_partitioned_PUdatetime`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  affiliated_base_number
AS
SELECT * FROM `level-agent-375808.fhv_trip_dataset.fhv_trip_2019_non_partitioned`;

Select distinct(Affiliated_base_number) from `level-agent-375808.fhv_trip_dataset.fhv_trip_2019_partitioned_PUdatetime`
where DATE(pickup_datetime) BETWEEN "2019-03-01" AND "2019-04-01"
order by Affiliated_base_number
# 24.1

#6: GCP Bucket

#7 False