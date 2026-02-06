
-- DATA ENGINEERING ZOOMCAMP: WEEK 3 - DATA WAREHOUSE
-- Database: de_zoomcamp
-- Infrastructure: AWS Athena & S3 (Dataset: Yellow Taxi 2024 Jan-Jun)

-------------------------------------------------------------------------
-- 1. SETUP: EXTERNAL TABLE
-------------------------------------------------------------------------
-- Creating the external table pointing to S3 Parquet files (loaded via Kestra)
CREATE EXTERNAL TABLE IF NOT EXISTS `de_zoomcamp`.`yellow_taxi_2024` (
  `VendorID` INT,
  `tpep_pickup_datetime` TIMESTAMP,
  `tpep_dropoff_datetime` TIMESTAMP,
  `passenger_count` BIGINT,
  `trip_distance` DOUBLE,
  `RatecodeID` BIGINT,
  `store_and_fwd_flag` STRING,
  `PULocationID` INT,
  `DOLocationID` INT,
  `payment_type` BIGINT,
  `fare_amount` DOUBLE,
  `extra` DOUBLE,
  `mta_tax` DOUBLE,
  `tip_amount` DOUBLE,
  `tolls_amount` DOUBLE,
  `improvement_surcharge` DOUBLE,
  `total_amount` DOUBLE,
  `congestion_surcharge` DOUBLE,
  `Airport_fee` DOUBLE
)
STORED AS PARQUET
LOCATION 's3://dez-james-taxi-data-2026/yellow_tripdata_2024/';

-------------------------------------------------------------------------
-- 2. MATERIALIZED TABLE (Internal Storage)
-------------------------------------------------------------------------
-- Creating a regular table from the external one for performance comparison
CREATE TABLE de_zoomcamp.yellow_taxi_2024_regular AS
SELECT * FROM de_zoomcamp.yellow_taxi_2024;

-------------------------------------------------------------------------
-- 3. HOMEWORK QUERIES & ANALYSIS
-------------------------------------------------------------------------

-- Q1: Total record count for 2024 (Jan to June)
-- Result: 20,332,093
SELECT count(*) FROM de_zoomcamp.yellow_taxi_2024;

-- Q2: Estimated bytes scan comparison (PULocationID)
-- External Table: ~13.91 MB (Metadata/Columnar efficiency)
-- Regular Table: ~16.81 MB
SELECT COUNT(DISTINCT(PULocationID)) FROM de_zoomcamp.yellow_taxi_2024;
SELECT COUNT(DISTINCT(PULocationID)) FROM de_zoomcamp.yellow_taxi_2024_regular;

-- Q4: Count records with fare_amount = 0
-- Result: 8,333
SELECT count(*) FROM de_zoomcamp.yellow_taxi_2024 WHERE fare_amount = 0;

-------------------------------------------------------------------------
-- 4. OPTIMIZATION: PARTITIONING & CLUSTERING
-------------------------------------------------------------------------

-- Best Strategy: Partition by tpep_dropoff_datetime and Cluster by VendorID
-- Note: In Athena, massive partitioning during a CTAS might trigger 
-- HIVE_TOO_MANY_OPEN_PARTITIONS if more than 100 writers are required.

-- Optimized table strategy (conceptual/BigQuery style logic):
-- Partitioning reduces scanned data by skipping irrelevant folders.
-- Clustering improves performance by grouping similar VendorIDs within partitions.

-------------------------------------------------------------------------
-- 5. BONUS: METADATA PERFORMANCE (Q9)
-------------------------------------------------------------------------
-- Querying total count from a materialized table typically scans 0 MB
-- because the engine retrieves the count from pre-calculated table metadata.
SELECT count(*) FROM de_zoomcamp.yellow_taxi_2024_regular;