# Capstone Project

## Quick start
After installing python3 + Apache Spark (pyspark) libraries and dependencies, run from command line:
- python3 etl.py

## Step 1: Scope the Project and Gather Data:
This Project process US I94 immigration data and us-cities-demographics data as a support to create a datawarehouse with fact and dimension table.
The purpose of this project is provide tools to automatically process US I94 Immigration data in a flexible way and help answering questions like:
    - What number of immigrants go to Maryland state?
    - What number of immigrants on Saturday/Monday...?
    ...
    Please refer last three cells of explore_data.ipynb file to see these examples.
The input data is in this workspace (locally): Immigration data is in parquet format, us-cities-demographics data is in CSV format. 
The output data (data lake) will be saved on AWS S3 as parquet format.

### Data sets:
- sas_data folder: contains all parquet files
    - Source: US I94 immigration data from 2016 (in sas_data folder): https://travel.trade.gov/research/reports/i94/historical/2016.html
    - Format: Parquet
    - Description: 3096313 records (> 1 milion). Data contains information about international visitor arrival like: cicid, time (year, month, arrdate, departure date), region, biryear, gender, admission number, visa type...

- us-cities-demographics.csv:
    - Source: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/
    - Format: CSV
    - Description: This dataset contains information about the demographics of all US cities.
### Tools:
- AWS S3: data storage because it's scalable, reliable, fast, inexpensive data storage infrastructure
- Python for data processing: PySpark - explore and process data because Spark can process a large data very fast.
## Step 2: Explore and Assess the Data
### Explore the Data:
- With CSV format (immigration_data_sample.csv, us-cities-demographics.csv), I can check directly from this file.
- Use Pyspark for view, explore to get overview data more detail.
- Use PySpark on one of the SAS data sets to test ETL data pipeline logic
### Process the Data:
- Change columns name for better understanding
- With immigration_data: 
    - Remove records that have null value from cicid, arrdate and depdate columns
    - Add new column: country
    - Convert date columns (arrival and departure) to date type
    - Extract all date date from date columns (arrival_date and departure_date), after that, extract day, week, month, year, weekday to create dim_time table
- With us_cities_demographics_data:
    - Remove records that have null value from state_code column
    - Calculate median_age, male_population, female_population, total_population for each state
- Please refer to explore_date.ipynb file.
## Step 3: Define the Data Model    
### Conceptual Data Model
- Star Schema:
Please refer to images/star_schema_model.PNG file if some issues occur.
![This is a alt text.](https://drive.google.com/file/d/1dYezndrpf1Dr8Joc6OQpnHfGnteHJHLv/view?usp=sharing "This is a star schema model image.")
We use star schema for this project because when we want to query to create a report, this schema allows for the relaxation of these rules and makes queries easier with simple JOINS, and aggregations perform calculations and clustering of our data. Examples : COUNT, SUM, GROUP BY etc
## Step 4: Run Pipelines to Model the Data
### Data Pipeline Build Up Steps
1. Assume all data sets are stored in this workspace (locally):
    - sas_data folder
    - us-cities-demographics.csv file
2. Process the Data following to step 2
3. Transform immigration data to 1 fact table and 2 dimension tables, fact table will be partitioned by state.
4. Tranform us-cities-demographics data to 1 dimension table
5. Store these tables to S3 bucket
6. Check data quality: 
    - Check number of records of each table to make sure that there is no empty table after running ETL data pipeline.
    - Check number of records of each table which the condition key column is null to make sure that there is no NULL value in key column.
    - Data schema of each table
### Data dictionary
#### Please refer to data_dictionary.json file of this work space.
## Step 5: Complete Project Write Up
### Tools and Technologies:
1. AWS S3 for data storage
2. Python for data processing: PySpark - explore and process data.
### How often ETL job should be run:
- ETL script should be run monthly basis (assuming that new I94 data is available once per month).
### Other scenarios:
1. If the data was increased by 100x:
- We should store input data on AWS S3
- If Spark with standalone server mode can not process 100x data set, we could consider to put data in AWS EMR which is a distributed data cluster for processing large data sets on cloud
- Output data (parquet files) also should need to stored on AWS S3
2. If the pipelines were run on a daily basis by 7am.
- We can set up Apache Airflow job to run ETL pipeline every 7am 
3. If the database needed to be accessed by 100+ people.
- AWS Redshift can handle large connections, so we can move database to AWS Redshift











