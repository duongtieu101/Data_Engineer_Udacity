import pandas as pd
import pyspark
import configparser
import os
from datetime import datetime, timedelta
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *




def create_spark_session():
    """Create spark session.
    Input:
        None.
    Output:
        spark: is the spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def rename_columns(df, new_columns):
    """Rename columns.
    Input:
        Data frame that needs to rename columns.
        List of new columns name.
    Output:
        Data frame with new columns name.
    """
    for original, new in zip(df.columns, new_columns):
        df = df.withColumnRenamed(original, new)
    return df

def to_date_func(date):
    """Process date numeric field, convert it to Date type.
        Input:
            date: date field that needs to convert data type.
        Output:
            New field with new format: Date.
    """
    if date is not None:
        return (datetime(1960,1,1) + timedelta(days=int(date)))
to_date_func_udf = udf(to_date_func, DateType())

def process_immigration_data(spark, input_data, output_data):
    """Process immigration data to get fact_immigration, 
    dim_time and dim_personal_information tables.
        Input:
            spark: is the spark session.
            input_data: Source of data path.
            output_data: Target S3 endpoint.
        Output:
            fact_immigration: data frame that contains information about fact table.
            dim_time: data frame that conntains information about time dimension table.
            dim_personal_information: data frame that conntains personal information of customers.
    """
    
    print("Start processing immigration.")
     
    # read immigration data file (can limit 100 records for sample)
    immigration_data_path = input_data + 'sas_data'
    df = spark.read.parquet(immigration_data_path)
    # remove records that have null value from cicid, arrdate and depdate columns
    df = df.filter(df.cicid.isNotNull() & \
                   df.arrdate.isNotNull() & \
                   df.depdate.isNotNull())
    # extract columns to create fact_immigration table
    fact_immigration = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', \
                                 'arrdate', 'depdate', 'i94mode', 'i94visa').distinct() \
                         .withColumn("immigration_id", monotonically_increasing_id())
    # rename to meaningful columns name
    new_columns = ['cic_id', 'arrival_year', 'arrival_month', 'city_code', 'state_code',\
                   'arrival_date', 'departure_date', 'mode', 'visa']
    fact_immigration = rename_columns(fact_immigration, new_columns)
    # add new column: country
    fact_immigration = fact_immigration.withColumn('country', lit('United States'))
    # convert date columns (arrival and departure) to date type
    fact_immigration = fact_immigration.withColumn('arrival_date', \
                                        to_date_func_udf(col('arrival_date')))
    fact_immigration = fact_immigration.withColumn('departure_date', \
                                        to_date_func_udf(col('departure_date')))
    # save to AWS S3
    fact_immigration.write.mode("overwrite").partitionBy('state_code') \
                    .parquet(path=output_data + 'fact_immigration')
    print("Done create fact_immigration table on AWS S3")
    
    # extract all date date from date columns (arrival_date and departure_date) to create dim_time table
    arrival_date = fact_immigration.select('arrival_date').rdd.flatMap(lambda x: x).collect()
    departure_date = fact_immigration.select('departure_date').rdd.flatMap(lambda x: x).collect()
    date = arrival_date + departure_date
    time_df = spark.createDataFrame(date, DateType())
    time_df = rename_columns(time_df, ['date'])
    # extract day, week, month, year, weekday from date field
    # weekday: 0: Sun, 1: Mon, 2: Tue...
    dim_time = time_df.select('date') \
                .withColumn('day', dayofmonth(time_df['date'])) \
                .withColumn('week', weekofyear(time_df['date'])) \
                .withColumn('month', month(time_df['date'])) \
                .withColumn('year', year(time_df['date'])) \
                .withColumn('weekday', (dayofweek(time_df['date'])+5)%7+1) \
                .distinct()
    # save dim_time table to AWS S3
    dim_time.write.mode("overwrite")\
                     .parquet(path=output_data + 'dim_time')
    print("Done create dim_time table on AWS S3")
    
    # extract columns to create dim_personal_information table
    dim_personal_information = df.select('cicid', 'i94cit', 'i94res', \
                                  'biryear', 'gender').distinct() \
                          .withColumn("immi_personal_id", monotonically_increasing_id())
    
    # rename to meaningful columns name
    new_columns = ['cic_id', 'citizen_country', 'residence_country', \
                   'birth_year', 'gender']
    dim_personal_information = rename_columns(dim_personal_information, new_columns)

    # save dim_personal_information table to AWS S3
    dim_personal_information.write.mode("overwrite") \
                     .parquet(path=output_data + 'dim_personal_information')
    print("Done create dim_personal_information table on AWS S3")
    return fact_immigration, dim_time, dim_personal_information

def process_us_cities_demographics_data(spark, input_data, output_data):
    """ Process us cities demographics data to get dim_state table.
        Input:
            spark: is the spark session.
            input_data: Source of data path.
            output_data: Target S3 endpoint.
        Output:
            dim_state: data frame that contains all information about state.
    """
    print("Start processing demographics.")
    # read demographics data file
    demographics_data_path = input_data + 'us-cities-demographics.csv'
    df = spark.read.options(header=True, delimiter=';').csv(demographics_data_path)
    # extract columns to create dim_state table
    dim_state = df.select('State Code', 'State', \
                          'Median Age', 'Male Population', \
                          'Female Population', 'Total Population')
    new_columns = ['state_code', 'state_name', 'median_age', 'male_population', 'female_population', 'total_population']
    dim_state = rename_columns(dim_state, new_columns)
    # remove records that have null value from state_code column
    dim_state = dim_state.filter(dim_state.state_code.isNotNull())
    # calculate median_age, male_population, female_population, total_population for each state
    dim_state = dim_state.groupBy("state_code", "state_name") \
        .agg(expr('percentile(median_age, array(0.5))')[0].alias('median_age'),
          sum("male_population").alias("male_population"), 
          sum("female_population").alias("female_population"), 
          sum("total_population").alias("total_population"))
    
    dim_state.write.mode("overwrite")\
                     .parquet(path=output_data + 'dim_state')
    print("Done create dim_state table on AWS S3")
    return dim_state

def check_data_quality( spark, all_df):
    """Check data quality of all dimension and fact tables.
    Input:
        spark: is the spark session.
        all_tables: dictionary of data frames (tables) that need to check data quality.
    Output:
        None
    """
    print("Start to validate data:")
    for df_name, df in all_df.items():
        num_records = df.count()
        if num_records <= 0:
            raise ValueError(f"Table {df_name} is empty!")
        else:
            print(f"Table: {df_name} is not empty: total {num_records} records.")
        
        num_null_key_records = df.filter(df[df.columns[0]].isNull()).count()
        if num_null_key_records > 0:
            raise ValueError(f"Table {df_name} have null value for {df.columns[0]} key column !")
        else:
            print(f"Table {df_name} does not have null value for {df.columns[0]} key column .")

        print("Schema of this table:")
        df.printSchema()
    
def main():
    # AWS CONFIG
    config = configparser.ConfigParser()
    config.read('capstone.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    input_data = config['S3']['SOURCE']
    output_data = config['S3']['DEST']

    spark = create_spark_session()

    fact_immigration, dim_time, dim_personal_information = process_immigration_data( \
                                                                    spark, \
                                                                    input_data, \
                                                                    output_data)
                                                                                    
    dim_state = process_us_cities_demographics_data(spark, input_data, output_data)
    
    # get dictionary of data frames to check data quality
    all_df = {'fact_immigration': fact_immigration, \
              'dim_time': dim_time, \
              'dim_personal_information': dim_personal_information, \
              'dim_state': dim_state}
    
    check_data_quality(spark, all_df)
    
    
if __name__ == "__main__":
    main()