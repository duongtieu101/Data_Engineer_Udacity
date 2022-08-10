# Data Modeling with Postgres
## Overview:
This project is used to collect, extract, tranform and load data related to music and user's event to analyze song play of user.

We will build an ETL pipeline using Python and use Postgres database with tables designed to optimize queries on song play analysis.

To complete the project, we define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL
    
    
## About Schema for Song Play Analysis:
    - Fact Table:
        + songplays - records in log data associated with song plays i.e. records with page NextSong
            (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent).
    - Dimension Tables:
        + users - users in the app
            (user_id, first_name, last_name, gender, level)
        + songs - songs in music database
            (song_id, title, artist_id, year, duration)
        + artists - artists in music database
            (artist_id, name, location, latitude, longitude)
        + time - timestamps of records in songplays broken down into specific units
            (start_time, hour, day, week, month, year, weekday)


    
## How to run the Python scripts:
    - Install Python3 environment.
    - Install pandas, postgresql (psycopg2) libraries.
    - Run command line:
        + python create_tables.py
        + python etl.py


## All folders/files are used in this project:
    - data folder: contain song and log data.
    - test.ipynb displays the first few rows of each table to let you check your database.
    - create_tables.py drops and creates your tables. Run this file to reset your tables before each time you run your ETL scripts.
    - etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables
    - etl.py reads and processes files from song_data and log_data and loads them into your tables.
    - sql_queries.py contains all your sql queries, and is imported into the last three files above.
    - README.md provides informantion about project.
