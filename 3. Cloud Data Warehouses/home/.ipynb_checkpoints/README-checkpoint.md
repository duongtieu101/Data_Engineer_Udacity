Overview:
    - This project is used to collect, extract, tranform and load data related to music and user's event to analyze song play of user.
    - We use AWS cloud to store and do ETL job.
    - We will build an ETL pipeline using Python and use Postgres database with tables designed to optimize queries on song play analysis.
    - To complete the project, we define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from S3 to Redshift, after that we push data to these tables (fact and dimension tables) in AWS Redshift using Python and PostgreSQL.
    
    
We have 2 tables (staging_events, staging_songs) that pull data from S3 to Redshift before create the data warehouse.
About Schema for Song Play Analysis:
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


    
How to run the Python scripts:
    - Create IAM_ROLE, Security group, Cluster subnet group, Redshift cluster and config them to dwh.cfg.
    - Install Python3 environment.
    - Install postgresql (psycopg2) libraries.
    - Run command line:
        + python create_tables.py
        + python etl.py


All folders/files are used in this project:
    - create_tables.py drops and creates your tables. Run this file to reset your tables before each time you run your ETL scripts.
    - etl.py is where load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
    - sql_queries.py contains all sql queries, and is imported into the last two files above.
    - README.md provides informantion about project.
