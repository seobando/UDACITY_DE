# PROJECT - DATA WAREHOUSE

The technologies learning in this project were Redshift for Data Warehousing.

## PURPOSE

This is the third udacity nanodegree project, which consists of developing a datawarehouse for a music streaming startup called Sparkify. The objective is to take the input data sets and through a series of ETLs convert them into a fact table and a series of dimensional tables to obtain the star shape schema required to implement the analytics required by the Sparkify team and stored them on Redshift.

## DATASETS

The approach used to solve this was the "Star Schema" Data Modelling, that's a method in Data Engineer used for developing fact tables, it means tables that contains dimensions and metrics in an aggregate way to make easy the analytic process.

For that purpose there is Input Data to be transformed into an start schema (output data), as show next:

- Input Data:

1. Song Dataset - Complete details and metadata about the song
2. Log Dataset - User activity log

- Output Data:

    - Fact Table
    
        - songplays - records in log data associated with song plays i.e. records with page NextSong: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    - Dimension Tables

        - users - users in the app: user_id, first_name, last_name, gender, level
        - songs - songs in music database: song_id, title, artist_id, year, duration
        - artists - artists in music database: artist_id, name, location, lattitude, longitude
        - time - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday
        
## FILES:

There are 5 files in this project: 

1. sql_queries.py -> The scripts declare the drop, create, and insert processes.
2. create_tables.py -> The creation table script
3. etl.py -> The ETL process script.
4. dwh.cfg -> The database autorization credentials and configuration parameters
5. Readme.md 

## FOLLOW NEXT INSTRUCTION TO RUN THE PROJECT

0. Make sure that you have a Redshift connection. 
1. Run the create_table.py
2. Run the etl.py

## DISCLAIMER

Pay attention to your cluster region, have to be in the same region of your bucket in order to work.