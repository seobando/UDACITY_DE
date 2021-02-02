# PROJECT - DATA MODELLING WITH POSTGRESSQL

The technologies learning in this project were PostgresSQL for developing ETLs and storage.

## PURPOSE 

This is a Data Modeling exercise for a startup called Sparkify that wants to analyze the data they've been collecting on songs and user activity on their new music streaming app to answer the question: Which are the songs that the users listen to?

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

There are 7 files in this project: 

1. data -> All the data files needed to be inserted and processed on Postgres.
2. sql_queries.py -> The scripts declare the drop, create, and insert processes.
3. create_tables.py -> The creation table script
4. etl.py -> The ETL process script.
5. test.ipynb -> A testing Jupiter notebook to check if the creation and etl process are correct
6. etl.ipynb -> Is a template for develop step by step ETLs.
7. Readme.md 

## FOLLOW NEXT INSTRUCTION TO RUN THE PROJECT

0. Make sure that you have a Postgres connection. 
1. Run the create_table.py
2. Run the etl.py
3. Run the test.ipynb