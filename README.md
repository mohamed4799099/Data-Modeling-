# Data-Modeling-
# Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
# Description
first , I read all json files from the streaming app then upload them in data Frames using pandas library in python then upload them in tables which designed in this project ,
the tables in Sparkify DATABASE divided into fact table (songplays_table ) and dimensions_table (songs,users,time,artist) and after that you can doing any query you need in this relational database .
# scripts
create_tables.py: Clean previous schema and creates tables.
sql_queries.py: All queries used in the ETL pipeline.
etl.py: Read JSON logs and JSON metadata and load the data into generated tables.
# schema
![DEND Project 1 ERD](https://user-images.githubusercontent.com/61945708/137330138-b3b8dc03-c1b3-4d32-845f-f04c6ad1f8b7.png)
