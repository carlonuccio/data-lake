#  Data Lake
***
## Udacity Data Engineer Nano Degree Project 4
***
### Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I'm tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow to the analytics team to continue finding insights in what songs their users are listening to.

### The goal
***
The purpose of this project is to build an ETL pipeline for a data lake hosted on S3. To complete the project, I will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. I'll deploy this Spark process on a cluster using AWS.


### Schema for Song Play Analysis
***
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table:
**songplays** - records in log data associated with song plays i.e. records with page **NextSong**\
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, month, year

#### Dimension Tables
**users** - users in the app\
*user_id, first_name, last_name, gender, level*\
**songs** - songs in music database\
*song_id, title, artist_id, year, duration*\
**artists** - artists in music database\
*artist_id, name, location, latitude, longitude*\
**time** - timestamps of records in songplays broken down into specific units\
*start_time, hour, day, week, month, year, weekday*

