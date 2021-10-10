## DATAWAREHOUSE ON AWS
### The purpose of this project is to build a datawarehouse to accomodate data of active user activity for music streaming application 'Sparkify'. This data model is implemented on AWS cloud infrastructure with following components -
   >- AWS S3 - Source datasets.
   >- AWS Redshift<br>
        >for staging extracted data<br>
        >for storing the resultant data model (facts and dimensions)<br><br>

### Data model designed for this project consists of a star schema.
### Table and attribute details are -
   >- Fact Table<br>
   > songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent<br><br>
   
   >- Dimension Tables<br>
   > users: user_id, first_name, last_name, gender, level<br>
   > songs: song_id, title, artist_id, year, duration<br>
   > artists: artist_id, name, location, lattitude, longitude<br>
   > time: start_time, hour, day, week, month, year, weekday<br><br>
   
### Source datasets to be extracted into dimension model are -
   > There are two json files for<br>
   >- Song data: s3://udacity-dend/song_data - Data for all songs with their respective artists available in application library.<br>
   >- Log data: s3://udacity-dend/log_data - Data for user events and activity activity on the application.<br><br>
   
   
### Datawarehouse is implemented using PostgreSQL.
### ETL pipeline to extract and load data from source to target is implemented using Python.
<br>
<br>

### TODO steps:
   >- Create sql_queries.py - to design and build tables for proposed data model<br>
   >- Run create_tables.py - to create tables by implementing the database queries from sql_queries.py<br>
   >- Run etl.py - to implement the data pipeline built over the data model which extract, stage and load data from AWS S3 to DWH on AWS Redshift<br>
   >- Design and fire analytical queries on the populated data model to gain insights of user events over streaming application<br>
    
       
   