# 1) Introduction

The startup company Sparkify launched a new music streaming app and wants to get insights about the music preferences of their users, in particular Sparkify wants to understand what songs users are listening to.

The goal of this project is to build a consistent Amazon Redshift Data Warehouse which will serve as a single source of truth for data analysis.
A Python/SQL ETL pipeline is designed to update the database with the collected data which currently resides in an AWS S3 bucket of JSON logs on user activity, as well as a directory with JSON metadata on the songs.

The ETL pipeline is supported by a fully automated Apache Airflow workflow including a node for data quality monitoring.

# 2) Repository
### Structure: 

**Pipeline:** </br>
https://github.com/marcus-repo/etl-airflow

```
etl-airflow
|-- dags
|	|-- create_tables.sql
|	|-- etl.py
|-- plugins
	|-- __init__.py
	|-- helpers
	|	|-- __init__.py
	|	|-- sql_queries.py
	|-- operators
		|-- __init__.py
		|-- data_quality.py
		|-- load_dimension.py
		|-- load_fact.py
		|-- stage_redshift.py
```

**Source Data:** </br>
*AWS S3 Bucket:*
https://s3.console.aws.amazon.com/s3/buckets/udacity-dend

```
udacity-dend
|-- log-data/
|-- song-data/
|-- log_json_path.json
```

### Explanation:

**Pipeline:** </br>
- **create_tables.py:** *python script to create the AWS Redshift database tables.*
- **etl.py:** *Airflow-Python script which reads the JSON sourcefiles and inserts it into the AWS Redshift database tables.*
- **README.md** *Describes the project.*
- **sql_queries.py:** *Contains sql statements to load data from staging to destination tables.*
- **operators (folder):**  *Airflow custom operators used for data loading and quality checks.*

**Source Data:** </br>
- log-data/ and song-data/ contains JSON files collected from the app.</br>
- log_json_path.json is a JSONPaths file to parse the JSON source data for log_data/. Link to AWS explanation:</br>
https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html

# 3) Airflow Workflow

![](Airflow_DAG.png)

### Airflow Benefits:
- Programatic Workflow Development
- Task Dependency Managment
- Easy to use Monitoring Interface
- Retry Policy for Workflows
- SLA (Service Level Agreements)
- Alerting System (e. g. email notification on failure)
- Easy AWS integration e. g. MWAA


# 4) Database and Schema
The purpose of the "sparkifydb" database is data analysis, hence a dimensional model (STAR Schema) is used to allow for optimized data reading and data aggregation.

### Benefits of Amazon Redshift
- Cloud Managed
- Massively Parallel Processing
- Column Oriented Storage
- Easy to scale
- Based on Postgres (well established)

### Benefits of the STAR Schema
- Simplifies Queries (less joins needed)
- Fast Aggregation (e. g. sums, counts)
- Easy to understand

### Tables
The schema includes the following tables and fields: </br>
#### Staging Tables
***staging_events***
- artist
- auth
- firstname
- gender
- iteminsession
- lastname
- length
- "level"
- location
- "method"
- page
- registration
- sessionid
- song
- status
- ts
- useragent
- userid

***staging_songs***
- num_songs
- artist_id
- artist_name
- artist_latitude
- artist_longitude
- artist_location
- song_id
- title
- duration
- year

#### Fact Table
***songplays***
- playid (PK)
- start_time (FK)
- userid (FK)
- songid (FK)
- artistid (FK)
- level
- sessionid
- location
- user_agent

#### Dimension Tables
***users***
- userid (PK)
- first_name
- last_name
- gender
- level

***songs***
- songid (PK)
- title
- artistid
- year
- duration

***artists***
- artistid (PK)
- name
- location
- lattitude
- longitude

***time***
- start_time (PK)
- hour
- day
- week
- month
- year
- weekday

# 5) Execution

1. Create AWS Redshift Cluster
2. Start Airflow (e. g. locally using a docker image)
3. Configure Airflow Connections (e. g. in GUI)
    - aws_credentials
    - redshift
4. Run DAG (etl.py)


# 6) Ressources
- https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html
- https://docs.aws.amazon.com/redshift/latest/dg/r_TRUNCATE.html
- https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
- https://medium.com/datareply/airflow-lesser-known-tips-tricks-and-best-practises-cf4d4a90f8f
- https://medium.com/analytics-and-data/10-benefits-to-using-airflow-33d312537bae
- https://aws.amazon.com/de/managed-workflows-for-apache-airflow/
- https://github.com/puckel/docker-airflow
