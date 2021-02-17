# 1) Introduction

The startup company Sparkify launched a new music streaming app and wants to get insights about the music preferences of their users, in particular Sparkify wants to understand what songs users are listening to.

The goal of this project is to build a consistent Amazon Redshift Data Warehouse which will serve as a single source of truth for data analysis.
A Python/SQL ETL pipeline is designed to update the database with the collected data which currently resides in an AWS S3 bucket of JSON logs on user activity, as well as a directory with JSON metadata on the songs.

The ETL pipeline is supported by a fully automated Apache Airflow workflow including a node for data quality monitoring.

# 2) Repository
### Structure: 

**Files:** </br>
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

**Files:** </br>
- **create_tables.py:** *python script to create the AWS Redshift database tables.*
- **etl.py:** *Airflow-Python script which reads the JSON sourcefiles and inserts it into the AWS Redshift database tables.*
- **README.md** *describes the project.*
- **sql_queries.py:** *SQL file which includes create/drop table and copy/insert statements used in the database creation and ETL process.*
- **operators (folder):**  *Airflow custom operators used for data loading and data quality checks.*

**Source Data:** </br>
- log-data/ and song-data/ contains JSON files collected from the app.</br>
- log_json_path.json is a JSONPaths file to parse the JSON source data for log_data/. Link to AWS explanation:</br>
https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html

# 3) Airflow Workflow

![](Airflow_DAG.png)


# x) Ressources
https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html
https://docs.aws.amazon.com/redshift/latest/dg/r_TRUNCATE.html
https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
https://medium.com/datareply/airflow-lesser-known-tips-tricks-and-best-practises-cf4d4a90f8f

