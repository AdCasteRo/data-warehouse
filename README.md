# DATA WAREHOUSE

The project "Data Warehouse" consists on two python scripts, one sql document and one congif archive. Due to the necesities of the project, I've added one Jupyter Notebook to open and close the Redshift cluster and a debuf python script to show the errors.

## Purpose of the data warehouse

The purpose of this datawarehouse has two steps. First, gets the logs and songs of the fictional start-up Sparkify and copy them into a staging databvase. From there, the information is divided and transformed into the analytical database for its future use.

## Analytical database justification

The analytical database has a star schema and is formed by one fact table (songplays) and 4 dimension tables (users, songs, artists and time).

Songplays is the transformed information from the staging_events table, with the information related to songs, users, artists and time normalized to their own dimension table.

## Use of the project

- (OPTIONAL) Follow the steps 1-4 of "cluster.ipynb", adding first your AWS key and secret into "dwh.cfg".
- Copy your ARN and Host into "dwh.cfg".
- Execute "create_tables.py".
- Execute "etl.py".
- (OPTIONAL) Follow step 5 of "cluster.ipynb" to close your cluster.

## Sources

This project includes code provided by Udacity for the creation of this project.

This project includes code form previous exercises.

This project includes feedback from the reviewer of the project.

## Changelog

25/03/2021:
- Added argument description to docstrings
- Added primary keys
- Added not null to foreign key "artist_id"
- Added SELECT DISTINCT instead of SELECT
