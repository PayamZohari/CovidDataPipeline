# Overview
This project is part of Mofid Securities Interview task which aimes to design and implement a data pipeline to collect, ingestion, store, process and finally visualize statistics regarding Covid-19 cases in U.S.A. The pipeline is illustrated in the diagram below:

![data pipeline overview.png](plots/data%20pipeline%20overview.png)

# Entities 

## Elastic ( dockerized ): 
data source

## Apache Airflow ( dockerized ): 
scheduler and orchestrator

## Postgres ( dockerized ):
OLTP Data warehouse 

## metabase ( dockerized ):
Visualization and dashboarding

#Steps

1. Capture & Ingest
   
In this section, collected covid-related records of data from hospitals which are gathered in a .csv file are provided and to persist it more seamlessly, I have implemented a python job ( ingest_elastic_to_csv.py ) Which reads data (batchwise) from csv file and inserts to elastic.

To avoid data loss and inconsistency, after each insert, checkpoints the offset to a .Jason file and at the beginning of next batch, it reads the checkpoint to read from next offset.

sample output of ingestion process:

Batch 34 insert status: 200
Inserting Batch 35 (rows 14101 to 14200)...
Batch 35 insert status: 200
Inserting Batch 36 (rows 14201 to 14300)...
Batch 36 insert status: 200
Inserting Batch 37 (rows 14301 to 14400)...
Batch 37 insert status: 200
Inserting Batch 38 (rows 14401 to 14500)...
Batch 38 insert status: 200
Inserting Batch 39 (rows 14501 to 14600)...
Batch 39 insert status: 200
Inserting Batch 40 (rows 14601 to 14700)...
Batch 40 insert status: 200


2. ETL
In this section, the raw data in the elastic is needed to be cleaned and processed, and then get inserted to the data warehouse (postgres) to be visualized.
To do so, I implemented a ETL process, including below three tasks

### extract

extract data from elastic 

### transform 
process and clean up the data, use only metrics and dimensions valuable to be used for the mission ( visualizing total records and most recent insertion time )

### load

insert the transformed data to DW ( postures)

But since the .csv data might be an stream of data ( never ending ), the first job ( ingestion ) is designed to continously ingest rest of stream. For ETL though, a periodic (hourly) job is needed to do it on new data every hour ( least period interval in airflow )
That's why this job is written as a DAG ( directed acyclic graph ) where there are depencies ( transform task depends on extract, and load depends on transform )

3. USE

The final section is the visualization via metabase. To do so, a distinct database is created in the postgres ( which is also used for metadata of airflow but in different databases ), then an Analytical dashboard is created in which, there are 3 visualizations:

## total counts of covid cases in the csv 

## most recent date of records 

## overview on important columns of the covid records 

The overview of dashboards can be seen in below images. after 30 and 90 batch jobs:

the final output of dashboards:

after batch 30 ( 3000 records )
![metabase-after batch 30.png](plots/metabase-after%20batch%2030.png)

after batch 90 ( 9000 records )
![metabase-after batch 90.png](plots/metabase-after%20batch%2090.png)




# How run the project

1. clone the project
   
   ```bash
   git clone git@github.com:PayamZohari/CovidDataPipeline.git
   ```

2. install requirements
   
   pip3 install -r requirements

   then load .csv data file via

   ```bash
   cp covid.csv ./datasource/usa_covid.csv
   ```

4. look into .env file which contains necessary credentials for the project.

5. set up project pipeline

   ```bash
   docker composed up [-d]
   ```

6. Run the ingestion
   
   ```bash
   python3 ingest_csv_to_elastic.py
   ```
7. Log into visualization

go to host:3000 to see metabase  dashboard. in Analytical dashboard you can see on tab 1 there is 3 visualization as illustrated in the picture
