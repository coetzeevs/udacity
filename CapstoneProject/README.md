# Udacity Data Engineering Nano Degree - Capstone Project
This repository holds the project code for the capstone project completed towards finishing the 
Udacity Data Engineering Nano Degree.

## Project context
This is the Udacity provided project. Three datasets are provided to work with, which is explained further in 
a section below called `Datasets`. 

### Project purpose
The purpose of the project is to combine skills that have been learned throughout the program and demonstrate 
a firm grasp of these skills, and the ability to use it towards a real-life application.

### Project steps
The following steps are followed in completing the project:
1. Scope the Project and Gather Data
1. Explore and Assess the Data
1. Define the Data Model
1. Validate the Data Model

The context of each step is explained further in subsequent sections below, where applicable.

### Project solution
In brief the solution is a Data Lake in S3 that is made up of a start schema of data models, built to aid in the 
analysis of migrant data throughout the USA. 

The raw immigration data as well as the supplementary data are all contained in the repository for the purposes of this
project, however in a "real-life" situation this data could be ingested from any source. This would require only a minor
alteration to the solution base, specifying the connection to and location of the source data. That's what makes this 
solution so flexible in its application. THe caveat being that the source data needs to match the data presented in this
repository as the solution is built specifically for this use case.   

The data is stored in parquet format in the destination S3 bucket. The associated advantages of using parquet as a storage
format include:
- Reducing I/O operations
- Fetching only specific columns that you need to access
- Consuming less space for storage - an important point when data becomes excessively large and there are costs associated 
with total bytes stored
- Supporting type-specific encoding

Further, a star schema is chosen to reduce redundancy in storing data and to facilitate interpretation 
of the individual data tables.

## Datasets
The main dataset includes data on immigration to the United States. 
Supplementary datasets include data on airport codes and U.S. city demographics. This data is provided by Udacity, 
specifically for this project. Additional data is derived from the `/data/docs/I94_SAS_Labels_Descriptions.SAS`
document also provided along with the immigration data. This includes:
- countries
- entry modes
- US cities
- US states
- visa types

Other datasets included to further expand on the analysis includes:
- airlines
- airports

## Explore and assess the data
This section involved understanding the structure of the input data and applying any cleaning steps, as well as 
transformations required, before continuing with modelling the data for the final data model structures. These sub-steps
are then:
1. Ingest the source data for processing
1. Clean the source data
1. Transform any datasets that require some steps of transformation

Ingestion of the data is taken care of using the class object created in `src/ops/etl/storage_ops.py`. A configuration 
for each source dataset is recorded in a data dictionary in `src/configs/sources.py`. This configuration is then used 
to ingest the data and storing each resultant Spark DataFrame in a dictionary object, used for ease of handling.

Cleaning is taken care of using the class object created in `src/ops/data/data_cleaning.py`. 
The primary cleaning steps involve:
- Filling the demographics dataset null values with 0, after grouping by a selection of columns and pivoting on race.
- Filter the airports dataset to select only US airports, and discard any data points not related to a specific 
airport type
- Rename data fields for a selection of the datasets, and cast some fields to specific data types
- Remove data from the airlines dataset that don't have an IATA code associated with that entry
- Select only applicable data points from the immigration dataset and rename the columns to appropriate values for use
down the line

Data transformation operations are taken care of using the class object created in `src/ops/data/data_transformation.py`.
This includes:
- Aggregate demographics data, grouping by state and calculating gender and race ratios for each state
- Split arrival date into date parts (day, month, year) for the immigrations dataset (this is used to partition the
resultant facts table)

Each of the steps discussed above has detailed descriptions captured in document strings for each of the class objects
and their associated methods, so reading through each file is encouraged to get an in depth understanding of the processing
involved.

## Define the Data Model
The second-to-last step is to create the final data model and store the data in the destination storage bucket in AWS S3.
This is taken care of with the class object created in `src/ops/etl/warehouse_ops.py`. The three primary steps here are:
- Store each supplementary table (having been cleaned and transformed) as the dimensions tables
- Create the facts table from the immigrations data and store

All data is stored as parquet formatted files, compressed using the snappy algorithm. 
Again, more detail associated with each step is documented under the class object and its associated methods.

## Validate the Data Model
The final step is validation of the resultant data model structure is performed using the class object created in 
`src/ops/data/data_validation.py`. Here the data is ingested from the storage bucket intended for the final storage.
This is the first validation check: can the data be read in from the source location, from parquet. Along with this is 
a check to ensure that data is received. Each dataset's records are counted and the assertion is that the record count is
greater than zero.
A final check is done to join the immigration facts data columns with corresponding dimension table columns, 
and assert if correct values are returned. If all checks are passed a boolean value of True is returned and the pipeline
is considered a success. If these checks fail, an error is raised.

## Final data model details
The final data model dictionary is available in `data/docs/data_dictionaries.md`. 

## Why did you choose the model you chose?
The final data model is intended to run queries to help understand things like how many migrants enter the US but never leave,
identify popular entry modes for migrants into the US, identify which visa types are the most used to enter the US,
understand which airlines are favoured for entering the US given specific countries of origin, and the list goes on.
The structure of the data model is chosen exactly to facilitate these lines of enquiry.

## How would Spark or Airflow be incorporated?
As Spark is already incorporated, the only component missing is Airflow. Since Airflow lends itself perfectly to the use 
of S3 operations and has operators for both Spark and S3, it's a natural fit to transform each of the steps outlined in 
the ETL process (see `src/etl.py`) into steps in a DAG. 

## Propose how often the data should be updated and why.
Data of this nature should be updated daily as timestamped data isn't present in the transactions dataset so anything 
more granular is redundant. Further the nature of the analytics doesn't require near-realtime updates.

## What-if scenarios
### If the data was increased by 100x.
Spark is capable of handling this due to its ability to distribute workloads amongst many worker nodes

### If the pipelines were run on a daily basis by 7am.
I'd use the same pipeline construction incorporated in Airflow and set up an required time to complete for the DAG run.

### If the database needed to be accessed by 100+ people.
This is no problem for AWS S3 and the solution lends itself perfectly to this scenario.
 