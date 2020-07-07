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

## Why did you choose the tools and technologies you chose?
Apache Spark is a purpose built framework to facilitate processing, querying and analysing large datasets centered in 
the realm of BigData. Computation is done in memory, so the physical architecture of a machine/computer made to process
data rapidly is utilised, in stead of the CPU (for example). It makes processing faster than, for example, MapReduce and 
and similar methodologies. Some of the features elevating it above competitor products include:
- It is orders of magnitude faster than traditional large scale data processing platforms/technologies
- It's easy to use in the most common data processing languages, including Python, R, and Scala
- Several additional libraries are provided for, like SQL, Streaming, and Graph computation

An important drawback to consider when working with Spark includes Spark job tuning can be a pain and it can end up 
being a large chunk of the development time.

Amazon S3 is chosen as the storage and access layer for this solution for the following reasons:
- Access management is made extremely simple and as granular as needed, thereby helping enforce good data policies
and protecting sensitive data where and when necessary
- Storage format is not limited in any form and the user is not constrained to using something like a relational database
model
- It's simple to use and to put into use for existing systems
- It's reliable and robust, as it's managed external to the company using it (AWS boasts a 99.99% durability for its 
storage objects)

## How would Spark or Airflow be incorporated?
As Spark is already incorporated, the only component missing is Airflow. Since Airflow lends itself perfectly to the use 
of S3 operations and has operators for both Spark and S3, it's a natural fit to transform each of the steps outlined in 
the ETL process (see `src/etl.py`) into steps in a DAG. 

## Propose how often the data should be updated and why.
Data of this nature should be updated daily as timestamped data isn't present in the transactions dataset so anything 
more granular is redundant. Further the nature of the analytics doesn't require near-realtime updates.

## What-if scenarios
### If the data was increased by 100x.
Spark is capable of handling masses of data by distributing workloads amongst worker nodes within the Spark cluster. That 
is, there is a driver node that processes the Spark job configuration and then distributes each of the tasks that can be 
run in parallel across the available worker nodes. There is some configuration required on the part of the engineer to 
ensure the Spark job is configured to be distributable, however. 

Further, if necessary, Airflow can be incorporated to achieve a similar solution since it also handles workload 
distribution across different worker nodes. The added possibility 
is making use of partitioning in Airflow whereby the data is processed in intervals over time, making the amount of data 
to "ETL" for each process less, thus alleviating pressure on the architecture. (A similar notion of partitioning the data 
for parallel processing is present in Spark).   

### If the pipelines were run on a daily basis by 7am.
The scenario is slightly ambiguous. It can be interpreted in two ways:
1. The data is required to be accessible by 7am each day
1. The pipeline should start off at 7am each day

In the case of 1, I'd use the same pipeline construction incorporated in Airflow and set up a cron schedule, 
along with an SLA that alerts when the pipeline exceeded it's intended runtime and the data isn't available 
at the required time (i.e. 7am). This then allows the data engineer working on the pipeline to revisit not only scheduling 
( i.e. when the pipeline is started) but also the processing. Further, during development the engineer is supposed to gauge 
how long the pipeline runs, and schedule the start time early enough to ensure in-time delivery of the end result. As 
the data grows and the pipeline becomes slower, tha SLA will alert the engineer and amendments can be made. 

In the case of 2, it's a simple case of scheduling the pipeline DAG in Airflow using the schedule interval functionality 
provided as part of the system. 

### If the database needed to be accessed by 100+ people.
As mentioned under `Why did you choose the tools and technologies you chose?`, S3 was chosen specifically because it
handles user access extremely well. Security is no longer a concern as access can be controlled on a data object level.
Further, as of 2018, AWS S3 has increase performance capabilities to allow for at least 3500 requests per second to add 
data to storage, and 5500 requests to retrieve data from storage. At an object level, this means even if 100+ users tries 
to access the same data object there won't be limitations imposed, unless the users are doing something they shouldn't 
(redundant calls to the same data objects at an enormous rate). These performance improvements were rolled out across 
all of S3's storage types. 