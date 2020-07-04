# TODO: fix path refs and filename refs
# TODO: remove refs to Redshift and explain solution in light of DataLakes option

# Udacity Data Engineering Nano Degree - Capstone Project
This repository holds the project code for the capstone project completed towards finishing the 
Udacity Data Engineering Nano Degree.

## Project context
This is the Udacity provided project. Three datasets are provided to work with, which is explained further in a section below called *Datasets*. 

### Project purpose
The purpose of the project is to combine skills that have been learned throughout the program and demonstrate a firm grasp of these skills, and the ability to use it towards a real-life application.

### Project steps
The following steps are followed in completing the project:
1. Scope the Project and Gather Data
1. Explore and Assess the Data
1. Define the Data Model
1. Run ETL to Model the Data
1. Complete Project Write Up

The context of each step is explained further in subsequent sections below, where applicable.

### Project solution
In brief, the solution is a relational database built using AWS S3 and AWS Redshift. The S3 bucket is used to hold the raw immigration data as well as the supplementary data. This is to make it simple to access the data for 
the ETL process involved in preparing the data for analytical use. 

A Redshift RDB is chosen for this particular solution for the following reasons:

- ACID transactions
- There is a need to use JOINS when querying the data
- SQL lends itself well to analysis, including but not limited to aggregations and other calculations
- Referential integrity

Further, a star schema is chosen to reduce redundancy in storing data and to faciliate interpretability of the individual data tables.

A note on Redshift: the solution infrastructure allows for parallelisation of queries run against the DB tables, which speeds up basic CRUD operations, making it and ideal choice when the need for an RDB is also present. This way, a "best of both worlds" scenario is available to analysts working with large datasets.

Have a look at the */requirements.txt* file for information on packages used towards achieving the envisioned functionality.

## Datasets
The main dataset includes data on immigration to the United States. Supplementary datasets include data on airport codes, and U.S. city demographics. This data is provided by Udacity, specifically for this project.

Have a look at the data dictionaries provided in */data/dictionaries/source* for more information about the data included. 

## Project development
This section describes the context of the different steps outlined above, where applicable. The rough work done towards completing each step is captured in the accompanying notebook, under `/notebooks/`.

### Explore and Assess the Data
Each of the different datasets provided are read into memory using the notebook mentioned above. Only a sample of the immigration data is used for this step, as the full set is overly large for preprocessing. Each dataset is profiled using Pandas Profiling to identify data types and missing values. These reports can be viewed in `/data/profiling/`.

### Immigration dataset

