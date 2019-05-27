# Data Lake on AWS with Spark

The purpose of this project is to build and ETL pipeline for a Data Lake hosted on Amazon S3

### Prerequisite

1. Install [Python 3.x](https://www.python.org/).

2. This project is build with **conda** instead of **pip**.
Install [anaconda](https://www.anaconda.com/distribution/) or modify the script to make use of pip.

### Main Goal
The compagny Sparkify need to analyses theirs data to better know the way users (free/paid) use theirs services.
With this data model we will be able to ask question like When? Who? Where? and What? about the data.
The task is to build an ETL Pipeline that extract data from a S3, wrangle them in memory with Spark and write it back to S3 for analysis.

### Data Model
![Song ERD](./Song_ERD.png)

This data model is called a **start schema** data model. At it's aim is a **Fact Table -songplays-** that containg fact on song play like user agent, location, session or user's level and then have columns of **foreign keys (FK)** of **4 dimension tables** :

* **Songs** table with data about songs
* **Artists** table
* **Users** table
* **Time** table

This model enable search with the minimum **SQL JOIN** possible and enable fast read queries.

### Run it
Few steps

1. Run **etl.py** to wrangle the data