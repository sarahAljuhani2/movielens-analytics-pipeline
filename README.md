
🎬 MovieLens Analytics Pipeline
This project builds a complete data pipeline to analyze movie ratings using the MovieLens 20M dataset. It covers the full lifecycle from data ingestion to interactive dashboards.

 Tools & Technologies
Snowflake – Cloud data warehouse to store raw and transformed data

dbt (Data Build Tool) – For data transformation, modeling, and testing

Amazon S3 – Cloud storage for staging raw CSV files

Power BI – To create interactive dashboards and insights

 Pipeline Overview
This diagram illustrates the full pipeline:


Extract: Raw data files are stored in Amazon S3

Load: Data is loaded into Snowflake tables

Transform: dbt transforms raw data into clean, analysis-ready models

Visualize: Power BI connects directly to Snowflake for dashboard creation

 Sample Dashboard Output
This dashboard showcases key movie insights including average ratings, total reviews, genre distribution, and rating categories.




💡 Key Skills Demonstrated
- SQL (Structured Query Language)

- Data Modeling with dbt

- Cloud Data Warehousing with Snowflake

- Data Visualization using Power BI
