# 🎬 MovieLens Analytics Pipeline

This project showcases a complete end-to-end data analytics pipeline for analyzing movie ratings using the [MovieLens](https://grouplens.org/datasets/movielens/) dataset.

---

## 🛠️ Tools & Technologies

- **Amazon S3** – Storing raw CSV files  
- **Snowflake** – Cloud data warehouse for raw and transformed data  
- **dbt** – Data modeling, transformation, testing, and documentation  
- **Power BI** – Interactive dashboards and visualization

---

##  Pipeline Workflow

1. **Extract** – Upload raw data files to Amazon S3  
2. **Load** – Ingest raw data into Snowflake  
3. **Transform** – Use dbt to build staging, fact, and dimension models  
4. **Visualize** – Connect Power BI to Snowflake for insightful analysis

---

##  Sample Dashboard Output

Created dashboard highlights key movie rating insights such as:

- Average ratings
- Rating distribution
- Genre popularity
- Tags used by users
- Rating categories based on score



---

##  Step-by-Step Instructions

1. **Prepare the Data**  
   - Download MovieLens dataset (ml-20m)  
   - Upload CSV files to an Amazon S3 bucket

2. **Set Up Snowflake**  
   - Create roles, users, and warehouse  
   - Create raw tables and load data from S3  
   - Clean and stage data

3. **Use dbt for Transformation**  
   - Create staging models for raw data  
   - Build dimension and fact models  
   - Create analytics models with tests and documentation  
   - Run and compile dbt project

4. **Build the Dashboard in Power BI**  
   - Connect to Snowflake  
   - Create visuals for average ratings, genres, tags, rating categories  
   - Add KPIs and interactive filters  
   - Customize layout and design

---

##  Skills Demonstrated

- SQL (Structured Query Language)  
- Data Modeling using dbt  
- Cloud Data Warehousing with Snowflake  
- Dashboarding and Visualization using Power BI  
