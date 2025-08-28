Books Data Pipeline Complete
This repository contains a comprehensive Extract, Transform, Load (ETL) pipeline for scraping book data from a website, transforming it using dbt, and loading it into a Snowflake Data Cloud. The entire pipeline is orchestrated using Apache Airflow, deployed and managed via Astronomer CLI, all housed within a monorepo structure for streamlined development and deployment.

Project Overview
This project aims to automate the process of collecting structured data about books, cleaning and transforming it into an analytics-ready format, and making it available in Snowflake for further analysis and reporting.

Architecture & Components
The pipeline is structured into the following key components:

Web Scraping (Data Ingestion):

Technology: Python scripts.

Purpose: Extracts raw book information (titles, authors, prices, etc.) from a specified website.

Output: Raw data is loaded into Snowflake tables.

dbt Transformations (Data Transformation):

Technology: dbt (data build tool).

Purpose: Takes the raw data loaded into Snowflake and applies a series of transformations (cleaning, standardizing, joining, creating fact/dimension tables) to create a clean, governed, and analytics-ready data warehouse layer.

Output: Transformed tables and views in Snowflake.

Apache Airflow (Orchestration):

Technology: Apache Airflow.

Purpose: Schedules, monitors, and manages the end-to-end data pipeline. It ensures that scraping runs first, then dbt transformations, handling dependencies and retries.

Deployment: Managed via Astronomer CLI for local development and cloud deployment.

Snowflake (Data Warehouse):

Purpose: Serves as the central data warehouse where raw, staging, and transformed data reside. It provides the compute and storage for the entire pipeline.

Monorepo Structure (Project Organization):

Purpose: All components (Scraping, dbt, Airflow) are managed within a single GitHub repository. This simplifies version control, dependency management, and deployment.

Getting Started
To set up and run this project, you'll need the following prerequisites and follow these high-level steps.

Prerequisites
Python 3.8+: For dbt, Airflow (local), and scraping scripts.

Git: For cloning the repository.

Docker Desktop: Required for Astronomer CLI local development environment.

Astronomer CLI: Install instructions can be found on the Astronomer Documentation.

Snowflake Account: With necessary roles, warehouses, databases, and schemas (RETAIL_DB.RETAIL as discussed previously).

Snowflake User: With appropriate permissions (USAGE on database/schema, CREATE TABLE/VIEW, INSERT, SELECT on all tables/views) for dbt and Airflow connections.

1. Clone the Repository
First, clone this monorepo to your local machine:

git clone https://github.com/Abobakar-A/books-data-pipeline-complete.git
cd books-data-pipeline-complete

2. Set Up dbt Project
The dbt project resides in a dedicated folder within this monorepo.

Navigate to dbt project: cd my-new-dbt-books-project (or whatever you renamed your dbt folder to).

Install dbt dependencies: pip install -r requirements.txt (ensure dbt-snowflake is listed).

Configure profiles.yml: Ensure your ~/.dbt/profiles.yml (or my-new-dbt-books-project/profiles.yml if you moved it there) is correctly configured to connect to your Snowflake account. It should match the profile: my_retail_project defined in dbt_project.yml.

Create Source Tables in Snowflake: Ensure the raw tables (e.g., DIM_CUSTOMERS, FACT_SALES, STG_PRODUCTS, etc., as defined in your sources.yaml files and used by your stg_ models) actually exist in your Snowflake RETAIL_DB.RETAIL schema and contain some data. You might need CREATE TABLE and INSERT statements for initial setup.

3. Set Up Airflow Project (with Scraping)
The Airflow project (which includes your scraping scripts) is also in its own folder.

Navigate to Airflow project: cd ecom_data_pipeline (or whatever you renamed your Airflow folder to).

Initialize Astronomer project (if not already): If this is a fresh setup within the monorepo, you might re-initialize or configure this project for Astronomer.

Update DAG paths:

Open your DAG files (ecom_data_pipeline/dags/*.py).

Ensure any BashOperator or PythonOperator tasks that call your scraping scripts or dbt commands (dbt run, dbt build) have their paths updated to reflect the monorepo structure. For example, a command might now look like:

# Assuming dbt project is at /usr/local/airflow/my-new-dbt-books-project
# and scraping scripts are at /usr/local/airflow/data_ingestion
bash_command='cd /usr/local/airflow/my-new-dbt-books-project && dbt run'

The /usr/local/airflow/ prefix is the default working directory for files deployed via Astronomer.

Snowflake Connection: Ensure your Airflow environment is configured with the correct Snowflake connection details. This is often done via Airflow Connections (e.g., snowflake_default or a custom connection).

4. Run the Pipeline Locally (via Astronomer CLI)
Once all components are configured, you can test the entire pipeline locally.

From the Airflow project root: cd ecom_data_pipeline

Start the local Airflow environment:

astro dev start

Access Airflow UI: Open your browser to http://localhost:8080 (or the port indicated by astro dev start).

Unpause and Trigger DAGs: Unpause your relevant DAGs in the Airflow UI and trigger them manually to observe the full pipeline execution (scraping -> dbt).

5. Deploy to Astronomer Cloud
When your pipeline is working as expected locally, deploy it to your Astronomer Workspace.

From the Airflow project root: cd ecom_data_pipeline

Deploy your project:

astro deploy

Follow the prompts to select your Astronomer Workspace and Deployment.

Usage
After successful deployment, your ETL pipeline will run automatically according to the schedule defined in your Airflow DAGs. You can monitor the status, logs, and performance of each task through the Astronomer UI or the Airflow UI.

Contributing
Feel free to fork this repository, open issues, or submit pull requests.

License
This project is open-sourced under the MIT License.
