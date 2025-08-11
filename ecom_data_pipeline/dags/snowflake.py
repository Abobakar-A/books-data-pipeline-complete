# Import necessary libraries
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import the scraping function from the new file
# Adjust the import path if you saved scrape_books.py in a different location.
# Change this line
# from scripts.scrape_books import scrape_and_load_books

# To this:
from scrape_books import scrape_and_load_books

# --- Airflow DAG definition ---
with DAG(
    dag_id='book_scraper_snowflake_conn',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['example', 'data-pipeline', 'books'],
) as dag:

    scrape_and_load_task = PythonOperator(
        task_id='scrape_and_load_books',
        python_callable=scrape_and_load_books,
    )
