# book_pipeline_tasks.py

import logging
from datetime import datetime

# Configuration variables
NUM_PAGES_TO_SCRAPE = 5
BASE_URL = 'http://books.toscrape.com/catalogue/page-{}.html'
SNOWFLAKE_TABLE = 'BOOKS_RAW'
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_SCHEMA = 'public' 
SNOWFLAKE_DATABASE = 'books' 

# Configure logging for better visibility
logger = logging.getLogger(__name__)

def create_snowflake_table():
    """
    Creates the raw table in Snowflake if it doesn't already exist.
    """
    # Import the library here to avoid a long loading time for the DAG
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    logger.info("Attempting to create the Snowflake table.")
    conn = None
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
                title VARCHAR,
                price FLOAT,
                rating INTEGER,
                extraction_date TIMESTAMP
            );
        """
        
        logger.info(f"Executing SQL: {create_table_sql}")
        cursor.execute(create_table_sql)
        
        logger.info(f"Table {SNOWFLAKE_TABLE} created successfully (if it did not exist).")
    except Exception as e:
        logger.error(f"Failed to create Snowflake table: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")

def scrape_and_load_books():
    """
    Scrapes book data from a website and loads it into a Snowflake table.
    """
    # Import the libraries here to avoid a long loading time for the DAG
    import requests
    from bs4 import BeautifulSoup
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    logger.info("Starting the web scraping and data loading process.")
    start_page = 1
    end_page = start_page + NUM_PAGES_TO_SCRAPE - 1

    logger.info(f"Scraping books from page {start_page} to {end_page}.")

    all_books_data = []
    for page_num in range(start_page, end_page + 1):
        url = BASE_URL.format(page_num)
        try:
            logger.info(f"Attempting to fetch data from {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            logger.info(f"Successfully fetched page {page_num}.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch page {page_num}: {e}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        books = soup.find_all('article', class_='product_pod')
        for book in books:
            title = book.h3.a['title']
            price_str = book.find('p', class_='price_color').text
            price = float(price_str.replace('Â£', ''))
            rating_str = book.p['class'][-1]
            rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
            rating = rating_map.get(rating_str, 0)
            all_books_data.append((title, price, rating, datetime.now()))
            logger.info(f"Scraped book: {title} (Rating: {rating})")

    if not all_books_data:
        logger.warning("No new books were scraped. Exiting.")
        return

    logger.info(f"Connecting to Snowflake using connection ID: {SNOWFLAKE_CONN_ID}")
    conn = None
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        logger.info(f"Inserting {len(all_books_data)} records into {SNOWFLAKE_TABLE}.")
        insert_query = f"""
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (title, price, rating, extraction_date)
            VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(insert_query, all_books_data)
        conn.commit()
        logger.info(f"Successfully inserted {len(all_books_data)} records into {SNOWFLAKE_TABLE}.")
    except Exception as e:
        logger.error(f"Failed to connect to or insert into Snowflake: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")
