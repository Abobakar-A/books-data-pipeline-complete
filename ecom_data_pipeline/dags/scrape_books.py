# Import necessary libraries
import logging
import os
import requests
from bs4 import BeautifulSoup
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Configuration variables
# The number of pages to scrape in each run
NUM_PAGES_TO_SCRAPE = 5
# The local file to store the last scraped page number
LAST_PAGE_FILE = 'last_page.txt'
# The base URL of the website to scrape
BASE_URL = 'http://books.toscrape.com/catalogue/page-{}.html'
# The Snowflake table to load the data into
SNOWFLAKE_TABLE = 'BOOKS_RAW' 
# The Airflow connection ID for Snowflake
SNOWFLAKE_CONN_ID = 'snowflake_default'

# Configure logging for better visibility
logger = logging.getLogger(__name__)

def scrape_and_load_books():
    """
    Scrapes book data from a website and loads it into a Snowflake table.
    """
    logger.info("Starting the web scraping and data loading process.")

    # 1. Determine the starting page for scraping
    start_page = 1
    if os.path.exists(LAST_PAGE_FILE):
        with open(LAST_PAGE_FILE, 'r') as f:
            try:
                start_page = int(f.read().strip()) + 1
            except (ValueError, IndexError):
                logger.warning(f"Could not read starting page from {LAST_PAGE_FILE}, starting from page 1.")
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

            # Remove the 'Â£' string, which is causing the ValueError, before converting.
            price = float(price_str.replace('Â£', ''))

            rating_str = book.p['class'][-1]
            rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
            rating = rating_map.get(rating_str, 0)
            
            # Use the correct column name 'extraction_date' and get the current timestamp
            all_books_data.append((title, price, rating, datetime.now()))
            logger.info(f"Scraped book: {title} (Rating: {rating})")

    if not all_books_data:
        logger.warning("No new books were scraped. Exiting.")
        return

    # 2. Load the scraped data into Snowflake using Airflow connection
    logger.info(f"Connecting to Snowflake using connection ID: {SNOWFLAKE_CONN_ID}")

    conn = None
    try:
        logger.info("Attempting to get Snowflake connection.")
        # Create a hook to manage the Snowflake connection
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        logger.info("Successfully established Snowflake connection.")
        cursor = conn.cursor()

        logger.info(f"Inserting {len(all_books_data)} records into {SNOWFLAKE_TABLE}.")

        # Prepare the SQL insert statement with the correct column name
        insert_query = f"""
            INSERT INTO {SNOWFLAKE_TABLE} (title, price, rating, extraction_date)
            VALUES (%s, %s, %s, %s)
        """

        # Execute the bulk insert
        cursor.executemany(insert_query, all_books_data)
        conn.commit()

        logger.info(f"Successfully inserted {len(all_books_data)} records into {SNOWFLAKE_TABLE}.")

    except Exception as e:
        logger.error(f"Failed to connect to or insert into Snowflake: {e}")
        # If an error occurs, do not update the last page file
        # so the process can be retried from the same point.
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")

    # 3. Update the last page file if loading was successful
    logger.info("Attempting to update the last page file.")
    with open(LAST_PAGE_FILE, 'w') as f:
        f.write(str(end_page))
    logger.info(f"Updated {LAST_PAGE_FILE} with the new end page: {end_page}")
