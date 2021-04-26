# Add path to scraping scripts
import sys
sys.path.append('Scraping')
sys.path.append('Database_scripts')
sys.path.append('Preprocessing scripts')

#Colab paths
sys.path.append('/content/Apartments')
sys.path.append('/content/Apartments/Scraping')
sys.path.append('/content/Apartments/Database_scripts')
sys.path.append('/content/Apartments/Preprocessing_scripts')
sys.path.append('/Apartments/Scraping')
sys.path.append('/Apartments/Database_scripts')
sys.path.append('/Apartments/Preprocessing_scripts')

from morizonScraper import ScrapingMorizon
from otodomScraper import ScrapingOtodom
from db_manipulation import DatabaseManipulation
from otodom import Preprocessing_Otodom
from morizon import Preprocessing_Morizon
import pandas as pd
import configparser
import urllib
from sqlalchemy import create_engine

if __name__ == "__main__":
    # Database connection

    config = configparser.ConfigParser()
    config.read('Database_scripts/config.ini')

    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)

    # ===Morizon===
    morizon_scraper = ScrapingMorizon(page = 'https://www.morizon.pl/do-wynajecia/mieszkania',page_name = 'https://www.morizon.pl',max_threads = 30)

    # Get links to scrape
    morizon_pages = morizon_scraper.get_pages()
    morizon_offers = morizon_scraper.get_offers(pages=morizon_pages, split_size=100)
    to_scrape = database_manipulation.push_to_database_links(activeLinks=morizon_offers, page_name="Morizon")
    # Scrape Details
    morizon_scraped = morizon_scraper.get_details(offers=to_scrape, split_size=500)

    # Prepare offers to insert into table
    morizon_scraped_c = morizon_scraped.copy().reset_index().drop(['index'], axis=1)
    morizon_preprocess = Preprocessing_Morizon(apartment_details=morizon_scraped_c.where(pd.notnull(morizon_scraped_c), None),
                                             information_types=morizon_scraped_c.columns)
    morizon_table = morizon_preprocess.create_table()
    morizon_table=morizon_table.where(pd.notnull(morizon_table), None)


    # Insert into table
    database_manipulation.push_to_database_offers(offers=morizon_scraped)

    # ===Otodom===
    otodom_scraper = ScrapingOtodom(page='https://www.otodom.pl/wynajem/mieszkanie/', page_name='https://www.otodom.pl', max_threads=20)

    # Get links to scrape
    otodom_pages = otodom_scraper.get_pages()
    otodom_offers = otodom_scraper.get_offers(pages=otodom_pages, split_size=100)
    to_scrape = database_manipulation.push_to_database_links(activeLinks = otodom_offers, page_name = "Otodom")

    # Scrape details
    otodom_scraped = otodom_scraper.get_details(offers=list(to_scrape["link"]),split_size=500)

    # Prepare offers to insert into table
    otodom_scraped_c = otodom_scraped.copy().reset_index().drop(['index'], axis=1)
    otodom_preprocess = Preprocessing_Otodom(apartment_details=otodom_scraped_c.where(pd.notnull(otodom_scraped_c), None),
                                             information_types=otodom_scraped_c.columns)
    otodom_table = otodom_preprocess.create_table()
    otodom_table=otodom_table.where(pd.notnull(otodom_table), None)

    # Insert offers into table
    database_manipulation.push_to_database_offers(offers=otodom_table)

