import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.g2.com/search?query={formatted_keyword}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code == 200:
                success = True
            
            else:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
                ## Extract Data

            soup = BeautifulSoup(response.text, "html.parser")
            
            div_cards = soup.find_all("div", class_="product-listing mb-1 border-bottom")


            for div_card in div_cards:

                name = div_card.find("div", class_="product-listing__product-name")

                g2_url = name.find("a").get("href")

                has_rating = div_card.find("span", class_="fw-semibold")
                rating = 0.0

                if has_rating:
                    rating = has_rating.text

                description = div_card.find("p").text
                
                search_data = {
                    "name": name.text,
                    "stars": rating,
                    "g2_url": g2_url,
                    "description": description
                }
                

                data_pipeline.add_data(search_data)
            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")



if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["online bank"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        scrape_search_results(keyword, location, retries=MAX_RETRIES)
    logger.info(f"Crawl complete.")
    