import psycopg2
from datetime import datetime
import json
import requests
import logging
from dotenv import load_dotenv
import os


#logging configuration
logging.basicConfig(level=logging.INFO, filename="log.log", filemode='a',
                    format="%(asctime)s-%(levelname)s - %(message)s")


#Data extractions function    
def extract_data():
    try:
        url = "https://api.rentcast.io/v1/listings/sale?city=Austin&state=TX&status=Active&limit=500"
        API_key = os.getenv("API_KEYS")
        headers = {
            "accept": "application/json",
            "X-Api-Key":API_key
        }

        response = requests.get(url, headers=headers)
        
        raw_data = response.json()
        return raw_data
    except requests.exceptions.RequestException as err:
        logging.info(err)
        

# function to date transformation
def convert_year(year):
    if year is None or not str(year).strip():  # Check for None or empty string
        return None  
    try:
        return date(int(year), 1, 1)
    except ValueError as err:
        logging.info(err)
        return None  # Handle other invalid inputs  