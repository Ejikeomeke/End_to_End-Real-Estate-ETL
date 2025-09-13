import psycopg2
from datetime import datetime, date
import json
import requests
import logging
from dotenv import load_dotenv
import os


#logging configuration
logging.basicConfig(level=logging.INFO, filename="log.log", filemode='a',
                    format="%(asctime)s-%(levelname)s - %(message)s")

load_dotenv() 

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
        logging.info("connection and retrieval successful")
        return raw_data
    except requests.exceptions.RequestException as err:
        logging.info(err)
        

# function for date_data transformation
def convert_year(year):
    if year is None or not str(year).strip():  # Check for None or empty string
        return None  
    try:
        return date(int(year), 1, 1)
    except ValueError as err:
        logging.info(err)
        return None  # Handle other invalid inputs  
    
    
# Function for data transformation 
def transform_data(raw_data):
    property_location = []
    for entries in raw_data:
        address = entries.get('addressLine1')
        city = entries.get('city')
        county = entries.get('county')
        latitude = entries.get('latitude')
        longitude = entries.get('longitude')
        
        property_location.append({
            'address':address,
            'city':city,
            'county':county,
            'latitude':latitude,
            'longitude':longitude
        } 
                                 )

        
    listing_details = []
    for entries in raw_data: 
        property_id = entries['id']
        price= entries.get('price')
        listing_type= entries.get('listingType')
        listing_type = entries.get('listingType')
        listed_date = entries.get('listedDate')
        status = entries.get('status')

        
        listing_details.append({
            'property_id':property_id,
            'price':price,
            'listing_type':listing_type,
            'listed_date':listed_date,
            'status':status,
            }
                               )
        
    
    property_specifications = []
    for entries in raw_data:
        property_id = entries['id']
        number_of_rooms = entries.get('bedrooms')
        number_of_bathrooms = entries.get('bathrooms')
        property_size = entries.get('squareFootage')
        yearBuilt = convert_year(entries.get('yearBuilt'))
        propertyType = entries.get('propertyType')
        lotSize = entries.get('lotSize')
        
        property_specifications.append({
            'property_id':property_id,
            'number_of_rooms':number_of_rooms,
            'number_of_bathrooms': number_of_bathrooms,
            'property_size':property_size,
            'year_built':yearBuilt,
            'property_type':propertyType,
            'lot_size':lotSize
            }
                                       )
    logging.info("data transformation completed")   
    return property_location, listing_details, property_specifications  


def database_connection():
    try:
        # Retrieve environment variables
        user_name=os.getenv("USER_NAME")
        password=os.getenv("PASS_WORD")
        database_name=os.getenv("DATA_BASE")
        host = os.getenv("HOST_NAME")
        port = os.getenv("PORT_NUMBER")

        # Validate environment variables
        if not all([user_name, password, database_name, host, port]):
            missing_variables = [var for var, value in [
                ("USERNAME", user_name),
                ("PASSWORD", password),
                ("DATABASE", database_name),
                ("HOST_NAME", host),
                ("PORT_NUMBER", port)
            ] if not value]
            logging.error(f"Missing environment variables: {', '.join(missing_variables)}")
            raise ValueError(f"Missing environment variables: {', '.join(missing_variables)}")

        # Establish database connection
        connection = psycopg2.connect(
            dbname=database_name,
            user=user_name,
            password=password,
            host=host,
            port=port
            )
        
        logging.info("Connected to database successfully")
        return connection

    except Exception as e:
        logging.error(f"error: {e}")
        raise  # Raise exceptions