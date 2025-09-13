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


# creating database schema, tables and relationship
def create_tables():
    try:
        with database_connection() as conn:
            with conn.cursor() as cur:
                querries = [
                    """CREATE SCHEMA IF NOT EXISTS royal_realtors;""",
                    """DROP TABLE IF EXISTS royal_realtors.property_listing CASCADE;""",
                    """DROP TABLE IF EXISTS royal_realtors.property_specification CASCADE;""",
                    """DROP TABLE IF EXISTS royal_realtors.property_location CASCADE;""",
                                            
                    """CREATE TABLE royal_realtors.property_location(
                        location_id SERIAL PRIMARY KEY,
                        address VARCHAR (300),
                        city VARCHAR (150),
                        county VARCHAR (150),
                        latitude FLOAT,
                        longitude FLOAT
                        );""",
                    """CREATE TABLE royal_realtors.property_specification(
                        property_id VARCHAR PRIMARY KEY,
                        location_id SERIAL,
                        citnumber_of_rooms INTEGER,
                        number_of_bathrooms INTEGER,
                        property_size FLOAT,
                        year_built DATE,
                        property_type VARCHAR(100),
                        lot_size FLOAT,
                        FOREIGN KEY (location_id) REFERENCES royal_realtors.property_location(location_id)
                        );""",
                                            
                    """CREATE TABLE royal_realtors.property_listing(
                        listing_id SERIAL PRIMARY KEY,
                        property_id VARCHAR,
                        price FLOAT,
                        listing_type  VARCHAR(150),
                        listed_date DATE,
                        status VARCHAR(50),
                        FOREIGN KEY (property_id) REFERENCES royal_realtors.property_specification(property_id)
                        );"""
                        ]
                for querry in querries:
                    cur.execute(querry)
                    logging.info(f"Executed Querry: {querry.split()[:30]}")

    except Exception as e:
        logging.error(f"connection error:{e}")
        raise  

#load property_location Table
def load_property_location(data):
    conn=database_connection()
    cursor = conn.cursor()
    insert_query =""" INSERT INTO royal_realtors.property_location(address, city, county, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s);"""
                        
    
                
    data_to_insert = [(entry['address'], entry['city'],  entry['county'], entry['latitude'], entry['longitude'])
                    for entry in data]
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction
    conn.commit()
    print(f"Inserted {cursor.rowcount} rows into property_-location table.")
        

#load property specification table
def load_property_specification(data):
    conn=database_connection()
    cursor = conn.cursor()
    insert_query =""" INSERT INTO royal_realtors.property_specification(property_id,  citnumber_of_rooms, number_of_bathrooms, property_size, year_built, property_type, lot_size)
                           VALUES (%s, %s, %s, %s, %s, %s, %s);"""
                           
    data_to_insert = [(entry['property_id'], entry['number_of_rooms'],  entry['number_of_bathrooms'], entry['property_size'], entry['year_built'], entry['property_type'], entry['lot_size'])
                     for entry in data]
    cursor.executemany(insert_query, data_to_insert)                       
                           
    # Commit the transaction
    conn.commit()
    print(f"Inserted {cursor.rowcount} rows into property_specification table.")
    

#load property_listing table
def load_property_listing(data):

    conn=database_connection()
    cursor = conn.cursor()
    insert_query =""" INSERT INTO royal_realtors.property_listing(
                            property_id,
                            price, 
                            listing_type,
                            listed_date, 
                            status)
                           VALUES (%s, %s, %s, %s, %s);"""
                           
    data_to_insert = [(entry['property_id'],
                       entry['price'], 
                       entry['listing_type'],
                       entry['listed_date'],
                       entry['status'])
                     for entry in data]
    cursor.executemany(insert_query, data_to_insert)                       
                           
    # Commit the transaction
    conn.commit()
    print(f"Inserted {cursor.rowcount} rows into listing_tables table.")
    cursor.close()
    conn.close()
    
    
if __name__ == "__main__":
    raw_data = extract_data()
    location_data, listing_data, specification_data = transform_data(raw_data)
    create_tables()
    load_property_location(location_data)
    load_property_specification(specification_data)
    load_property_listing(listing_data)
