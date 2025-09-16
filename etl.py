import psycopg2
from datetime import datetime, date
import json
import requests
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO, filename="log.log", filemode='a',
                    format="%(asctime)s-%(levelname)s - %(message)s")

load_dotenv()

#Data extractions    
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
        

 # date formating and validation
def convert_year(year):
    from datetime import date
    if year is None or not str(year).strip():  # Check for None or empty string
        return None  
    try:
        return date(int(year), 1, 1)
    except ValueError as err:
        logging.info(err)
        return None  # Handle other invalid inputs  
        
    
# data transformation 
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
    logging.info("property_location transformation completed")

    listing_details = []
    for entries in raw_data: 
        property_id = entries['id']
        price= entries.get('price')
        listing_type= entries.get('listingType')
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
    logging.info("listing_details transformation completed")    
    
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
    logging.info("all data transformed succesfully")
        
    return property_location, property_specifications, listing_details 


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
    #handle database connection exceptions
    except Exception as e:
        logging.error(f"error: {e}")
        raise  # Raise exceptions


# creating database schema, tables and relationship using context manager
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
                        number_of_rooms INTEGER,
                        number_of_bathrooms INTEGER,
                        property_size FLOAT,
                        year_built DATE,
                        property_type VARCHAR(100),
                        lot_size FLOAT
                        );""",
                                            
                    """CREATE TABLE royal_realtors.property_listing(
                        location_id INT,
                        property_id VARCHAR,
                        PRIMARY KEY (location_id, property_id),
                        price FLOAT,
                        listing_type  VARCHAR(150),
                        listed_date DATE,
                        status VARCHAR(50),
                        FOREIGN KEY (location_id) REFERENCES royal_realtors.property_location(location_id),
                        FOREIGN KEY (property_id) REFERENCES royal_realtors.property_specification(property_id)
                        );"""
                        ]
                for querry in querries:
                    cur.execute(querry)
                    conn.commit()
                    logging.info(f"Executed Querry: {querry.split()[:30]}")
    #handle schema creation exceptions
    except Exception as e:
        logging.error(f"connection error:{e}")
        raise       

#loading data into database 
def load_data(property_location, property_specification, property_listing ):
    try:
        with database_connection() as conn:
            with conn.cursor() as cursor:
                
                #loading property_location table
                insert_location_query =""" INSERT INTO royal_realtors.property_location(address, city, county, latitude, longitude)
                                    VALUES (%s, %s, %s, %s, %s)
                                    RETURNING location_id;"""
                location_ids = []               
                for entry in property_location:
                    cursor.execute(insert_location_query, (
                        entry['address'],
                        entry['city'], 
                        entry['county'],
                        entry['latitude'],
                        entry['longitude']
                        )
                                   )
                    location_id = cursor.fetchone()[0]
                    location_ids.append(location_id) #save location_id for loading as secodary key in other tables
                logging.info(f"Inserted {len(location_ids)} rows into property_-location table.")
                
                #loading property_specification table
                insert_specification_query ="""
                    INSERT INTO royal_realtors.property_specification(
                        property_id,
                        number_of_rooms,
                        number_of_bathrooms,
                        property_size,
                        year_built,
                        property_type,
                        lot_size
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING property_id;"""
                 
                property_ids = []              
                for i, entry in enumerate(property_specification):
                    cursor.execute(insert_specification_query, (
                        entry['property_id'],
                        entry['number_of_rooms'], 
                        entry['number_of_bathrooms'],
                        entry['property_size'],
                        entry['year_built'],
                        entry['property_type'],
                        entry['lot_size']
                    )
                    )
                    property_id = cursor.fetchone()[0]
                    property_ids.append(property_id)
                logging.info(f"Inserted {len(property_ids)} rows into property_specification table.")

                #loading property listing table
                insert_listing_query =""" INSERT INTO royal_realtors.property_listing(
                    location_id,
                    property_id,
                    price, 
                    listing_type,
                    listed_date, 
                    status
                    )
                        VALUES (%s, %s, %s, %s, %s, %s);"""
                
                for i, entry in enumerate(property_listing):
                    cursor.execute(insert_listing_query, 
                        (
                            location_ids[i],
                            property_ids[i],
                            entry['price'],
                            entry['listing_type'],
                            entry['listed_date'],
                            entry['status']
                            )
                    )                                    
                
                logging.info(f"Inserted {len(location_ids)} rows into property_listing table.")
                conn.commit()
    #handle exceptions               
    except Exception as err:
        logging.error(f"data loading error:: {err}")            
        raise  
    

if __name__ == "__main__":
    raw_data =extract_data()
    location_data, specification_data, listing_data = transform_data(raw_data)
    create_tables()
    load_data(location_data, specification_data, listing_data)