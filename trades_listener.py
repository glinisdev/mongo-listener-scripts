from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def trades_listener():
    FMT = "%(asctime)s [%(levelname)s] - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=FMT,
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    # Get method for list
    def safe_list_get(l, idx, default):
        try:
            return l[idx]
        except IndexError:
            return default

    load_dotenv()

    df_trades = pd.DataFrame(columns=['_id', 'type', 'name', 'product_id', 'product_name', 'package_size', 'measurement_unit', 'unit_price', 'quantity', 'total_price',
                          'number', 'organization', 'created_by', 'notes', 'status', 'deleted', 'date', 'due_date', 'date_created'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/trades.csv'):
        df_trades.to_csv('dags/data/daily_updates/trades.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["trades"]
    elements_array = []

    logging.info("Listening trades.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (trades)")

                    # Extract ID and TimeStamp
                    id = update_change['documentKey']['_id']

                    # Quering from mongo by ID
                    mongo_query = list(db_applications.aggregate(
                        [
                            {
                                '$match': {
                                    '_id': ObjectId(id)
                                }
                            },

                            {
                                '$project': {

                                    # Trade information
                                    'type': 1,
                                    'name': 1,
                                    'products': 1,
                                    'totalPrice': 1,
                                    'number': 1,
                                    'organization': 1,
                                    'createdBy': 1,
                                    'notes': 1,

                                    # Status
                                    'status': 1,
                                    'deleted': 1,

                                    # Dates
                                    'date': 1,
                                    'dueDate': 1,
                                    'dateCreated': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Trade information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["type"] = element.get("type", None)
                    elem_dict["name"] = element.get("name", None)

                    elem_dict["product_id"] = safe_list_get(element["products"], 0, {}).get("productId", None)
                    elem_dict["product_name"] = safe_list_get(element["products"], 0, {}).get("name", None)
                    elem_dict["package_size"] = safe_list_get(element["products"], 0, {}).get("packageSize", None)
                    elem_dict["measurement_unit"] = safe_list_get(element["products"], 0, {}).get("measurementUnit", None)
                    elem_dict["unit_price"] = safe_list_get(element["products"], 0, {}).get("unitPrice", None)
                    elem_dict["quantity"] = safe_list_get(element["products"], 0, {}).get("quantity", None)

                    elem_dict["total_price"] = element.get("totalPrice", None)
                    elem_dict["number"] = element.get("number", None)
                    elem_dict["organization"] = element.get("organization", None)
                    elem_dict["created_by"] = element.get("createdBy", None)
                    elem_dict["notes"] = element.get("notes", None)

                    # Status
                    elem_dict["status"] = element.get("status", None)      
                    elem_dict["deleted"] = element.get("deleted", False)

                    # Dates
                    elem_dict["date"] = element.get("date", datetime(1990, 1, 1))
                    elem_dict["due_date"] = element.get("dueDate", datetime(1990, 1, 1))
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
            
                    # Append to array
                    elements_array.append(elem_dict)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/trades.csv')
                    logging.info("DATA WRITTEN IN CSV (trades)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/trades.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (trades)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (trades)")

                    # Extract ID and TimeStamp
                    id = update_change['documentKey']['_id']

                    # Quering from mongo by ID
                    mongo_query = list(db_applications.aggregate(
                        [
                            {
                                '$match': {
                                    '_id': ObjectId(id)
                                }
                            },

                            {
                                '$project': {

                                    # Trade information
                                    'type': 1,
                                    'name': 1,
                                    'products': 1,
                                    'totalPrice': 1,
                                    'number': 1,
                                    'organization': 1,
                                    'createdBy': 1,
                                    'notes': 1,

                                    # Status
                                    'status': 1,
                                    'deleted': 1,

                                    # Dates
                                    'date': 1,
                                    'dueDate': 1,
                                    'dateCreated': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Trade information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["type"] = element.get("type", None)
                    elem_dict["name"] = element.get("name", None)

                    elem_dict["product_id"] = safe_list_get(element["products"], 0, {}).get("productId", None)
                    elem_dict["product_name"] = safe_list_get(element["products"], 0, {}).get("name", None)
                    elem_dict["package_size"] = safe_list_get(element["products"], 0, {}).get("packageSize", None)
                    elem_dict["measurement_unit"] = safe_list_get(element["products"], 0, {}).get("measurementUnit", None)
                    elem_dict["unit_price"] = safe_list_get(element["products"], 0, {}).get("unitPrice", None)
                    elem_dict["quantity"] = safe_list_get(element["products"], 0, {}).get("quantity", None)

                    elem_dict["total_price"] = element.get("totalPrice", None)
                    elem_dict["number"] = element.get("number", None)
                    elem_dict["organization"] = element.get("organization", None)
                    elem_dict["created_by"] = element.get("createdBy", None)
                    elem_dict["notes"] = element.get("notes", None)

                    # Status
                    elem_dict["status"] = element.get("status", None)      
                    elem_dict["deleted"] = element.get("deleted", False)

                    # Dates
                    elem_dict["date"] = element.get("date", datetime(1990, 1, 1))
                    elem_dict["due_date"] = element.get("dueDate", datetime(1990, 1, 1))
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
            
                    # Append to array
                    elements_array.append(elem_dict)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/trades.csv')
                    logging.info("DATA WRITTEN IN CSV (trades)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/trades.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (trades)")

trades_listener()
