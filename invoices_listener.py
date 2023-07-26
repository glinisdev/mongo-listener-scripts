from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def invoices_listener():
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

    df_invoices = pd.DataFrame(columns=['_id', 'organization', 'name', 'phone_number', 'email', 'payment_terms', 'payment_method', 'terms_and_conditions', 'tax',
                          'created_by', 'product_id', 'product_name', 'product_package_size', 'product_measurement_unit', 'product_unit_price', 'product_quantity', 
                          'deleted', 'status', 'issue_date', 'supply_date', 'due_date', 'date_created'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/invoices.csv'):
        df_invoices.to_csv('dags/data/daily_updates/invoices.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["invoices"]
    elements_array = []

    logging.info("Listening invoices.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:
        
                    logging.info(f"Catch type: {update_change['operationType']} (invoices)")

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

                                    # Invoice information
                                    'organization': 1,
                                    'name': 1,
                                    'address': 1,
                                    'phoneNumber': 1,
                                    'email': 1,
                                    'paymentTerms': 1,
                                    'paymentMethod': 1,
                                    'termsAndConditions': 1,
                                    'taxPercentaje': 1,
                                    'total': 1,
                                    'createdBy': 1,
                                    
                                    # Product information               
                                    'products': 1,

                                    # Status
                                    'deleted': 1,
                                    'status': 1,

                                    # Dates
                                    'issueDate': 1,
                                    'supplyDate': 1,
                                    'dueDate': 1,
                                    'dateCreated': 1,
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Invoice information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["organization"] = element.get("organization", None)
                    elem_dict["name"] = element.get("name", None)
                    elem_dict["phone_number"] = element.get("phoneNumber", None)
                    elem_dict["email"] = element.get("email", None)
                    elem_dict["payment_method"] = element.get("paymentMethod", None)
                    elem_dict["payment_terms"] = element.get("paymentTerms", None)
                    elem_dict["terms_and_conditions"] = element.get("termsAndConditions", None)
                    elem_dict["tax"] = element.get("taxPercentaje", None)
                    elem_dict["created_by"] = element.get("createdBy", None)

                    # Product information
                    if element.get("products"):
                        elem_dict["product_id"] = safe_list_get(element["products"], 0, {}).get("productId", None)
                        elem_dict["product_name"] = safe_list_get(element["products"], 0, {}).get("name", None)
                        elem_dict["product_package_size"] = safe_list_get(element["products"], 0, {}).get("packageSize", None)
                        elem_dict["product_measurement_unit"] = safe_list_get(element["products"], 0, {}).get("measurementUnit", None)
                        elem_dict["product_unit_price"] = safe_list_get(element["products"], 0, {}).get("unitPrice", None)
                        elem_dict["product_quantity"] = safe_list_get(element["products"], 0, {}).get("quantity", None)
                    else:
                        elem_dict["product_id"] = None
                        elem_dict["product_name"] = None
                        elem_dict["product_package_size"] = None
                        elem_dict["product_measurement_unit"] = None
                        elem_dict["product_unit_price"] = None
                        elem_dict["product_quantity"] = None

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)
                    elem_dict["status"] = element.get("status", False)

                    # Dates
                    elem_dict["issue_date"] = element.get("issueDate", datetime(1990, 1, 1))
                    elem_dict["supply_date"] = element.get("supplyDate", datetime(1990, 1, 1))
                    elem_dict["due_date"] = element.get("dueDate", datetime(1990, 1, 1))
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/invoices.csv')
                    logging.info("DATA WRITTEN IN CSV (invoices)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/invoices.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (invoices)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (invoices)")

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

                                    # Invoice information
                                    'organization': 1,
                                    'name': 1,
                                    'address': 1,
                                    'phoneNumber': 1,
                                    'email': 1,
                                    'paymentTerms': 1,
                                    'paymentMethod': 1,
                                    'termsAndConditions': 1,
                                    'taxPercentaje': 1,
                                    'total': 1,
                                    'createdBy': 1,
                                    
                                    # Product information               
                                    'products': 1,

                                    # Status
                                    'deleted': 1,
                                    'status': 1,

                                    # Dates
                                    'issueDate': 1,
                                    'supplyDate': 1,
                                    'dueDate': 1,
                                    'dateCreated': 1,
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Invoice information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["organization"] = element.get("organization", None)
                    elem_dict["name"] = element.get("name", None)
                    elem_dict["phone_number"] = element.get("phoneNumber", None)
                    elem_dict["email"] = element.get("email", None)
                    elem_dict["payment_method"] = element.get("paymentMethod", None)
                    elem_dict["payment_terms"] = element.get("paymentTerms", None)
                    elem_dict["terms_and_conditions"] = element.get("termsAndConditions", None)
                    elem_dict["tax"] = element.get("taxPercentaje", None)
                    elem_dict["created_by"] = element.get("createdBy", None)

                    # Product information
                    if element.get("products"):
                        elem_dict["product_id"] = safe_list_get(element["products"], 0, {}).get("productId", None)
                        elem_dict["product_name"] = safe_list_get(element["products"], 0, {}).get("name", None)
                        elem_dict["product_package_size"] = safe_list_get(element["products"], 0, {}).get("packageSize", None)
                        elem_dict["product_measurement_unit"] = safe_list_get(element["products"], 0, {}).get("measurementUnit", None)
                        elem_dict["product_unit_price"] = safe_list_get(element["products"], 0, {}).get("unitPrice", None)
                        elem_dict["product_quantity"] = safe_list_get(element["products"], 0, {}).get("quantity", None)
                    else:
                        elem_dict["product_id"] = None
                        elem_dict["product_name"] = None
                        elem_dict["product_package_size"] = None
                        elem_dict["product_measurement_unit"] = None
                        elem_dict["product_unit_price"] = None
                        elem_dict["product_quantity"] = None

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)
                    elem_dict["status"] = element.get("status", False)

                    # Dates
                    elem_dict["issue_date"] = element.get("issueDate", datetime(1990, 1, 1))
                    elem_dict["supply_date"] = element.get("supplyDate", datetime(1990, 1, 1))
                    elem_dict["due_date"] = element.get("dueDate", datetime(1990, 1, 1))
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/invoices.csv')
                    logging.info("DATA WRITTEN IN CSV (invoices)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/invoices.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (invoices)")


invoices_listener()
