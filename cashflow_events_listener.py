from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def cashflow_events_listener():
    FMT = "%(asctime)s [%(levelname)s] - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=FMT,
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    load_dotenv()

    df_cashflow_events = pd.DataFrame(columns=['_id', 'organization', 'amount', 'type', 'created_by', 'products',
                          'deleted', 'status', 'date', 'date_created'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/cashflow_events.csv'):
        df_cashflow_events.to_csv('dags/data/daily_updates/cashflow_events.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["cashflowevents"]
    elements_array = []

    logging.info("Listening cashflowevents.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (cashflowevents)")

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

                                    # Event information
                                    'organization': 1,
                                    'products': 1,
                                    'amount': 1,
                                    'type': 1,
                                    'createdBy': 1,

                                    # Product information               
                                    'products': 1,

                                    # Status
                                    'deleted': 1,
                                    'status': 1,

                                    # Dates
                                    'date': 1,
                                    'dateCreated': 1,
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Event information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["organization"] = element.get("organization", None)
                    elem_dict["amount"] = element.get("amount", None)
                    elem_dict["type"] = element.get("type", None)
                    elem_dict["created_by"] = element.get("createdBy", None)

                    # Product information
                    elem_dict["products"] = element.get("products", None)

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)
                    elem_dict["status"] = element.get("status", False)

                    # Dates
                    elem_dict["date"] = element.get("date", datetime(1990, 1, 1))
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/cashflow_events.csv')
                    logging.info("DATA WRITTEN IN CSV (cashflowevents)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/cashflow_events.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (cashflowevents)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (cashflowevents)")

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

                                    # Event information
                                    'organization': 1,
                                    'products': 1,
                                    'amount': 1,
                                    'type': 1,
                                    'createdBy': 1,

                                    # Product information               
                                    'products': 1,

                                    # Status
                                    'deleted': 1,
                                    'status': 1,

                                    # Dates
                                    'date': 1,
                                    'dateCreated': 1,
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Event information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["organization"] = element.get("organization", None)
                    elem_dict["amount"] = element.get("amount", None)
                    elem_dict["type"] = element.get("type", None)
                    elem_dict["created_by"] = element.get("createdBy", None)

                    # Product information
                    elem_dict["products"] = element.get("products", None)

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)
                    elem_dict["status"] = element.get("status", False)

                    # Dates
                    elem_dict["date"] = element.get("date", datetime(1990, 1, 1))
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/cashflow_events.csv')
                    logging.info("DATA WRITTEN IN CSV (cashflowevents)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/cashflow_events.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (cashflowevents)")

cashflow_events_listener()
