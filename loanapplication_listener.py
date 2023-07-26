from datetime import datetime, timezone
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def loanapplication_listener():
    FMT = "%(asctime)s [%(levelname)s] - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=FMT,
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    load_dotenv()

    df_loanapplications = pd.DataFrame(columns=['_id', 'deleted', 'dateCreated', 'name', 'email', 'phone_number',
                          'status', 'assignee', 'products', 'dealId'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/loanapplications.csv'):
        df_loanapplications.to_csv('dags/data/daily_updates/loanapplications.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["loanapplications"]
    elements_array = []

    logging.info("Listening loanapplications.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (loanapplications)")

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
                                '$match': {
                                    'dateCreated': {
                                        '$gt': datetime(2022, 10, 5, 0, 0, 0, tzinfo=timezone.utc)
                                    }
                                }
                            },

                            {
                                '$unwind': {
                                    'path': '$products'
                                }
                            },

                            {
                                '$project': {
                                    'personalDetails.email': 1,
                                    'personalDetails.primaryPhoneNumber': 1,
                                    'businessDetails.name': 1,
                                    'deleted': 1, 
                                    'dateCreated': 1, 
                                    'assignee': 1, 
                                    'status': 1, 
                                    'products': 1, 
                                    'dealId': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    
                    for element in mongo_query:
                        elem_dict = {}                    
                        # element = mongo_query[0]

                        elem_dict["_id"] = element.get("_id", None)
                        elem_dict["deleted"] = element.get("deleted", False)
                        elem_dict["dateCreated"] = element.get("dateCreated", datetime(1990, 1, 1))
                        elem_dict['name'] = element.get("businessDetails", {}).get("name", None)
                        elem_dict['email'] = element.get("personalDetails", {}).get("email", None)
                        elem_dict['phoneNumber'] = element.get("personalDetails", {}).get("primaryPhoneNumber", None)
                        elem_dict["status"] = element.get("status", None)
                        elem_dict["assignee"] = element.get("assignee", None)
                        elem_dict["products"] = element.get("products", None)
                        elem_dict["dealId"] = element.get("dealId", None)

                        # Append dictionary to list
                        elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['products'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/loanapplications.csv')
                    logging.info("DATA WRITTEN IN CSV (loanapplications)")

                    # # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/loanapplications.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (loanapplications)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (loanapplications)")

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
                                '$match': {
                                    'dateCreated': {
                                        '$gt': datetime(2022, 10, 5, 0, 0, 0, tzinfo=timezone.utc)
                                    }
                                }
                            },

                            {
                                '$project': {
                                    'personalDetails.email': 1,
                                    'personalDetails.primaryPhoneNumber': 1,
                                    'businessDetails.name': 1,
                                    'deleted': 1, 
                                    'dateCreated': 1, 
                                    'assignee': 1, 
                                    'status': 1, 
                                    'products': 1, 
                                    'dealId': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["deleted"] = element.get("deleted", False)
                    elem_dict["dateCreated"] = element.get("dateCreated", datetime(1990, 1, 1))
                    elem_dict['name'] = element.get("businessDetails", {}).get("name", None)
                    elem_dict['email'] = element.get("personalDetails", {}).get("email", None)
                    elem_dict['phoneNumber'] = element.get("personalDetails", {}).get("primaryPhoneNumber", None)
                    elem_dict["status"] = element.get("status", None)
                    elem_dict["assignee"] = element.get("assignee", None)
                    elem_dict["products"] = element.get("products", None)
                    elem_dict["dealId"] = element.get("dealId", None)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/loanapplications.csv')
                    logging.info("DATA WRITTEN IN CSV (loanapplications)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/loanapplications.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (loanapplications)")

loanapplication_listener()
