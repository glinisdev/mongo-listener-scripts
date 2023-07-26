from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def agribusinesses_listener():
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

    df_agribusinesses = pd.DataFrame(columns=['_id', 'organization', 'business_details_name', 'business_details_phone', 'referrers', 'created_by',
                          'contact_deleted', 'contact_first_name', 'contact_last_name','contact_id', 'contact_date_created', 'deleted', 'date_created'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/agribusinesses.csv'):
        df_agribusinesses.to_csv('dags/data/daily_updates/agribusinesses.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["agribusinesses"]
    elements_array = []

    logging.info("Listening agribusinesses.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (agribusinesses)")

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

                                    # Agribusiness information
                                    'organization': 1,
                                    'businessDetails': 1,
                                    'referrers': 1,
                                    'contacts': 1,
                                    'createdBy': 1,

                                    # Status
                                    'deleted': 1,

                                    # Dates
                                    'dateCreated': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Agribusiness information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["organization"] = element.get("organization", None)
                    elem_dict["business_details_name"] = element.get("businessDetails", {}).get("name", None)
                    elem_dict["business_details_phone"] = element.get("businessDetails", {}).get("phoneNumber", None)
                    elem_dict["referrers"] = safe_list_get(str(element["referrers"]), 0, None)
                    elem_dict["created_by"] = element.get("createdBy", None)

                    # Contact information
                    if element['contacts']:
                        elem_dict["contact_deleted"] = safe_list_get(element["contacts"], 0, {}).get("deleted", False)
                        elem_dict["contact_first_name"] = safe_list_get(element["contacts"], 0, {}).get("firstName", None)
                        elem_dict["contact_last_name"] = safe_list_get(element["contacts"], 0, {}).get("lastName", None)
                        elem_dict["contact_id"] = safe_list_get(element["contacts"], 0, {}).get("_id")
                        elem_dict["contact_date_created"] = safe_list_get(element["contacts"], 0, {}).get("dateCreated", datetime(1990, 1, 1))
                    else:
                        elem_dict["contact_deleted"] = False
                        elem_dict["contact_first_name"] = None
                        elem_dict["contact_last_name"] = None
                        elem_dict["contact_id"] = None
                        elem_dict["contact_date_created"] = datetime(1990, 1, 1)

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)

                    # Dates
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/agribusinesses.csv')
                    logging.info("DATA WRITTEN IN CSV (agribusinesses)")

                    # upload file:

                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/agribusinesses.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (agribusinesses)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (agribusinesses)")

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

                                    # Agribusiness information
                                    'organization': 1,
                                    'businessDetails': 1,
                                    'referrers': 1,
                                    'contacts': 1,
                                    'createdBy': 1,

                                    # Status
                                    'deleted': 1,

                                    # Dates
                                    'dateCreated': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Agribusiness information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["organization"] = element.get("organization", None)
                    elem_dict["business_details_name"] = element.get("businessDetails", {}).get("name", None)
                    elem_dict["business_details_phone"] = element.get("businessDetails", {}).get("phoneNumber", None)
                    elem_dict["referrers"] = safe_list_get(str(element["referrers"]), 0, None)
                    elem_dict["created_by"] = element.get("createdBy", None)

                    # Contact information
                    if element['contacts']:
                        elem_dict["contact_deleted"] = safe_list_get(element["contacts"], 0, {}).get("deleted", False)
                        elem_dict["contact_first_name"] = safe_list_get(element["contacts"], 0, {}).get("firstName", None)
                        elem_dict["contact_last_name"] = safe_list_get(element["contacts"], 0, {}).get("lastName", None)
                        elem_dict["contact_id"] = safe_list_get(element["contacts"], 0, {}).get("_id")
                        elem_dict["contact_date_created"] = safe_list_get(element["contacts"], 0, {}).get("dateCreated", datetime(1990, 1, 1))
                    else:
                        elem_dict["contact_deleted"] = False
                        elem_dict["contact_first_name"] = None
                        elem_dict["contact_last_name"] = None
                        elem_dict["contact_id"] = None
                        elem_dict["contact_date_created"] = datetime(1990, 1, 1)

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)

                    # Dates
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/agribusinesses.csv')
                    logging.info("DATA WRITTEN IN CSV (agribusinesses)")

                    # upload file:

                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/agribusinesses.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (agribusinesses)")

agribusinesses_listener()
