from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def organizations_listener():
    FMT = "%(asctime)s [%(levelname)s] - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=FMT,
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    load_dotenv()

    df_organizations = pd.DataFrame(columns=['_id', 'business_name', 'registration_number', 'type', 'value_chain', 'created_by', 'org_user', 'owner',
                          'deleted', 'date_created', 'business_operations', 'business_line', 'business_type', 'business_date_created', 'business_owner', 'employees_amount', 'avenews_reason'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/organizations.csv'):
        df_organizations.to_csv('dags/data/daily_updates/organizations.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["organizations"]
    elements_array = []

    logging.info("Listening organizations.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (organizations)")

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
                        
                                    # Organization information
                                    'businessName': 1,
                                    'businessAddress': 1,
                                    'registrationNumber': 1,
                                    'type': 1,
                                    'valueChain': 1,

                                    # Users information
                                    'createdBy': 1,
                                    'orgUser': 1,
                                    'owner': 1,

                                    # Status
                                    'deleted': 1,

                                    # Dates
                                    'dateCreated': 1,

                                    # Onboarding information
                                    'onboardingInformation': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Organization information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["business_name"] = element.get("businessName", None)
                    elem_dict["registration_number"] = element.get("registrationNumber", None)      
                    elem_dict["type"] = element.get("businessName", None)
                    elem_dict["value_chain"] = element.get("valueChain", None)

                    # User informarmation
                    elem_dict["created_by"] = element.get("createdBy", None)
                    elem_dict["org_user"] = element.get("orgUser", None)
                    elem_dict["owner"] = element.get("owner", None)

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)

                    # Dates
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Onboarding information
                    elem_dict["business_operations"] = element.get("onboardingInformation", {}).get("businessOperations", None)
                    elem_dict["business_line"] = element.get("onboardingInformation", {}).get("businessLine", None)
                    elem_dict["business_type"] = element.get("onboardingInformation", {}).get("businessType", None)
                    elem_dict["business_date_created"] = element.get("onboardingInformation", {}).get("businessDateCreated", None)
                    elem_dict["business_owner"] = element.get("onboardingInformation", {}).get("businessOwner", None)
                    elem_dict["employees_amount"] = element.get("onboardingInformation", {}).get("employeesAmount", None)
                    elem_dict["avenews_reason"] = element.get("onboardingInformation", {}).get("avenewsReason", None)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/organizations.csv')
                    logging.info("DATA WRITTEN IN CSV (organizations)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/organizations.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (organizations)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (organizations)")

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
                        
                                    # Organization information
                                    'businessName': 1,
                                    'businessAddress': 1,
                                    'registrationNumber': 1,
                                    'type': 1,
                                    'valueChain': 1,

                                    # Users information
                                    'createdBy': 1,
                                    'orgUser': 1,
                                    'owner': 1,

                                    # Status
                                    'deleted': 1,

                                    # Dates
                                    'dateCreated': 1,

                                    # Onboarding information
                                    'onboardingInformation': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Organization information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["business_name"] = element.get("businessName", None)
                    elem_dict["registration_number"] = element.get("registrationNumber", None)      
                    elem_dict["type"] = element.get("businessName", None)
                    elem_dict["value_chain"] = element.get("valueChain", None)

                    # User informarmation
                    elem_dict["created_by"] = element.get("createdBy", None)
                    elem_dict["org_user"] = element.get("orgUser", None)
                    elem_dict["owner"] = element.get("owner", None)

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)

                    # Dates
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Onboarding information
                    elem_dict["business_operations"] = element.get("onboardingInformation", {}).get("businessOperations", None)
                    elem_dict["business_line"] = element.get("onboardingInformation", {}).get("businessLine", None)
                    elem_dict["business_type"] = element.get("onboardingInformation", {}).get("businessType", None)
                    elem_dict["business_date_created"] = element.get("onboardingInformation", {}).get("businessDateCreated", None)
                    elem_dict["business_owner"] = element.get("onboardingInformation", {}).get("businessOwner", None)
                    elem_dict["employees_amount"] = element.get("onboardingInformation", {}).get("employeesAmount", None)
                    elem_dict["avenews_reason"] = element.get("onboardingInformation", {}).get("avenewsReason", None)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/organizations.csv')
                    logging.info("DATA WRITTEN IN CSV (organizations)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/organizations.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (organizations)")

organizations_listener()
