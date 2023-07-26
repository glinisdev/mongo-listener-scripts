from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def user_listener():
    FMT = "%(asctime)s [%(levelname)s] - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=FMT,
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    load_dotenv()

    df_users = pd.DataFrame(columns=['_id', 'username', 'first_name', 'last_name', 'email', 'phone_number', 'company_name', 'roles', 'deleted', 'blocked',
                          'has_password', 'logged_in', 'account_reviewed', 'validation_email', 'validation_phone_number', 'date_created', 'last_login'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/users.csv'):
        df_users.to_csv('dags/data/daily_updates/users.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["users"]
    elements_array = []

    logging.info("Listening users.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']}")

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

                                    # Personal information
                                    'username': 1,
                                    'personalInformation.firstName': 1,
                                    'personalInformation.lastName': 1,
                                    'personalInformation.email': 1,
                                    'personalInformation.phoneNumber': 1,

                                    # Business information
                                    'companyInformation.companyName': 1,
                                    'roles': 1,

                                    # Status
                                    'deleted': 1,
                                    'blocked': 1,
                                    'hasPassword': 1,
                                    'loggedIn': 1,
                                    'accountReviewed': 1,
                                    'validations': 1,

                                    # Dates
                                    'lastLogin': 1,
                                    'dateCreated': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Personal information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["username"] = element.get("username", None)
                    elem_dict["first_name"] = element.get("personalInformation", {}).get("firstName", None)
                    elem_dict["last_name"] = element.get("personalInformation", {}).get("lastName", None)
                    elem_dict["email"] = element.get("personalInformation", {}).get("email", None)
                    elem_dict["phone_number"] = element.get("personalInformation", {}).get("phoneNumber", None)

                    # Business information
                    elem_dict["company_name"] = element.get("companyInformation", {}).get("companyName", None)
                    elem_dict["roles"] = element.get("roles", None)

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)
                    elem_dict["blocked"] = element.get("blocked", False)
                    elem_dict["has_password"] = element.get("hasPassword", False)
                    elem_dict["logged_in"] = element.get("loggedIn", False)
                    elem_dict["account_reviewed"] = element.get("accountReviewed", False)
                    elem_dict["validation_email"] = element.get("validations", {}).get("email", False)
                    elem_dict["validation_phone_number"] = element.get("validations", {}).get("phoneNumber", False)

                    # Dates
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
                    elem_dict["last_login"] = element.get("lastLogin", datetime(1990, 1, 1))
                    # elem_dict["timeStamp"] = timestamp

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/users.csv')
                    logging.info("DATA WRITTEN IN CSV (users)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/users.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (users)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (users)")

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

                                    # Personal information
                                    'username': 1,
                                    'personalInformation.firstName': 1,
                                    'personalInformation.lastName': 1,
                                    'personalInformation.email': 1,
                                    'personalInformation.phoneNumber': 1,

                                    # Business information
                                    'companyInformation.companyName': 1,
                                    'roles': 1,

                                    # Status
                                    'deleted': 1,
                                    'blocked': 1,
                                    'hasPassword': 1,
                                    'loggedIn': 1,
                                    'accountReviewed': 1,
                                    'validations': 1,

                                    # Dates
                                    'lastLogin': 1,
                                    'dateCreated': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    # Personal information
                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["username"] = element.get("username", None)
                    elem_dict["first_name"] = element.get("personalInformation", {}).get("firstName", None)
                    elem_dict["last_name"] = element.get("personalInformation", {}).get("lastName", None)
                    elem_dict["email"] = element.get("personalInformation", {}).get("email", None)
                    elem_dict["phone_number"] = element.get("personalInformation", {}).get("phoneNumber", None)

                    # Business information
                    elem_dict["company_name"] = element.get("companyInformation", {}).get("companyName", None)
                    elem_dict["roles"] = element.get("roles", None)

                    # Status
                    elem_dict["deleted"] = element.get("deleted", False)
                    elem_dict["blocked"] = element.get("blocked", False)
                    elem_dict["has_password"] = element.get("hasPassword", False)
                    elem_dict["logged_in"] = element.get("loggedIn", False)
                    elem_dict["account_reviewed"] = element.get("accountReviewed", False)
                    elem_dict["validation_email"] = element.get("validations", {}).get("email", False)
                    elem_dict["validation_phone_number"] = element.get("validations", {}).get("phoneNumber", False)

                    # Dates
                    elem_dict["date_created"] = element.get("dateCreated", datetime(1990, 1, 1))
                    elem_dict["last_login"] = element.get("lastLogin", datetime(1990, 1, 1))
                    # elem_dict["timeStamp"] = timestamp

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/users.csv')
                    logging.info("DATA WRITTEN IN CSV (users)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/users.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (users)")

user_listener()
