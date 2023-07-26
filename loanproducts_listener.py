from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def loanproducts_listener():
    FMT = "%(asctime)s [%(levelname)s] - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=FMT,
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    load_dotenv()

    df_loanproducts = pd.DataFrame(columns=['_id', 'name', 'productType', 'type', 'sellersType', 'totalBuyingPrice'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/loanproducts.csv'):
        df_loanproducts.to_csv('dags/data/daily_updates/loanproducts.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["loanproducts"]
    elements_array = []

    logging.info("Listening loanproducts.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (loanproducts)")

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

                                    'name': 1,
                                    'productType': 1,
                                    'type': 1,
                                    'sellersType': 1,
                                    'totalBuyingPrice': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["name"] = element.get("name", None)
                    elem_dict["productType"] = element.get("productType", None)
                    elem_dict["type"] = element.get("type", None)
                    elem_dict["sellersType"] = element.get("sellersType", None)
                    elem_dict["totalBuyingPrice"] = element.get('totalBuyingPrice', None)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/loanproducts.csv')
                    logging.info("DATA WRITTEN IN CSV (loanproducts)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/loanproducts.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (loanproducts)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (loanproducts)")

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

                                    'name': 1,
                                    'productType': 1,
                                    'type': 1,
                                    'sellersType': 1,
                                    'totalBuyingPrice': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["name"] = element.get("name", None)
                    elem_dict["productType"] = element.get("productType", None)
                    elem_dict["type"] = element.get("type", None)
                    elem_dict["sellersType"] = element.get("sellersType", None)
                    elem_dict["totalBuyingPrice"] = element.get('totalBuyingPrice', None)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/loanproducts.csv')
                    logging.info("DATA WRITTEN IN CSV (loanproducts)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/loanproducts.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (loanproducts)")

loanproducts_listener()
