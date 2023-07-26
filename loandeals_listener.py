from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def loandeals_listener():
    FMT = "%(asctime)s [%(levelname)s] - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=FMT,
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    load_dotenv()

    df_loandeals = pd.DataFrame(columns=['_id', 'minOffer', 'totalBuying', 'periodWeeks', 'deleted'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/loandeals.csv'):
        df_loandeals.to_csv('dags/data/daily_updates/loandeals.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["loandeals"]
    elements_array = []

    logging.info("Listening loandeals.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (loandeals)")

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
                                    'minOffer': 1,
                                    'totalBuying': 1,
                                    'periodWeeks': 1,
                                    'deleted': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]
                    
                    elem_dict["_id"] = element.get("_id", None)       
                    elem_dict["minOffer"] = element.get("minOffer", None)
                    elem_dict["totalBuying"] = element.get("totalBuying", None)
                    elem_dict["periodWeeks"] = element.get("periodWeeks", None)
                    elem_dict["deleted"] = element.get("deleted", False)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/loandeals.csv')
                    logging.info("DATA WRITTEN IN CSV (loandeals)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/loandeals.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (loandeals)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (loandeals)")

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
                                    'minOffer': 1,
                                    'totalBuying': 1,
                                    'periodWeeks': 1,
                                    'deleted': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]
                    
                    elem_dict["_id"] = element.get("_id", None)       
                    elem_dict["minOffer"] = element.get("minOffer", None)
                    elem_dict["totalBuying"] = element.get("totalBuying", None)
                    elem_dict["periodWeeks"] = element.get("periodWeeks", None)
                    elem_dict["deleted"] = element.get("deleted", False)

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/loandeals.csv')
                    logging.info("DATA WRITTEN IN CSV (loandeals)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/loandeals.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (loandeals)")

loandeals_listener()
