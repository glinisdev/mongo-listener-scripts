from datetime import datetime
import pandas as pd
import pymongo
import logging
import os
from bson import ObjectId
from dotenv import load_dotenv
import boto3


def mlscore_listener():
    FMT = "%(asctime)s [%(levelname)s] - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=FMT,
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    load_dotenv()

    df_mlscore = pd.DataFrame(columns=['_id', 'loanId', 'score', 'categoriesTotalScore', 'dateCreated'])

    # Create empty .csv if there is not
    if not os.path.exists('dags/data/daily_updates/mlscore.csv'):
        df_mlscore.to_csv('dags/data/daily_updates/mlscore.csv', header=True)

    conn_str = "mongodb+srv://readonly:" + os.getenv("MONGO_DB_PASSWORD") + "@production.cstrb.mongodb.net/agt4-kenya-prod?" \
               "authSource=admin&replicaSet=atlas-4ip6rz-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    mongo_db = client["agt4-kenya-prod"]
    db_applications = mongo_db["mlscoredatas"]
    elements_array = []

    logging.info("Listening mlscoredatas.......")

    try:
        resume_token = None
        with db_applications.watch() as stream:
            for update_change in stream:
                if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (mlscoredatas)")

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
                                    'loanId': 1,
                                    'score': 1,
                                    'categoriesTotalScore': 1,
                                    'dateCreated': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["loanId"] = element.get("loanId", None)
                    elem_dict["score"] = element.get("score", None)
                    elem_dict["categoriesTotalScore"] = element.get("categoriesTotalScore", None)
                    elem_dict["dateCreated"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/mlscore.csv')
                    logging.info("DATA WRITTEN IN CSV (mlscoredatas)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/mlscore.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (mlscoredatas)")

                resume_token = stream.resume_token

    except pymongo.errors.PyMongoError:
        if resume_token is None:
            logging.error('...')
        else:
            with db_applications.watch(resume_after=resume_token) as stream:
                for update_change in stream:

                  if update_change['operationType'] in ['update', 'insert', 'replace']:

                    logging.info(f"Catch type: {update_change['operationType']} (mlscoredatas)")

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
                                    'loanId': 1,
                                    'score': 1,
                                    'categoriesTotalScore': 1,
                                    'dateCreated': 1
                                }
                            }
                        ]))

                    # Create dictionary
                    elem_dict = {}
                    element = mongo_query[0]

                    elem_dict["_id"] = element.get("_id", None)
                    elem_dict["loanId"] = element.get("loanId", None)
                    elem_dict["score"] = element.get("score", None)
                    elem_dict["categoriesTotalScore"] = element.get("categoriesTotalScore", None)
                    elem_dict["dateCreated"] = element.get("dateCreated", datetime(1990, 1, 1))

                    # Append dictionary to list
                    elements_array.append(elem_dict)

                    # Create DF from list of dict
                    df = pd.DataFrame(elements_array)

                    # Drop dublicates
                    df.drop_duplicates(['_id'], keep='last', inplace=True)

                    # Write .csv
                    df.to_csv('dags/data/daily_updates/mlscore.csv')
                    logging.info("DATA WRITTEN IN CSV (mlscoredatas)")

                    # upload file:
                    s3 = boto3.resource('s3')

                    bucket = 'avenews-airflow'
                    filename = 'dags/data/daily_updates/mlscore.csv'
                    s3.meta.client.upload_file(Filename = filename, Bucket= bucket, Key = filename)

                    logging.info("Uploaded CSV to S3 (mlscoredatas)")

mlscore_listener()
