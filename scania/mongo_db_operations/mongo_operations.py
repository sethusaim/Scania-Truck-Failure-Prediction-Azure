import json
import os

import pandas as pd
from pymongo import MongoClient
from utils.read_params import read_params


class MongoDB_Operation:
    """
    Description :   This method is used for all mongodb operations

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.DB_URL = os.environ["MONGODB_URL"]

        self.client = MongoClient(self.DB_URL)

    def get_database(self, db_name):
        """
        Method Name :   get_database
        Description :   This method is creating a database in MongoDB

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            db = self.client[db_name]

            return db

        except Exception as e:
            raise e

    def get_collection(self, collection_name, database):
        """
        Method Name :   get_collection
        Description :   This method is used for getting collection from a database

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            collection = database[collection_name]

            return collection

        except Exception as e:
            raise e

    def get_collection_as_dataframe(self, db_name, collection_name):
        """
        Method Name :   get_collection_as_dataframe
        Description :   This method is used for converting the selected collection to dataframe

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            database = self.get_database(db_name)

            collection = self.get_collection(
                collection_name=collection_name, database=database
            )

            df = pd.DataFrame(list(collection.find()))

            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)

            return df

        except Exception as e:
            raise e

    def insert_dataframe_as_record(self, data_frame, db_name, collection_name):
        """
        Method Name :   insert_dataframe_as_record
        Description :   This method is used for inserting the dataframe in collection as record

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            records = json.loads(data_frame.T.to_json()).values()

            database = self.get_database(db_name)

            collection = self.get_collection(
                collection_name=collection_name, database=database
            )

            collection.insert_many(records)

        except Exception as e:
            raise e

    def insert_record(self, db_name, collection_name, data):
        """
        Method Name :   insert_record
        Description :   This method is used for inserting a record in database collection

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            db = self.get_database(db_name=db_name)

            collection = self.get_collection(
                collection_name=collection_name, database=db
            )

            collection.insert_one(data)

        except Exception as e:
            raise e
