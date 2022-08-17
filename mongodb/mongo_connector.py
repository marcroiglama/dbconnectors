import json
from pymongo import MongoClient


class MongoConnector:

    def __init__(self, credentials_file, db_name):
        self.credentials_file = credentials_file
        self.credentials = self._set_credentials()
        self.db_name = db_name
        self.client, self.db = self._set_connection()

    def _set_credentials(self):
        with open(self.credentials_file) as f:
            credentials = json.load(f)
        return credentials

    @staticmethod
    def process_fields(fields):
        if type(fields) == list:
            fields = {field: 1 for field in fields}
        fields["_id"] = 0
        return fields

    def _set_connection(self):
        connection_string = "mongodb://{}:{}@{}:{}/?authSource={}".format(
            self.credentials["USER"],
            self.credentials["PASSWORD"],
            self.credentials["HOST"],
            self.credentials["PORT"],
            self.credentials["AUTH"]
        )
        client = MongoClient(connection_string)
        db = client[self.db_name]
        return client, db

    def find_query(self, collection, query, fields):
        fields = self.process_fields(fields)
        collection_con = self.db[collection]
        result = list(collection_con.find(query, fields))
        return result

    def find_query_lazy(self, collection, query, fields):
        fields = self.process_fields(fields)
        collection_con = self.db[collection]
        result = collection_con.find(query, fields)
        return result

    def aggregate(self, collection, query):
        collection_con = self.db[collection]
        return list(collection_con.aggregate(query))

    def count(self, collection, query):
        collection_con = self.db[collection]
        result = collection_con.count_documents(query)
        return result

    def close(self):
        self.client.close()

