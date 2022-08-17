from mongodb.mongo_connector import MongoConnector
from sql.sql_connector import SQLconnector


def mongo_example(credentials, db_name, collection):
    connector = MongoConnector(credentials_file=credentials, db_name=db_name)

    find_query = {
        "field1": "value_to_match"
    }
    fields = {
        "_id": 0,
        "field1": 1,
        "field2": 1
    }
    data = connector.find_query(collection, find_query, fields)
    connector.close()
    return data


def sql_example(credentials, db_name):
    connector = SQLconnector(credentials_file=credentials, db_name=db_name)
    query = (
        "SELECT * FROM table;"
    )
    data = connector.execute_query(query)
    connector.close()
    return data


if __name__ == "__main__":
    mongo_data = mongo_example("mongodb/credentials/example_credentials.json", "example_db_name", "collection_to_search")
    sql_data = sql_example("sql/credentials/example_credentials.json", "example_db_name")
