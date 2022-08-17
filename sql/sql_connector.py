import pymysql.cursors
import json


class SQLconnector:

    def __init__(self, credentials_file, dbname="stats"):
        self.credentials_file = credentials_file
        self.dbname = dbname
        self.credentials = self._set_credentials()
        self.connection, self.entity_cursor = self._set_connection()

    def _set_credentials(self):
        with open(self.credentials_file) as f:
            credentials = json.load(f)
        return credentials

    def _set_connection(self):
        connection, entity_cursor = self.open_connection(self.credentials["HOST"],
                                                         self.credentials["PORT"],
                                                         self.credentials["USER"],
                                                         self.credentials["PASSWORD"])
        return connection, entity_cursor

    def open_connection(self, host, port, user, password):
        connection = pymysql.connect(host=host, port=port, user=user, password=password, db=self.dbname)
        entity_cursor = connection.cursor(pymysql.cursors.DictCursor)
        return connection, entity_cursor

    def execute_query(self, query):
        self.entity_cursor.execute(query)
        results = [row for row in self.entity_cursor.fetchall()]
        return results

    def execute_query_one_field(self, query, field):
        self.entity_cursor.execute(query)
        results = [row[field] for row in self.entity_cursor.fetchall()]
        return results

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    _query = "SELECT * FROM restaurant LIMIT 10;"

    sql = SQLconnector(credentials_file="sql_connector/credentials.json", dbname="stats")
    sql.execute_query(_query)
    sql.close()

