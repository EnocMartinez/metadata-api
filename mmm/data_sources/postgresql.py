#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/10/23
"""

from ..common import LoggerSuperclass
import psycopg2
import time
import pandas as pd


class PgDatabaseConnector(LoggerSuperclass):
    """
    Interface to access a PostgresQL database
    """

    def __init__(self, host, port, db_name, db_user, db_password, logger):
        LoggerSuperclass.__init__(self, logger, "DB connector")
        self.__host = host
        self.__port = port
        self.__name = db_name
        self.__user = db_user
        self.__pwd = db_password

        self.connection = None
        self.cursor = None

        self.query_time = -1  # stores here the execution time of the last query
        self.db_initialized = False

        self.init_db()

    def init_db(self):
        try:
            self.connection = psycopg2.connect(host=self.__host, port=self.__port, dbname=self.__name, user=self.__user,
                                               password=self.__pwd)
            self.cursor = self.connection.cursor()
            self.db_initialized = True
        except Exception as e:
            self.error("Can't initialize the database!")
            self.error(f"Exception {e.__class__.__name__}: {e}", exception=True)
            self.db_initialized = False
            return self.db_initialized

        return self.db_initialized

    def exec_query(self, query, debug=False):
        """
        Wrapper to query with psycopg2
        :param cursor: DB cursor
        :param query as string
        :returns nothing
        """
        if not self.db_initialized:
            self.warning("Database is not initialized!, trying to open connection...")
            if not self.init_db():
                raise ConnectionError("Database not initialized!")
            self.info("Database initialized!")
        if debug:
            self.debug(f"query: '{query}'")
        init = time.time()
        self.cursor.execute(query)
        self.connection.commit()
        self.query_time = time.time() - init

    def list_from_query(self, query):
        """
        Makes a query to the database using a cursor object and returns a list with the reponse
        :param cursor: database cursor
        :param query: string with the query
        :returns DataFrame with the query result
        """
        self.exec_query(query)
        response = self.cursor.fetchall()
        # colnames = [desc[0] for desc in self.cursor.description]  # Get the Column names
        return self.cursor.description

    def dataframe_from_query(self, query, debug=False):
        """
        Makes a query to the database using a cursor object and returns a DataFrame object
        with the reponse
        :param cursor: database cursor
        :param query: string with the query
        :param debug:
        :returns DataFrame with the query result
        """
        self.exec_query(query, debug=debug)
        response = self.cursor.fetchall()
        column_names = [desc[0] for desc in self.cursor.description]  # Get the Column names
        return pd.DataFrame(response, columns=column_names)

    def close(self):
        self.connection.close()
