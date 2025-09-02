from pymongo import MongoClient
import pymysql
import logging


class DBConnector:
    """
    A class to manage a single database connection, supporting MySQL and MongoDB.
    """

    def __init__(self, db_type, host, port, user, password, connection_url, database):
        """
        Initialize and establish a single database connection.

        Args:
            db_type (str): The type of the database, either "MySQL" or "MongoDB".
            host (str): The hostname or IP address of the database server.
            port (int): The port number for the database connection.
            user (str): The username for authentication.
            password (str): The password for authentication.
            database (str): The name of the database to connect to.
        """
        self._logger = logging.getLogger(__name__)
        self.db_type = db_type.lower()
        self.connection = self._connect(
            db_type, host, port, user, password, connection_url, database
        )

    @property
    def logger(self):
        """Provides access to the logger instance."""
        return self._logger

    def _connect(self, db_type, host, port, user, password, connection_url, database):
        """
        Internal method to establish the database connection based on the type.

        Returns:
            The database connection object, or None if the connection fails.
        """
        if self.db_type == "mysql":
            return self._connect_mysql(host, port, user, password, database)
        elif self.db_type == "mongodb":
            return self._connect_mongo(connection_url, database)
        else:
            self.logger.error(f"Unsupported database type: {db_type}")
            raise ValueError(f"Unsupported database type: {db_type}")

    def _connect_mysql(self, host, port, user, password, database):
        """Establishes a connection to a MySQL database."""
        try:
            connection = pymysql.connect(
                host=host,
                port=port or 3306,
                user=user,
                password=password,
                database=database,
                cursorclass=pymysql.cursors.DictCursor,
            )
            self.logger.info("Successfully connected to MySQL.")
            return connection
        except pymysql.MySQLError as e:
            self.logger.error(f"MySQL connection error: {e}")
            return None

    def _connect_mongo(self, connection_url, database):
        """Establishes a connection to a MongoDB database."""
        try:
            # Remove trailing / if it exists in the connection_url
            if connection_url.endswith("/"):
                connection_url = connection_url[:-1]
            mongo_uri = f"{connection_url}/{database}"
            client = MongoClient(mongo_uri)
            db = client[database]
            self.logger.info("Successfully connected to MongoDB.")
            return db
        except Exception as e:
            self.logger.error(f"MongoDB connection error: {e}")
            return None

    def execute_query_general(self, query):
        if self.db_type == "mysql":
            return self.execute_query(query)
        elif self.db_type == "mongodb":
            return self.mongo_execute(query)
        else:
            self.logger.error(f"Unsupported database type: {self.db_type}")
            return None

    # ----------------------
    # MySQL operations
    # ----------------------
    def execute_query(self, query, params=None):
        """
        Executes a query on the MySQL database.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): The parameters to substitute into the query.

        Returns:
            A list of dictionaries representing the fetched rows, or None on error.
        """
        if self.db_type != "mysql" or self.connection is None:
            self.logger.error("Not a valid MySQL connection.")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())

                if query.strip().lower().startswith("select"):
                    return cursor.fetchall()

                elif query.strip().lower().startswith("insert"):
                    self.connection.commit()
                    return cursor.lastrowid

                else:  # UPDATE or DELETE
                    self.connection.commit()
                    return cursor.rowcount
        except pymysql.MySQLError as e:
            self.logger.error(f"MySQL query error: {e}")
            return None

    # ----------------------
    # MongoDB operations
    # ----------------------
    def mongo_execute(self, query_str: str):
        """
        Executes arbitrary MongoDB queries in the form of mongosh commands:
            db.collection.find({...})
            db.collection.insert_one({...})
            db.collection.update_many({...})
            db.collection.find({...}).limit(5).sort({"age": -1})
        """
        if self.db_type != "mongodb":
            raise RuntimeError("Not a MongoDB connection")

        try:
            # Replace "db" with the actual db reference
            query_python = query_str.strip().replace("db", "self.connection")

            # Execute the query string
            result = eval(query_python)  # pylint: disable=eval-used

            # Convert cursor to list for find-like queries
            if hasattr(result, "sort") and hasattr(result, "limit"):
                # Cursor case
                return list(result)
            return result
        except Exception as e:
            return f"Error executing query: {e}"
