import psycopg2.extras
from sqlalchemy import create_engine
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()


class PostgresConfig:
    """
    Configuration and connection management for PostgresSQL database
    """

    def __init__(self,
                 user: str,
                 password: str,
                 host: str,
                 port: str = "5432",
                 database: str = ""):
        """
        Initialize database connection parameters

        :param user: Database username
        :param password: Database password
        :param host: Database host
        :param port: Database port (default: 5432)
        :param database: Database name
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

        # Create SQLAlchemy engine for potential future use
        self.database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.database_url)

    def get_connection(self):
        """
        Establish a database connection

        :return: psycopg2 connection object
        """
        return psycopg2.connect(
            dbname=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )


class MongoDBConfig:
    """
    Configuration and connection management for MongoDB
    """

    def __init__(self,
                 uri: str,
                 database_name: str,
                 collection_name: str):
        """
        Initialize MongoDB connection parameters

        :param uri: MongoDB's connection URI
        :param database_name: Name of the database
        :param collection_name: Name of the collection
        """
        self.uri = uri
        self.database_name = database_name
        self.collection_name = collection_name

        # Establish connection
        self.client = MongoClient(self.uri)
        self.db = self.client[self.database_name]
        self.collection = self.db[self.collection_name]

    def get_collection(self):
        """
        Get the specified MongoDB collection

        :return: pymongo collection object
        """
        return self.collection

    def close_connection(self):
        """
        Close the MongoDB connection
        """
        self.client.close()


pg_config = PostgresConfig(
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    database=os.getenv("POSTGRES_DB"),
)

mongo_config = MongoDBConfig(
    uri=os.getenv("MONGO_URI"),
    database_name=os.getenv("MONGO_DB"),
    collection_name=os.getenv("MONGO_COLLECTION"),
)
