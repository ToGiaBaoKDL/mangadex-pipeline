from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()


def load_config():
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_host = os.getenv("POSTGRES_HOST")
    pg_database = os.getenv("POSTGRES_DB")

    mongo_db = os.getenv("MONGO_DB")
    mongo_collection = os.getenv("MONGO_COLLECTION")

    return {
        "postgres": {
            "uri": f"postgresql://{pg_user}:{pg_password}@{pg_host}:5432/{pg_database}",
            "ttl": 3600
        },
        "mongodb": {
            "uri": os.getenv("MONGO_URI"),
            "database_name": mongo_db,
            "collection_name": mongo_collection,
            "ttl": 3600
        },
        "app": {
            "cache_ttl": 1800,
            "page_sizes": [10, 25, 50, 100],
            "default_page_size": 50,
            "max_search_results": 25
        }
    }


class PostgresConfig:
    def __init__(self):
        config = load_config()
        self.engine = create_engine(config["postgres"]["uri"])


class MongoConfig:
    def __init__(self):
        config = load_config()
        self.uri = config["mongodb"]["uri"]
        self.database_name = config["mongodb"]["database_name"]
        self.collection_name = config["mongodb"]["collection_name"]


pg_config = PostgresConfig()
mongo_config = MongoConfig()
