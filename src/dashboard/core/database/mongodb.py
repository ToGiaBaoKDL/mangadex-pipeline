# src/database/mongodb.py
import streamlit as st
from pymongo import MongoClient
import logging
from src.dashboard.core.config import mongo_config


@st.cache_resource(ttl=3600)
def get_mongo_collection():
    """Get MongoDB collection with proper connection management."""
    try:
        client = MongoClient(mongo_config.uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_config.database_name]
        collection = db[mongo_config.collection_name]
        summary_collection = db['summary_metrics']
        collection.create_index([("manga_id", 1)])
        collection.create_index([("chapter_id", 1)])
        collection.find_one()  # Test connection
        return collection, summary_collection, client
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {str(e)}")
        logging.error(f"MongoDB connection error: {str(e)}")
        return None, None, None
    