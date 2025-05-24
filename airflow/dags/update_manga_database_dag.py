from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
import requests
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, TIMESTAMP, ForeignKey
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
import pymongo
from dotenv import load_dotenv
import sys
import os
import logging
import asyncio
from typing import List, Dict, Any
from src.crawler import MangaDexMangaCrawler, MangaDexChapterCrawler, MangaDexImageCrawler
from src.utils import setup_logger
from src.populate_db import pg_config, mongo_config
from src.populate_db.update_db import (
    update_manga_data_postgres,
    update_chapter_data_postgres,
    update_image_data_mongodb,
    remove_replaced_chapters
)


# DatabaseTransaction class
class DatabaseTransaction:
    def __init__(self, pg_url: str, mongo_uri: str = None, mongo_db: str = None, mongo_collection: str = None):
        self.pg_url = pg_url
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection_name = mongo_collection
        self.rollback_stack = []
        self.session = None
        self.engine = None
        self.mongo_collection = None
        self.metadata = None
        self.manga_table = None
        self.chapter_table = None

    def init_tables(self):
        self.metadata = MetaData()
        self.manga_table = Table(
            "manga_test", self.metadata,
            Column("manga_id", String, primary_key=True),
            Column("title", String(350)),
            Column("alt_title", String(255)),
            Column("status", String(20)),
            Column("published_year", Integer),
            Column("created_at", TIMESTAMP),
            Column("updated_at", TIMESTAMP),
        )
        self.chapter_table = Table(
            "chapter_test", self.metadata,
            Column("chapter_id", String, primary_key=True),
            Column("manga_id", String, ForeignKey("manga.manga_id"), nullable=False),
            Column("chapter_number", String(50)),
            Column("volume", String(50)),
            Column("title", String(255)),
            Column("lang", String(20)),
            Column("pages", Integer),
            Column("created_at", TIMESTAMP)
        )

    def begin(self):
        self.engine = create_engine(self.pg_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.init_tables()
        if self.mongo_uri and self.mongo_db and self.mongo_collection_name:
            client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_collection = client[self.mongo_db][self.mongo_collection_name]
        self.rollback_stack = []
        logger.info("Transaction started")
        return self.session

    def __getstate__(self):
        def serialize_value(value):
            if isinstance(value, (datetime, TIMESTAMP)):
                return value.isoformat() if isinstance(value, datetime) else str(value)
            return value

        serialized_rollback_stack = []
        for operation in self.rollback_stack:
            serialized_op = {}
            for key, val in operation.items():
                if key == 'original_data':
                    serialized_op[key] = {
                        k: {ck: serialize_value(cv) for ck, cv in v.items()}
                        for k, v in val.items()
                    }
                else:
                    serialized_op[key] = val
            serialized_rollback_stack.append(serialized_op)

        state = {
            'pg_url': self.pg_url,
            'mongo_uri': self.mongo_uri,
            'mongo_db': self.mongo_db,
            'mongo_collection_name': self.mongo_collection_name,
            'rollback_stack': serialized_rollback_stack
        }
        return state

    def __setstate__(self, state):
        self.pg_url = state['pg_url']
        self.mongo_uri = state['mongo_uri']
        self.mongo_db = state['mongo_db']
        self.mongo_collection_name = state['mongo_collection_name']
        self.rollback_stack = state['rollback_stack']
        self.engine = None
        self.session = None
        self.mongo_collection = None
        self.metadata = None
        self.manga_table = None
        self.chapter_table = None

    def register_manga_update(self, manga_ids: List[str]):
        if not manga_ids:
            return
        try:
            if self.manga_table is None:
                self.init_tables()
            original_manga_data = {}
            for row in self.session.query(self.manga_table).filter(self.manga_table.c.manga_id.in_(manga_ids)).all():
                manga_dict = {c.name: getattr(row, c.name) for c in self.manga_table.columns}
                original_manga_data[row.manga_id] = manga_dict
            new_manga_ids = [m_id for m_id in manga_ids if m_id not in original_manga_data]
            self.rollback_stack.append({
                'type': 'manga_update',
                'original_data': original_manga_data,
                'new_manga_ids': new_manga_ids
            })
            logger.info(f"Registered manga rollback data for {len(manga_ids)} manga IDs")
        except Exception as e:
            logger.error(f"Failed to register manga update for rollback: {str(e)}")
            raise

    def register_chapter_update(self, updated_chapters: List[str], replaced_chapters: List[str]):
        try:
            if self.chapter_table is None:
                self.init_tables()
            all_chapters = updated_chapters + replaced_chapters
            if not all_chapters:
                return
            original_chapter_data = {}
            for row in self.session.query(self.chapter_table).filter(
                    self.chapter_table.c.chapter_id.in_(all_chapters)).all():
                chapter_dict = {c.name: getattr(row, c.name) for c in self.chapter_table.columns}
                original_chapter_data[row.chapter_id] = chapter_dict
            new_chapter_ids = [c_id for c_id in all_chapters if c_id not in original_chapter_data]
            self.rollback_stack.append({
                'type': 'chapter_update',
                'original_data': original_chapter_data,
                'new_chapter_ids': new_chapter_ids,
                'updated_chapters': updated_chapters,
                'replaced_chapters': replaced_chapters
            })
            logger.info(f"Registered chapter rollback data for {len(all_chapters)} chapter IDs")
        except Exception as e:
            logger.error(f"Failed to register chapter update for rollback: {str(e)}")
            raise

    def register_image_update(self, new_image_chapters: List[str], deleted_chapters: List[str]):
        try:
            if self.mongo_collection is None:
                if self.mongo_uri and self.mongo_db and self.mongo_collection_name:
                    client = pymongo.MongoClient(self.mongo_uri)
                    self.mongo_collection = client[self.mongo_db][self.mongo_collection_name]
                else:
                    raise ValueError("MongoDB configuration missing for image update")
            original_image_data = {}
            if deleted_chapters:
                cursor = self.mongo_collection.find({"chapter_id": {"$in": deleted_chapters}})
                for doc in cursor:
                    original_image_data[doc["chapter_id"]] = doc["images"]
            self.rollback_stack.append({
                'type': 'image_update',
                'new_image_chapters': new_image_chapters,
                'deleted_chapters': deleted_chapters,
                'original_image_data': original_image_data
            })
            logger.info(
                f"Registered image rollback data for {len(new_image_chapters)} new chapters and {len(deleted_chapters)} deleted chapters")
        except Exception as e:
            logger.error(f"Failed to register image update for rollback: {str(e)}")
            raise

    def rollback(self):
        logger.warning("Starting transaction rollback")
        if not self.session and not self.engine:
            self.engine = create_engine(self.pg_url)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            self.init_tables()
        if self.mongo_collection is None and self.mongo_uri and self.mongo_db and self.mongo_collection_name:
            client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_collection = client[self.mongo_db][self.mongo_collection_name]
        while self.rollback_stack:
            operation = self.rollback_stack.pop()
            op_type = operation.get('type')
            try:
                if op_type == 'image_update':
                    logger.info("Rolling back image updates")
                    if operation.get('new_image_chapters'):
                        self.mongo_collection.delete_many({"chapter_id": {"$in": operation['new_image_chapters']}})
                        logger.info(f"Removed {len(operation['new_image_chapters'])} newly added image chapters")
                    if operation.get('original_image_data'):
                        bulk_ops = []
                        for chapter_id, images in operation['original_image_data'].items():
                            bulk_ops.append(pymongo.InsertOne({"chapter_id": chapter_id, "images": images}))
                        if bulk_ops:
                            self.mongo_collection.bulk_write(bulk_ops)
                            logger.info(f"Restored {len(bulk_ops)} deleted image chapters")
                elif op_type == 'chapter_update':
                    logger.info("Rolling back chapter updates")
                    if operation.get('new_chapter_ids'):
                        self.session.execute(
                            self.chapter_table.delete().where(
                                self.chapter_table.c.chapter_id.in_(operation['new_chapter_ids'])
                            )
                        )
                        logger.info(f"Removed {len(operation['new_chapter_ids'])} newly added chapters")
                    original_data = operation.get('original_data', {})
                    for chapter_id, data in original_data.items():
                        if chapter_id not in operation.get('new_chapter_ids', []):
                            self.session.execute(
                                self.chapter_table.update()
                                .where(self.chapter_table.c.chapter_id == chapter_id)
                                .values(**data)
                            )
                    if original_data:
                        logger.info(f"Restored {len(original_data)} original chapter records")
                elif op_type == 'manga_update':
                    logger.info("Rolling back manga updates")
                    if operation.get('new_manga_ids'):
                        self.session.execute(
                            self.manga_table.delete().where(
                                self.manga_table.c.manga_id.in_(operation['new_manga_ids'])
                            )
                        )
                        logger.info(f"Removed {len(operation['new_manga_ids'])} newly added manga")
                    original_data = operation.get('original_data', {})
                    for manga_id, data in original_data.items():
                        self.session.execute(
                            self.manga_table.update()
                            .where(self.manga_table.c.manga_id == manga_id)
                            .values(**data)
                        )
                    if original_data:
                        logger.info(f"Restored {len(original_data)} original manga records")
            except Exception as e:
                logger.error(f"Error during rollback of {op_type}: {str(e)}")
                continue
        try:
            if self.session:
                self.session.commit()
                logger.info("Rollback committed successfully")
        except Exception as e:
            logger.error(f"Failed to commit rollback changes: {str(e)}")
            try:
                self.session.rollback()
            except:
                pass
        finally:
            if self.session:
                self.session.close()
                self.session = None
            if self.engine:
                self.engine.dispose()
                self.engine = None
            self.mongo_collection = None
            self.metadata = None
            self.manga_table = None
            self.chapter_table = None

    def commit(self):
        if self.session:
            try:
                self.session.commit()
                logger.info("Transaction committed successfully")
            except Exception as e:
                logger.error(f"Failed to commit transaction: {str(e)}")
                self.rollback()
                raise
            finally:
                self.session.close()
                self.session = None
                if self.engine:
                    self.engine.dispose()
                    self.engine = None
                self.mongo_collection = None
                self.metadata = None
                self.manga_table = None
                self.chapter_table = None


sys.path.append('/opt/airflow')
load_dotenv()
logger = setup_logger('/opt/airflow/logs/airflow_update_db.log')
logging.getLogger('airflow').setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 10,
}


@dag(
    dag_id='update_manga_database',
    default_args=default_args,
    description='A DAG to update manga, chapter, and image data in PostgreSQL and MongoDB with transaction support',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=['manga', 'database', 'update'],
)
def update_manga_database_dag():
    @task
    def initialize_connections() -> Dict[str, str]:
        try:
            engine = create_engine(pg_config.database_url)
            with engine.connect() as conn:
                logger.info("Successfully connected to PostgreSQL database")
            client = MongoClient(mongo_config.uri)
            client.admin.command('ping')
            db = client[mongo_config.database_name]
            collection = db[mongo_config.collection_name]
            logger.info("Successfully connected to MongoDB")
            return {
                'pg_url': pg_config.database_url,
                'mongo_uri': mongo_config.uri,
                'mongo_db': mongo_config.database_name,
                'mongo_collection': mongo_config.collection_name
            }
        except Exception as e:
            logger.error(f"Failed to initialize connections: {str(e)}")
            raise AirflowException(f"Connection initialization failed: {str(e)}")

    @task
    def fetch_and_process_manga_data() -> List[Dict[str, Any]]:
        try:
            logger.info("Fetching manga data")
            manga_crawler = MangaDexMangaCrawler(is_original=False)
            new_mangas = asyncio.run(manga_crawler.fetch_all_manga())
            new_mangas_processed = manga_crawler.process_manga_data(new_mangas)
            logger.info(f"Processed {len(new_mangas_processed)} manga")
            return new_mangas_processed
        except Exception as e:
            logger.error(f"Failed to fetch/process manga data: {str(e)}")
            raise AirflowException(f"Manga data fetch/process failed: {str(e)}")

    @task
    def fetch_and_process_chapter_data(changed_manga_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        if not changed_manga_ids:
            logger.info("No manga changes detected, skipping chapter fetch")
            return {}
        try:
            logger.info("Fetching chapter data")
            chapter_crawler = MangaDexChapterCrawler(is_original=False)
            new_chapters = asyncio.run(chapter_crawler.fetch_all_chapters(changed_manga_ids))
            new_chapters_processed = {
                manga_id: [chapter_crawler.extract_chapter_info(manga_id, chapter) for chapter in chapter_list]
                for manga_id, chapter_list in new_chapters.items()
            }
            logger.info(f"Processed chapters for {len(new_chapters_processed)} manga")
            return new_chapters_processed
        except Exception as e:
            logger.error(f"Failed to fetch/process chapter data: {str(e)}")
            raise AirflowException(f"Chapter data fetch/process failed: {str(e)}")

    @task
    def fetch_and_process_image_data(updated_or_added_chapters: List[str]) -> Dict[str, List[str]]:
        if not updated_or_added_chapters:
            logger.info("No chapters to fetch images for")
            return {}
        try:
            logger.info("Fetching image data")
            image_crawler = MangaDexImageCrawler(is_original=False)
            new_images = image_crawler.fetch_all_chapter_images(updated_or_added_chapters)
            logger.info(f"Processed images for {len(new_images)} chapters")
            return new_images
        except Exception as e:
            logger.error(f"Failed to fetch/process image data: {str(e)}")
            raise AirflowException(f"Image data fetch/process failed: {str(e)}")

    @task
    def update_all_databases(connections: Dict[str, str], new_mangas: List[Dict[str, Any]],
                             new_chapters: Dict[str, List[Dict[str, Any]]], new_images: Dict[str, List[str]]
                             ) -> Dict[str, Any]:
        try:
            logger.info("Starting all database updates")
            transaction = DatabaseTransaction(
                pg_url=connections['pg_url'],
                mongo_uri=connections['mongo_uri'],
                mongo_db=connections['mongo_db'],
                mongo_collection=connections['mongo_collection']
            )
            transaction.begin()
            engine = create_engine(connections['pg_url'])
            client = MongoClient(connections['mongo_uri'])
            db = client[connections['mongo_db']]
            collection = db[connections['mongo_collection']]

            # Update manga
            changed_manga_ids = update_manga_data_postgres(engine, new_mangas)
            transaction.register_manga_update(changed_manga_ids)
            logger.info(f"Updated {len(changed_manga_ids)} manga")

            # Update chapters
            updated_or_added_chapters = []
            replaced_chapters = []
            if new_chapters:
                updated_or_added_chapters, replaced_chapters = update_chapter_data_postgres(engine, new_chapters)
                transaction.register_chapter_update(updated_or_added_chapters, replaced_chapters)
                logger.info(
                    f"Updated/added {len(updated_or_added_chapters)} chapters, replaced {len(replaced_chapters)}")

            # Update images
            inserted_count = 0
            skipped_count = 0
            deleted_count = 0
            if new_images:
                logger.info("Starting image data update")
                inserted_count, skipped_count = update_image_data_mongodb(new_images, collection)
                logger.info(f"Inserted {inserted_count} chapters, skipped {skipped_count}")
            if replaced_chapters:
                logger.info("Removing replaced chapters")
                deleted_count = remove_replaced_chapters(replaced_chapters, collection)
                logger.info(f"Deleted {deleted_count} replaced chapters")
            if new_images or replaced_chapters:
                transaction.mongo_collection = collection
                transaction.register_image_update(
                    new_image_chapters=list(new_images.keys()) if new_images else [],
                    deleted_chapters=replaced_chapters
                )

            transaction.commit()
            logger.info("All updates committed successfully")

            # Prepare summary statistics
            manga_titles = [manga['title'] for manga in new_mangas][:5]
            chapter_titles = [chapter['title'] for chapters in new_chapters.values() for chapter in chapters][:5]
            return {
                'manga_count': len(changed_manga_ids),
                'chapter_count': len(updated_or_added_chapters),
                'replaced_chapter_count': len(replaced_chapters),
                'image_inserted_count': inserted_count,
                'image_skipped_count': skipped_count,
                'image_deleted_count': deleted_count,
                'manga_titles': manga_titles,
                'chapter_titles': chapter_titles
            }
        except Exception as e:
            logger.error(f"Update failed: {str(e)}")
            transaction.rollback()
            raise AirflowException(f"Database update failed: {str(e)}")

    @task
    def extract_manga_ids(new_mangas: list[dict]) -> list[str]:
        return [manga['manga_id'] for manga in new_mangas]

    @task
    def extract_chapter_ids(new_chapters: dict[str, list[dict]]) -> list[str]:
        return [chapter['chapter_id'] for chapters in new_chapters.values() for chapter in chapters]

    @task
    def send_success_email(**kwargs):
        ti = kwargs['ti']
        stats = ti.xcom_pull(task_ids='update_all_databases')
        execution_date = kwargs['execution_date'].strftime('%Y-%m-%d %H:%M:%S')

        html_content = f"""
        <p><strong>Note:</strong> Ngoc Quynh cutie. Luv you 🐳</p>

        <h3>Manga Database Update DAG - Success Notification</h3>
        <p><strong>Execution Date:</strong> {execution_date}</p>
        <p>The Manga Database Update DAG completed successfully. Below is a summary of the updates:</p>
        <h4>Statistics:</h4>
        <ul>
            <li><strong>Manga Updated/Added:</strong> {stats.get('manga_count', 0)}</li>
            <li><strong>Chapters Updated/Added:</strong> {stats.get('chapter_count', 0)}</li>
            <li><strong>Chapters Replaced:</strong> {stats.get('replaced_chapter_count', 0)}</li>
            <li><strong>Images Inserted:</strong> {stats.get('image_inserted_count', 0)}</li>
            <li><strong>Images Skipped:</strong> {stats.get('image_skipped_count', 0)}</li>
            <li><strong>Images Deleted:</strong> {stats.get('image_deleted_count', 0)}</li>
        </ul>
        <h4>Additional Information:</h4>
        <p>
            - The DAG fetched manga data from MangaDex using the MangaDex[Manga, Chapter, Image]Crawler.<br>
            - Data was stored in PostgreSQL (manga_test and chapter_test tables) and MongoDB (image data).<br>
            - Transaction support ensured data consistency with rollback capabilities in case of failures.<br>
            - Logs are available at /opt/airflow/logs/airflow_update_db.log for detailed information.<br>
        </p>
        """

        # Prepare SendGrid API payload
        url = "https://api.sendgrid.com/v3/mail/send"
        payload = {
            "personalizations": [{"to": [{"email": "baokdl2226@gmail.com"},
                                         # {"email": "quynhpham.31221026734@st.ueh.edu.vn"},
                                         # {"email": "Phamquynh040104@gmail.com"}
                                         ]}],
            "from": {"email": "baokdl2226@gmail.com"},
            "subject": f"Manga Database Update DAG - Success ({execution_date})",
            "content": [{"type": "text/html", "value": html_content}],
        }

        # Get SendGrid API key from Airflow Variables
        sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
        headers = {
            "Authorization": f"Bearer {sendgrid_api_key}",
            "Content-Type": "application/json"
        }

        # Use send_email to wrap the API call
        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code == 202:
                logger.info("Email sent successfully via SendGrid API")
            else:
                logger.error(f"Failed to send email: {response.status_code} - {response.text}")
                raise AirflowException(f"Email sending failed: {response.text}")

        except Exception as e:
            logger.error(f"Error sending email via SendGrid: {str(e)}")
            raise AirflowException(f"Email sending failed: {str(e)}")

    # Define task flow
    connections = initialize_connections()

    new_mangas = fetch_and_process_manga_data()
    manga_ids = extract_manga_ids(new_mangas)

    new_chapters = fetch_and_process_chapter_data(manga_ids)
    chapter_ids = extract_chapter_ids(new_chapters)

    new_images = fetch_and_process_image_data(chapter_ids)

    update_task = update_all_databases(connections, new_mangas, new_chapters, new_images)
    email_task = send_success_email()

    update_task >> email_task


dag = update_manga_database_dag()
