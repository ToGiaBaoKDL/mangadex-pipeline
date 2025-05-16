import psycopg2.extras
import csv
import os
import json
from typing import Dict, Any, List, Tuple
import pymongo
from tqdm import tqdm
from src.populate_db import MongoDBConfig, PostgresConfig
from src.utils import setup_logger


# Setup logging file
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
data_dir = os.path.join(os.path.dirname(project_root), "data")
log_dir = os.path.join(os.path.dirname(project_root), "logs")
log_file = os.path.join(log_dir, "insert_original_db.log")
logger = setup_logger(log_file)


def validate_file_and_count_LOC(csv_file):
    # Validate CSV file
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    # Determine total number of lines
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        total_lines = sum(1 for _ in reader)

    logger.info(f"📂 Reading data from {csv_file} ({total_lines} rows)...")
    return total_lines


class MangaDataInserter:
    """
    Handles insertion of manga data into PostgresSQL database
    """

    def __init__(self, db_config: PostgresConfig):
        """
        Initialize the inserter with database configuration

        :param db_config: PostgresConfig instance
        """
        self.db_config = db_config

    def insert_manga_from_csv(self,
                              csv_file: str,
                              batch_size: int = 10000) -> None:
        """
        Insert manga data from CSV file into PostgreSQL database

        :param csv_file: Path to the CSV file
        :param batch_size: Number of rows to insert in each batch
        """
        # Validate CSV file
        total_lines = validate_file_and_count_LOC(csv_file)

        # Prepare SQL query
        query = """
        INSERT INTO manga_test (manga_id, title, alt_title, status, published_year, created_at, updated_at)
        VALUES %s
        ON CONFLICT (manga_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            alt_title = EXCLUDED.alt_title,
            status = EXCLUDED.status,
            published_year = EXCLUDED.published_year,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at;
        """

        # Database insertion process
        with self.db_config.get_connection() as conn:
            with conn.cursor() as cursor:
                with open(csv_file, mode="r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    batch: List[Tuple] = []

                    with tqdm(total=total_lines, desc="⏳ Inserting manga", unit=" rows") as pbar:
                        for row in reader:
                            try:
                                batch.append((
                                    row["manga_id"],
                                    row["title"].strip(),
                                    row["alt_title"].strip(),
                                    row["status"].strip(),
                                    int(float(row["year"])) if row["year"] is not None and row["year"] != "" else None,
                                    row["created_at"],
                                    row["updated_at"]
                                ))

                                # Insert batch when size is reached
                                if len(batch) >= batch_size:
                                    psycopg2.extras.execute_values(cursor, query, batch)
                                    conn.commit()
                                    pbar.update(len(batch))
                                    logger.info(f"✅ Inserted {len(batch)} manga into database.")
                                    batch = []

                            except Exception as e:
                                logger.error(f"❌ Error processing row: {e}")

                        # Insert remaining batch
                        if batch:
                            psycopg2.extras.execute_values(cursor, query, batch)
                            conn.commit()
                            pbar.update(len(batch))
                            logger.info(f"✅ Inserted final {len(batch)} mangas.")

                        logger.info(f"🎉 Completed! Total {total_lines} manga processed.")


class ChapterDataInserter:
    """
    Handles insertion of chapter data into PostgreSQL database
    """

    def __init__(self, db_config: PostgresConfig):
        """
        Initialize the chapter inserter with database configuration

        :param db_config: DatabaseConfig instance
        """
        self.db_config = db_config

    def insert_chapters_from_csv(self,
                                 csv_file: str,
                                 batch_size: int = 60000) -> None:
        """
        Insert chapter data from CSV file into PostgreSQL database

        :param csv_file: Path to the CSV file
        :param batch_size: Number of rows to insert in each batch
        """
        # Validate CSV file
        total_lines = validate_file_and_count_LOC(csv_file)

        # Prepare SQL query
        query = """
        INSERT INTO chapter_test (chapter_id, manga_id, chapter_number, volume, title, lang, pages, created_at)
        VALUES %s
        ON CONFLICT (chapter_id)
        DO UPDATE SET
            manga_id = EXCLUDED.manga_id,
            chapter_number = EXCLUDED.chapter_number,
            volume = EXCLUDED.volume,
            title = EXCLUDED.title,
            lang = EXCLUDED.lang,
            pages = EXCLUDED.pages,
            created_at = EXCLUDED.created_at
        """

        # Database insertion process
        with self.db_config.get_connection() as conn:
            with conn.cursor() as cursor:
                with open(csv_file, mode="r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    batch: List[Tuple] = []

                    with tqdm(total=total_lines, desc="⏳ Inserting chapters", unit=" rows") as pbar:
                        for row in reader:
                            try:
                                batch.append((
                                    row["chapter_id"],
                                    row["manga_id"],
                                    row["chapter_number"],
                                    row["volume"],
                                    row["title"],
                                    row["lang"],
                                    int(row["pages"]) if row["pages"].isdigit() else None,
                                    row["created_at"]
                                ))

                                # Insert batch when size is reached
                                if len(batch) >= batch_size:
                                    psycopg2.extras.execute_values(cursor, query, batch)
                                    conn.commit()
                                    pbar.update(len(batch))
                                    logger.info(f"✅ Inserted {len(batch)} chapters into database.")
                                    batch = []

                            except Exception as e:
                                logger.error(f"❌ Error processing chapter row: {e}")

                        # Insert remaining batch
                        if batch:
                            psycopg2.extras.execute_values(cursor, query, batch)
                            conn.commit()
                            pbar.update(len(batch))
                            logger.info(f"✅ Inserted final {len(batch)} chapters.")

                        logger.info(f"🎉 Completed! Total {total_lines} chapters processed.")


class ImageDataInserter:
    """
    Handles insertion of image data into MongoDB
    """

    def __init__(self, mongo_config: MongoDBConfig):
        self.mongo_config = mongo_config
        self.collection = mongo_config.get_collection()
        logger.info("Initialized ImageDataInserter with collection: %s", self.collection.full_name)

    @staticmethod
    def _validate_image_data(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        documents = []
        for chapter_id, image_urls in raw_data.items():
            if not chapter_id or not image_urls:
                logger.warning("Skipped invalid entry with chapter_id=%s", chapter_id)
                continue

            documents.append({
                "chapter_id": str(chapter_id),
                "images": image_urls
            })

        logger.info("Validated %d chapters for insertion", len(documents))
        return documents

    def insert_image_data_from_json(self,
                                    json_file: str,
                                    batch_size: int = 5000,
                                    upsert: bool = True) -> None:
        if not os.path.exists(json_file):
            logger.error("JSON file not found: %s", json_file)
            raise FileNotFoundError(f"JSON file not found: {json_file}")

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            logger.info("Loaded JSON data from %s", json_file)
        except json.JSONDecodeError as e:
            logger.error("Error decoding JSON file %s: %s", json_file, e)
            return
        except Exception as e:
            logger.error("Unexpected error reading file %s: %s", json_file, e)
            return

        documents = self._validate_image_data(raw_data)

        if not documents:
            logger.warning("No valid data to insert from file: %s", json_file)
            return

        logger.info("Starting insertion of %d chapters", len(documents))

        try:
            for i in tqdm(range(0, len(documents), batch_size),
                          desc="⏳ Inserting Images",
                          ncols=100):
                batch = documents[i:i + batch_size]
                bulk_operations = []

                for doc in batch:
                    if upsert:
                        bulk_operations.append(
                            pymongo.UpdateOne(
                                {"chapter_id": doc["chapter_id"]},
                                {"$set": doc},
                                upsert=True
                            )
                        )
                    else:
                        bulk_operations.append(
                            pymongo.InsertOne(doc)
                        )

                if bulk_operations:
                    result = self.collection.bulk_write(bulk_operations, ordered=False)

                    if upsert:
                        logger.info("Batch upserted: %d inserted, %d modified",
                                    len(result.upserted_ids), result.modified_count)
                    else:
                        logger.info("Batch inserted: %d", result.inserted_count)

            logger.info("🎉 Successfully inserted %d chapters from %s", len(documents), json_file)

        except Exception as e:
            logger.exception("Error during bulk insert: %s", e)

    def __del__(self):
        self.mongo_config.close_connection()
        logger.info("Closed MongoDB connection")
