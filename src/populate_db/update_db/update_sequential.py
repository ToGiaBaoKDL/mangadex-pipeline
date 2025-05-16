from sqlalchemy import create_engine, Table, Column, String, Integer, TIMESTAMP, MetaData, ForeignKey
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from src.crawler import MangaDexMangaCrawler, MangaDexChapterCrawler, MangaDexImageCrawler
from datetime import datetime
from typing import List, Dict, Tuple, Any
from tqdm import tqdm
import pymongo
from pymongo.errors import BulkWriteError, PyMongoError
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils import setup_logger
from src.populate_db import pg_config, mongo_config
import os
import asyncio
import traceback

# Setup logging file
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
data_dir = os.path.join(os.path.dirname(project_root), "data")
log_dir = os.path.join(os.path.dirname(project_root), "logs")
log_file = os.path.join(log_dir, "update_db.log")
logger = setup_logger(log_file)


def update_manga_data_postgres(
        engine: Any,
        new_mangas: List[Dict[str, Any]]
) -> List[str]:
    """
    Update manga information in PostgresSQL database using SQLAlchemy.

    Args:
        engine (sqlalchemy.engine.Engine): PostgresSQL connection.
        new_mangas (List[Dict[str, Any]]): List of processed manga to update/add.

    Returns:
        List[str]: List of manga IDs that were updated or added.

    Raises:
        SQLAlchemyError: If there's an issue with the database operations
    """
    try:
        metadata = MetaData()
        manga_table = Table(
            "manga_test", metadata,
            Column("manga_id", String, primary_key=True),
            Column("title", String(350)),
            Column("alt_title", String(255)),
            Column("status", String(20)),
            Column("published_year", Integer),
            Column("created_at", TIMESTAMP),
            Column("updated_at", TIMESTAMP),
        )

        Session = sessionmaker(bind=engine)

        # Process manga list
        logger.info(f"Checking {len(new_mangas)} manga for updates")

        with Session() as session:
            try:
                # Get existing manga data as a dictionary for quick lookups
                existing_manga = {
                    str(row.manga_id): {"status": row.status, "updated_at": row.updated_at}
                    for row in
                    session.query(manga_table.c.manga_id, manga_table.c.status, manga_table.c.updated_at).all()
                }

                updated_count, added_count = 0, 0
                changed_manga_ids = []

                # Process each manga
                for new_manga in new_mangas:
                    manga_id = new_manga["manga_id"]
                    updated_at = datetime.fromisoformat(new_manga["updated_at"]).replace(tzinfo=None)

                    if manga_id in existing_manga:
                        # Manga exists - check if we need to update
                        db_manga = existing_manga[manga_id]

                        if db_manga["status"] != new_manga["status"] or db_manga["updated_at"] < updated_at:
                            # Log changes for debugging
                            changes = []
                            if db_manga["status"] != new_manga["status"]:
                                changes.append(f"status: '{db_manga['status']}' → '{new_manga['status']}'")
                            if db_manga["updated_at"] < updated_at:
                                changes.append(f"updated_at: '{db_manga['updated_at']}' → '{updated_at}'")

                            # Update manga
                            stmt = (
                                manga_table.update()
                                .where(manga_table.c.manga_id == manga_id)
                                .values(status=new_manga["status"], updated_at=updated_at)
                            )
                            session.execute(stmt)
                            changed_manga_ids.append(manga_id)
                            updated_count += 1
                            logger.info(f"UPDATED: {new_manga['title']} (ID: {manga_id}) | {', '.join(changes)}")
                    else:
                        # Manga doesn't exist - add it
                        stmt = insert(manga_table).values(
                            manga_id=manga_id,
                            title=new_manga["title"],
                            alt_title=new_manga["alt_title"],
                            status=new_manga["status"],
                            published_year=new_manga["year"],
                            created_at=new_manga["created_at"],
                            updated_at=updated_at
                        ).on_conflict_do_nothing(index_elements=["manga_id"])

                        session.execute(stmt)
                        changed_manga_ids.append(manga_id)
                        added_count += 1
                        logger.info(f"ADDED: {new_manga['title']} (ID: {manga_id})")

                # Commit all changes
                session.commit()
                logger.info("Database update complete")
                logger.info(f"SUMMARY: {updated_count} manga updated, {added_count} manga added")

                return changed_manga_ids

            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Database error during manga update: {str(e)}")
                logger.error(traceback.format_exc())
                raise

    except Exception as e:
        logger.error(f"Unexpected error in update_manga_data_postgres: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def update_chapter_data_postgres(
        engine: Any,
        new_chapters: Dict[str, List[Dict[str, Any]]]
) -> Tuple[List[str], List[str]]:
    """
    Update chapter information in PostgresSQL database:
    - Add new chapters if they don't exist
    - Replace non-English chapters with English versions when available
    - Only return chapter_ids with pages > 0
    - Track replaced chapter_ids

    Args:
        engine (sqlalchemy.engine.Engine): PostgresSQL connection
        new_chapters (Dict[str, List[Dict[str, Any]]]): Dictionary mapping manga_id to list of processed chapter data

    Returns:
        Tuple[List[str], List[str]]:
            - updated_or_added_chapters: List of chapter_ids added or updated with pages > 0
            - replaced_chapters: List of chapter_ids that were replaced by English translations

    Raises:
        SQLAlchemyError: If there's an issue with the database operations
    """
    try:
        metadata = MetaData()
        chapter_table = Table(
            "chapter_test", metadata,
            Column("chapter_id", String, primary_key=True),
            Column("manga_id", String, ForeignKey("manga.manga_id"), nullable=False),
            Column("chapter_number", String(50)),
            Column("volume", String(50)),
            Column("title", String(255)),
            Column("lang", String(20)),
            Column("pages", Integer),
            Column("created_at", TIMESTAMP)
        )

        Session = sessionmaker(bind=engine)

        logger.info(
            f"Checking {sum(len(chapters) for chapters in new_chapters.values())} "
            f"chapters across {len(new_chapters)} manga")

        updated_or_added_chapters = []
        replaced_chapters = []
        changes_made = False

        with Session() as session:
            try:
                # Get existing chapters as a lookup dictionary for efficient querying
                existing_chapters = {
                    (str(row.manga_id), row.chapter_number): (row.lang, str(row.chapter_id))
                    for row in session.query(
                        chapter_table.c.manga_id,
                        chapter_table.c.chapter_number,
                        chapter_table.c.lang,
                        chapter_table.c.chapter_id
                    ).all()
                }

                # Process chapters by manga
                for manga_id, chapters in new_chapters.items():
                    for chapter_data in chapters:
                        try:
                            # Convert UUIDs to strings for PostgresSQL compatibility
                            chapter_data["chapter_id"] = str(chapter_data["chapter_id"])
                            chapter_data["manga_id"] = str(chapter_data["manga_id"])

                            key = (chapter_data["manga_id"], chapter_data["chapter_number"])
                            lang = chapter_data["lang"]
                            chapter_id = chapter_data["chapter_id"]
                            pages = chapter_data["pages"]

                            logger.info(
                                f"Processing manga {manga_id}, chapter {chapter_data['chapter_number']} ({lang})")

                            # Check if chapter exists
                            if key in existing_chapters:
                                existing_lang, existing_chap_id = existing_chapters[key]

                                # Replace non-English chapters with English versions
                                if existing_lang != "en" and lang == "en":
                                    logger.info(
                                        f"Replacing chapter {chapter_data['chapter_number']} "
                                        f"({existing_lang} → en) for manga {manga_id}")

                                    stmt = (
                                        chapter_table.update()
                                        .where(chapter_table.c.chapter_id == existing_chap_id)
                                        .values(**chapter_data)
                                    )
                                    session.execute(stmt)

                                    replaced_chapters.append(existing_chap_id)

                                    # Only track chapters with pages
                                    if pages and pages > 0:
                                        updated_or_added_chapters.append(chapter_id)

                                    changes_made = True
                                else:
                                    logger.info(
                                        f"Chapter {chapter_data['chapter_number']} "
                                        f"({lang}) already exists, no change needed")

                                continue

                            # Add new chapter
                            logger.info(
                                f"Adding new chapter {chapter_data['chapter_number']} ({lang}) for manga {manga_id}")

                            stmt = insert(chapter_table).values(**chapter_data).on_conflict_do_nothing(
                                index_elements=["chapter_id"])
                            session.execute(stmt)

                            # Only track chapters with pages
                            if pages and pages > 0:
                                updated_or_added_chapters.append(chapter_id)

                            changes_made = True

                        except Exception as e:
                            logger.error(f"Error processing chapter for manga {manga_id}: {str(e)}")
                            logger.error(traceback.format_exc())
                            # Continue with other chapters

                # Commit changes if any were made
                if changes_made:
                    session.commit()
                    logger.info("Database update complete")
                else:
                    logger.info("No changes needed")

                logger.info(f"Total chapters added/updated: {len(updated_or_added_chapters)}")
                logger.info(f"Total chapters replaced with English translations: {len(replaced_chapters)}")

                return updated_or_added_chapters, replaced_chapters

            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Database error during chapter update: {str(e)}")
                logger.error(traceback.format_exc())
                raise

    except Exception as e:
        logger.error(f"Unexpected error in update_chapter_data_postgres: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def update_image_data_mongodb(
        chapter_images: Dict[str, List[str]],
        collection: pymongo.collection.Collection,
        BATCH_SIZE: int = 10_000
) -> Tuple[int, int]:
    """
    Efficiently update MongoDB by inserting only new chapter_ids and skipping duplicates.

    Args:
        chapter_images (Dict[str, List[str]]): Dictionary containing chapter_id as key and list of image URLs as value
        collection (pymongo.collection.Collection): MongoDB collection to update
        BATCH_SIZE (int): Number of documents per bulk insert batch

    Returns:
        Tuple[int, int]: (new_chapters, skipped_chapters) Count of newly inserted and skipped (duplicate) chapters

    Raises:
        PyMongoError: If there's an issue with MongoDB operations
    """
    try:
        if not chapter_images:
            logger.info("No data to update.")
            return 0, 0

        logger.info("Checking for existing chapters...")
        chapter_ids = list(chapter_images.keys())

        try:
            # Fetch existing chapter_ids from MongoDB
            existing_chapters = set(
                doc["chapter_id"] for doc in collection.find(
                    {"chapter_id": {"$in": chapter_ids}},
                    {"chapter_id": 1, "_id": 0}
                )
            )
        except PyMongoError as e:
            logger.error(f"Failed to fetch existing chapters from MongoDB: {str(e)}")
            logger.error(traceback.format_exc())
            raise

        bulk_operations = []
        new_chapters = 0
        skipped_chapters = 0

        # Only insert chapters that do not already exist in the database
        for chapter_id, images in tqdm(chapter_images.items(), desc="Preparing data", ncols=100):
            if chapter_id in existing_chapters:
                skipped_chapters += 1
                logger.debug(f"Skipping duplicate chapter: {chapter_id}")
            else:
                new_chapters += 1
                bulk_operations.append(
                    pymongo.InsertOne({"chapter_id": chapter_id, "images": images})
                )

        if not bulk_operations:
            logger.info(f"No new chapters to insert. Duplicates skipped: {skipped_chapters}")
            return 0, skipped_chapters

        logger.info(f"Starting MongoDB bulk update with {new_chapters} new chapters...")

        def bulk_write_batch(batch: List[pymongo.InsertOne]) -> None:
            """Write a single batch to MongoDB."""
            if batch:
                try:
                    collection.bulk_write(batch, ordered=False)
                except BulkWriteError as bwe:
                    # Log the error but don't stop the process
                    successful_ops = len(batch) - len(bwe.details.get('writeErrors', []))
                    logger.warning(f"Bulk write partially failed: {successful_ops}/{len(batch)} operations succeeded")
                    logger.warning(
                        f"First error: {bwe.details.get('writeErrors', [{}])[0].get('errmsg', 'Unknown error')}")
                except PyMongoError as e:
                    logger.error(f"Failed to write batch to MongoDB: {str(e)}")
                    logger.error(traceback.format_exc())
                    raise

        # Split operations into batches and write in parallel using ThreadPool
        error_count = 0
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(0, len(bulk_operations), BATCH_SIZE):
                batch = bulk_operations[i:i + BATCH_SIZE]
                futures.append(executor.submit(bulk_write_batch, batch))

            # Track progress with tqdm
            with tqdm(total=len(bulk_operations), desc="Updating MongoDB", ncols=100) as progress:
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error in batch processing: {str(e)}")
                    finally:
                        progress.update(min(BATCH_SIZE, len(bulk_operations) - progress.n))

        if error_count > 0:
            logger.warning(f"{error_count} batches encountered errors during processing")

        logger.info(f"MongoDB update completed: Inserted: {new_chapters} | Duplicates skipped: {skipped_chapters}")
        return new_chapters, skipped_chapters

    except Exception as e:
        logger.error(f"Unexpected error in update_image_data_mongodb: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def remove_replaced_chapters(
        chapter_ids: List[str],
        collection: pymongo.collection.Collection
) -> int:
    """
    Remove chapters that have been replaced from MongoDB.

    Args:
        chapter_ids (List[str]): List of chapter_ids to be deleted
        collection (pymongo.collection.Collection): MongoDB collection to operate on

    Returns:
        int: Number of documents deleted

    Raises:
        PyMongoError: If there's an issue with MongoDB operations
    """
    try:
        if not chapter_ids:
            logger.info("No chapters to delete.")
            return 0

        logger.info(f"Preparing to delete {len(chapter_ids)} replaced chapters...")

        try:
            result = collection.delete_many({"chapter_id": {"$in": chapter_ids}})
            deleted_count = result.deleted_count

            logger.info(f"Deleted {deleted_count} replaced chapters")
            return deleted_count

        except PyMongoError as e:
            logger.error(f"Failed to delete replaced chapters: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    except Exception as e:
        logger.error(f"Unexpected error in remove_replaced_chapters: {str(e)}")
        logger.error(traceback.format_exc())
        raise


class DatabaseTransaction:
    """
    A transaction manager for handling multi-database operations with rollback capability.
    Keeps track of operations performed, so they can be rolled back in reverse order.
    """

    def __init__(self, engine, mongo_collection):
        self.engine = engine
        self.mongo_collection = mongo_collection
        self.rollback_stack = []
        self.session = None
        self.metadata = MetaData()
        self.manga_table = None
        self.chapter_table = None

    def init_tables(self):
        """Initialize database table references"""
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
        """Begin a new transaction"""
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.init_tables()
        self.rollback_stack = []
        logger.info("Transaction started")
        return self.session

    def register_manga_update(self, manga_ids: List[str]):
        """Register manga updates for potential rollback"""
        # Get original manga data before our changes
        if not manga_ids:
            return

        try:
            # Store original data for potential rollback
            original_manga_data = {}
            for row in self.session.query(self.manga_table).filter(self.manga_table.c.manga_id.in_(manga_ids)).all():
                manga_dict = {c.name: getattr(row, c.name) for c in self.manga_table.columns}
                original_manga_data[row.manga_id] = manga_dict

            # Get newly inserted manga IDs (those not in original_manga_data)
            new_manga_ids = [m_id for m_id in manga_ids if m_id not in original_manga_data]

            # Register rollback operation
            self.rollback_stack.append({
                'type': 'manga_update',
                'original_data': original_manga_data,
                'new_manga_ids': new_manga_ids
            })
            logger.info(f"Registered manga rollback data for {len(manga_ids)} manga IDs")
        except Exception as e:
            logger.error(f"Failed to register manga update for rollback: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def register_chapter_update(self, updated_chapters: List[str], replaced_chapters: List[str]):
        """Register chapter updates for potential rollback"""
        try:
            # Get original chapter data
            all_chapters = updated_chapters + replaced_chapters
            if not all_chapters:
                return

            original_chapter_data = {}
            for row in self.session.query(self.chapter_table).filter(
                    self.chapter_table.c.chapter_id.in_(all_chapters)).all():
                chapter_dict = {c.name: getattr(row, c.name) for c in self.chapter_table.columns}
                original_chapter_data[row.chapter_id] = chapter_dict

            # Get newly inserted chapter IDs (those not in original_chapter_data)
            new_chapter_ids = [c_id for c_id in all_chapters if c_id not in original_chapter_data]

            # Register rollback operation
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
            logger.error(traceback.format_exc())
            raise

    def register_image_update(self, new_image_chapters: List[str], deleted_chapters: List[str]):
        """Register MongoDB image updates for potential rollback"""
        try:
            # Store original image data for potential rollback
            original_image_data = {}

            # Get data for chapters that might be deleted
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
            logger.error(traceback.format_exc())
            raise

    def rollback(self):
        """Roll back all operations in reverse order"""
        logger.warning("Starting transaction rollback")

        if not self.session:
            logger.warning("No active session to rollback")
            return

        # Rollback operations in reverse order (last in, first out)
        while self.rollback_stack:
            operation = self.rollback_stack.pop()
            op_type = operation.get('type')

            try:
                if op_type == 'image_update':
                    logger.info("Rolling back image updates")

                    # Delete newly added chapters
                    if operation.get('new_image_chapters'):
                        self.mongo_collection.delete_many({"chapter_id": {"$in": operation['new_image_chapters']}})
                        logger.info(f"Removed {len(operation['new_image_chapters'])} newly added image chapters")

                    # Restore deleted chapters
                    if operation.get('original_image_data'):
                        bulk_ops = []
                        for chapter_id, images in operation['original_image_data'].items():
                            bulk_ops.append(pymongo.InsertOne({"chapter_id": chapter_id, "images": images}))

                        if bulk_ops:
                            self.mongo_collection.bulk_write(bulk_ops)
                            logger.info(f"Restored {len(bulk_ops)} deleted image chapters")

                elif op_type == 'chapter_update':
                    logger.info("Rolling back chapter updates")

                    # Delete newly added chapters
                    if operation.get('new_chapter_ids'):
                        self.session.execute(
                            self.chapter_table.delete().where(
                                self.chapter_table.c.chapter_id.in_(operation['new_chapter_ids'])
                            )
                        )
                        logger.info(f"Removed {len(operation['new_chapter_ids'])} newly added chapters")

                    # Restore original data for updated chapters
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

                    # Delete newly added manga
                    if operation.get('new_manga_ids'):
                        self.session.execute(
                            self.manga_table.delete().where(
                                self.manga_table.c.manga_id.in_(operation['new_manga_ids'])
                            )
                        )
                        logger.info(f"Removed {len(operation['new_manga_ids'])} newly added manga")

                    # Restore original data for updated manga
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
                logger.error(traceback.format_exc())
                # Continue with other rollbacks regardless of errors

        try:
            # Commit the rollbacks to PostgresSQL
            self.session.commit()
            logger.info("Rollback committed successfully")
        except Exception as e:
            logger.error(f"Failed to commit rollback changes: {str(e)}")
            logger.error(traceback.format_exc())
            try:
                self.session.rollback()
            except:
                pass
        finally:
            self.session.close()
            self.session = None

    def commit(self):
        """Commit the transaction"""
        if self.session:
            try:
                self.session.commit()
                logger.info("Transaction committed successfully")
            except Exception as e:
                logger.error(f"Failed to commit transaction: {str(e)}")
                logger.error(traceback.format_exc())
                self.rollback()
                raise
            finally:
                self.session.close()
                self.session = None


async def main():
    """
    Main function to update manga, chapter, and image data with transaction support.

    Implements a complete transaction system that can roll back all changes if an error
    occurs at any point in the update process. The rollback will revert:
    1. Image data in MongoDB
    2. Chapter data in PostgresSQL
    3. Manga data in PostgresSQL

    Everything is wrapped in a single logical transaction that rolls back if any component fails.
    """
    try:
        logger.info("Starting database update process with transaction support")

        try:
            engine = create_engine(pg_config.database_url)
            logger.info("Successfully connected to PostgresSQL database")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to PostgresSQL: {str(e)}")
            logger.error(traceback.format_exc())
            return

        # Attempt to connect to MongoDB and validate connection
        try:
            client = pymongo.MongoClient(mongo_config.uri)
            client.admin.command('ping')  # Simple command to validate connection
            db = client[mongo_config.database_name]
            collection = db[mongo_config.collection_name]
            logger.info("Successfully connected to MongoDB")
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            logger.error(traceback.format_exc())
            return

        # Create transaction manager
        transaction = DatabaseTransaction(engine, collection)

        try:
            # Begin transaction
            transaction.begin()

            # Update manga database
            logger.info("Starting manga data update")
            manga_crawler = MangaDexMangaCrawler(is_original=False)
            new_mangas = await manga_crawler.fetch_all_manga()
            new_mangas_processed = manga_crawler.process_manga_data(new_mangas)

            # Perform manga updates using direct session access
            changed_manga_ids = []
            try:
                changed_manga_ids = update_manga_data_postgres(engine, new_mangas_processed)
                # Register for potential rollback
                transaction.register_manga_update(changed_manga_ids)
                logger.info(f"Successfully updated manga data, {len(changed_manga_ids)} manga changed")
            except Exception as e:
                logger.error(f"Failed to update manga data: {str(e)}")
                logger.error(traceback.format_exc())
                transaction.rollback()
                return

            # Update chapter database
            updated_or_added_chapters = []
            replaced_chapters = []
            if changed_manga_ids:
                logger.info("Starting chapter data update")
                try:
                    chapter_crawler = MangaDexChapterCrawler(is_original=False)
                    new_chapters = await chapter_crawler.fetch_all_chapters(changed_manga_ids)
                    new_chapters_processed = {
                        manga_id: [chapter_crawler.extract_chapter_info(manga_id, chapter)
                                   for chapter in chapter_list]
                        for manga_id, chapter_list in new_chapters.items()
                    }
                    updated_or_added_chapters, replaced_chapters = update_chapter_data_postgres(engine,
                                                                                                new_chapters_processed)
                    # Register for potential rollback
                    transaction.register_chapter_update(updated_or_added_chapters, replaced_chapters)
                    logger.info(
                        f"Successfully updated chapter data, {len(updated_or_added_chapters)} chapters updated/added")
                except Exception as e:
                    logger.error(f"Failed to update chapter data: {str(e)}")
                    logger.error(traceback.format_exc())
                    transaction.rollback()
                    return

                # Update image database
                inserted_count = 0
                deleted_count = 0

                try:
                    if updated_or_added_chapters:
                        logger.info("Starting image data update")
                        image_crawler = MangaDexImageCrawler(is_original=False)
                        new_images = image_crawler.fetch_all_chapter_images(updated_or_added_chapters)
                        inserted_count, skipped_count = update_image_data_mongodb(new_images, collection)
                        logger.info(
                            f"Successfully updated image data, {inserted_count} chapters inserted, {skipped_count} skipped")

                    if replaced_chapters:
                        logger.info("Removing replaced chapters from image database")
                        deleted_count = remove_replaced_chapters(replaced_chapters, collection)
                        logger.info(f"Successfully removed {deleted_count} replaced chapters")

                    # Register MongoDB operations for potential rollback
                    transaction.register_image_update(
                        new_image_chapters=list(new_images.keys()) if 'new_images' in locals() else [],
                        deleted_chapters=replaced_chapters
                    )
                except Exception as e:
                    logger.error(f"Failed to update image data: {str(e)}")
                    logger.error(traceback.format_exc())
                    transaction.rollback()
                    return
            else:
                logger.info("No manga changes detected, skipping chapter and image updates")

            # Commit the transaction
            transaction.commit()
            logger.info("Database update process completed successfully")

        except Exception as e:
            logger.error(f"Error during database update process: {str(e)}")
            logger.error(traceback.format_exc())
            # Rollback all changes
            transaction.rollback()
            logger.warning("All changes have been rolled back due to errors")

    except Exception as e:
        logger.error(f"Critical error in main function: {str(e)}")
        logger.error(traceback.format_exc())


if __name__ == '__main__':
    try:
        # Create log directory if it doesn't exist
        logger.info("=" * 50)
        logger.info("Starting database update script")

        asyncio.run(main())

        logger.info("Database update script completed")
        logger.info("=" * 50)
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        logger.critical(traceback.format_exc())
