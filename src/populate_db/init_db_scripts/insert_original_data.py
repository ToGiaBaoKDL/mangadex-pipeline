from src.populate_db.init_db_scripts import MangaDataInserter, ChapterDataInserter, ImageDataInserter
from src.populate_db import pg_config, mongo_config
from src.utils import setup_logger
import os


# Setup logging file
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
data_dir = os.path.join(os.path.dirname(project_root), "data")
log_dir = os.path.join(os.path.dirname(project_root), "logs")
log_file = os.path.join(log_dir, "insert_original_db.log")
logger = setup_logger(log_file)


def main():
    """
    Main function to demonstrate data insertion
    """

    # Insert mangas and chapters from CSV
    manga_inserter = MangaDataInserter(pg_config)
    chapter_inserter = ChapterDataInserter(pg_config)

    try:
        manga_inserter.insert_manga_from_csv(f"{data_dir}\\manga_data.csv")
    except Exception as e:
        logger.error(f"❌ Error in inserting manga process: {e}")
    #
    # try:
    #     chapter_inserter.insert_chapters_from_csv(f"{data_dir}\\chapter_data.csv")
    # except Exception as e:
    #     logger.error(f"❌ Error in inserting chapter process: {e}")
    #
    # # Insert images' url from JSON
    # image_inserter = ImageDataInserter(mongo_config)
    #
    # try:
    #     image_inserter.insert_image_data_from_json(f"{data_dir}\\chapter_images.json")
    # except Exception as e:
    #     logger.exception("Error in inserting image process: %s", e)
    # finally:
    #     mongo_config.close_connection()
    #     logger.info("MongoDB connection closed in main")


if __name__ == "__main__":
    logger.info("=" * 50)
    main()
    logger.info("=" * 50)
