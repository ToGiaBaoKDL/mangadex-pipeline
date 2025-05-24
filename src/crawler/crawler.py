from src.crawler import MangaDexMangaCrawler, MangaDexChapterCrawler, MangaDexImageCrawler
import asyncio
import pandas as pd
import os
from src.utils import setup_logger
import sys


# Setup logging file
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
log_dir = os.path.join(project_root, "logs")
log_file = os.path.join(log_dir, "crawler.log")
logger = setup_logger(log_file)


async def crawl_manga():
    manga_crawler = MangaDexMangaCrawler(is_original=True)
    manga_list_raw = await manga_crawler.fetch_all_manga()
    logger.info(f"Fetched {len(manga_list_raw)} manga.")

    manga_list_raw = await manga_crawler.enrich_with_covers(manga_list_raw)
    logger.info(f"Added cover for {len(manga_list_raw)} manga.")

    manga_list_processed = manga_crawler.process_manga_data(manga_list_raw)
    logger.info(f"Processed {len(manga_list_processed)} manga.")

    manga_df = manga_crawler.save_to_csv(manga_list_processed)
    logger.info(f"Manga data saved to CSV with {len(manga_df)} records.")

    return manga_df


async def crawl_chapter(manga_df: pd.DataFrame):
    chapter_crawler = MangaDexChapterCrawler(is_original=True)
    chapters_list_raw = await chapter_crawler.fetch_all_chapters(manga_df["manga_id"].tolist())
    logger.info(f"Fetched chapters for {len(chapters_list_raw)} manga.")

    chapters_list_processed = {
        manga_id: [chapter_crawler.extract_chapter_info(manga_id, chapter)
                   for chapter in chapter_list]
        for manga_id, chapter_list in chapters_list_raw.items()
    }
    logger.info(f"Processed {len(chapters_list_processed)} chapters.")

    chapter_df = chapter_crawler.save_to_csv(chapters_list_processed)
    logger.info(f"Chapter data saved to CSV with {len(chapter_df)} records.")

    return chapter_df


def crawl_image(chapter_df: pd.DataFrame):
    image_crawler = MangaDexImageCrawler(is_original=True)
    chapter_ids = chapter_df["chapter_id"].dropna().astype(str).tolist()
    images = image_crawler.fetch_all_chapter_images(chapter_ids)
    logger.info(f"Fetched image URLs for {len(images)} chapters.")

    image_crawler.save_image_urls(images)
    logger.info(f"Image URLs saved successfully.")


async def main():
    """Entry point for crawler"""
    try:
        logger.info("=" * 50)
        logger.info("Start crawling manga list...")

        # Crawl manga
        manga_df = await crawl_manga()

        logger.info("=" * 20)
        # logger.info("Start crawling chapter list...")
        #
        # # Crawl chapter
        # chapter_df = await crawl_chapter(manga_df)
        #
        # logger.info("=" * 20)
        # logger.info("Start crawling image URLs...")
        #
        # # Crawl image
        # crawl_image(chapter_df)
        #
        # logger.info("=" * 50)
        # logger.info("Crawling completed successfully.")

    except Exception as e:
        logger.error("=" * 50)
        logger.error(f"Crawling failed: {e}", exc_info=True)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
