import asyncio
import aiohttp
import pandas as pd
from src.utils import setup_logger
from typing import List, Dict, Tuple, Union, Any
from datetime import datetime, timedelta
import random
import os
import csv
import json
from tqdm import tqdm
import time
import requests
from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor, as_completed


class MangaDexMangaCrawler:
    def __init__(self, is_original=True):
        """
        Initialize MangaDex Crawler
        """
        self.is_original = is_original

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.output_data_dir = os.path.join(project_root, "data")

        if self.is_original:
            self.output_log_dir = os.path.join(project_root, "logs")
            log_path = os.path.join(self.output_log_dir, "crawler.log")
            self.logger = setup_logger(log_path)
        else:
            self.output_log_dir = os.path.join(project_root, "logs")
            log_path = os.path.join(self.output_log_dir, "update_db.log")
            self.logger = setup_logger(log_path)

    async def fetch_all_manga(self) -> List[Dict[str, Any]]:
        """
        Asynchronously fetch all manga from MangaDex API

        Returns:
            List of manga dictionaries
        """
        manga_list = []
        limit = 100
        last_created_at = None
        seen_ids = set()
        total = 0
        three_days_ago = datetime.utcnow() - timedelta(days=3)
        created_at_since = three_days_ago.strftime("%Y-%m-%dT%H:%M:%S")
        offset = 0

        async with aiohttp.ClientSession() as session:
            while True:
                if self.is_original:
                    url = f"https://api.mangadex.org/manga?limit={limit}&order[createdAt]=asc"
                    if last_created_at:
                        url += f"&createdAtSince={last_created_at}"
                else:
                    url = (f"https://api.mangadex.org/manga?limit={limit}&offset={offset}"
                           f"&order[createdAt]=desc&createdAtSince={created_at_since}")

                try:
                    async with session.get(url) as response:
                        if response.status != 200:
                            self.logger.error(f"API Error: {response.status}")
                            break

                        data = await response.json()
                        if not data.get("data"):
                            break

                        new_manga = [
                            manga for manga in data["data"]
                            if manga["id"] not in seen_ids
                        ]

                        if not new_manga:
                            break

                        if total == 0:
                            total = data.get("total", 0)

                        manga_list.extend(new_manga)
                        seen_ids.update(manga["id"] for manga in new_manga)

                        self.logger.info(f"Collected {len(manga_list)}/{total} manga...")

                        if self.is_original:
                            # Update cursor timestamp
                            last_created_at = datetime.strptime(
                                new_manga[-1]["attributes"]["createdAt"],
                                "%Y-%m-%dT%H:%M:%S+00:00"
                            ) + timedelta(seconds=1)
                            last_created_at = last_created_at.strftime("%Y-%m-%dT%H:%M:%S")
                        else:
                            if len(data["data"]) < limit:
                                break

                            offset += limit

                        await asyncio.sleep(0.25)  # Rate limiting
                    break
                except Exception as e:
                    self.logger.error(f"Network error: {e}")
                    await asyncio.sleep(2)

        return manga_list

    @staticmethod
    def process_manga_data(manga_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process raw manga data into a structured DataFrame

        Args:
            manga_list (List[Dict]): Raw manga data from API

        Returns:
            Processed DataFrame of manga information
        """

        def extract_first_from_dict(d: Dict) -> str:
            """Helper to extract first non-empty value from multilingual dict"""
            return next(iter(d.values()), None) if d else None

        def extract_first_from_list_of_dicts(lst: List[Dict]) -> str:
            """Helper to extract first non-empty value from multilingual list"""
            return next(iter(lst[0].values()), None) if lst else None

        manga_info = [{
            "manga_id": manga.get("id"),
            "title": extract_first_from_dict(manga["attributes"].get("title", {})),
            "alt_title": extract_first_from_list_of_dicts(manga["attributes"].get("altTitles", [])),
            "description": extract_first_from_dict(manga["attributes"].get("description", {})),
            "status": manga["attributes"].get("status"),
            "year": int(manga["attributes"].get("year")) if manga["attributes"].get("year") else None,
            "created_at": manga["attributes"].get("createdAt"),
            "updated_at": manga["attributes"].get("updatedAt")
        } for manga in manga_list]

        return manga_info

    def save_to_csv(self, manga_info: List[Dict[str, Any]]) -> pd.DataFrame():
        df = pd.DataFrame(manga_info)

        # Save to CSV
        df.to_csv(f"{self.output_data_dir}/manga_data.csv", index=False, encoding="utf-8-sig")
        self.logger.info(f"Saved {len(df)} manga records")

        return df


class MangaDexChapterCrawler:
    """
    A class to crawl chapters from MangaDex API with advanced features
    """

    def __init__(
        self,
        preferred_language: str = "en",
        max_concurrent_requests: int = 4,
        is_original: bool = True
    ) -> None:
        """
        Initialize the Chapter Crawler
        """
        self.preferred_language = preferred_language
        self.max_concurrent_requests = max_concurrent_requests
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.is_original = is_original

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.output_data_dir = os.path.join(project_root, "data")

        if self.is_original:
            self.output_log_dir = os.path.join(project_root, "logs")
            log_path = os.path.join(self.output_log_dir, "crawler.log")
            self.logger = setup_logger(log_path)
        else:
            self.output_log_dir = os.path.join(project_root, "logs")
            log_path = os.path.join(self.output_log_dir, "update_db.log")
            self.logger = setup_logger(log_path)

    async def get_chapters(
        self,
        session: aiohttp.ClientSession,
        manga_id: str,
        progress_bar: tqdm
    ) -> Tuple[str, List[Dict]]:
        """
        Retrieve chapters for a specific manga, prioritizing preferred language
        """
        chapter_dict: Dict[str, Dict] = {}
        limit = 500
        offset = 0
        one_week_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")
        total: Union[int, None] = None

        while True:
            if self.is_original:
                url = f"https://api.mangadex.org/manga/{manga_id}/feed?limit={limit}&offset={offset}"
            else:
                url = (f"https://api.mangadex.org/manga/{manga_id}/feed?limit={limit}"
                       f"&offset={offset}&createdAtSince={one_week_ago}&order[chapter]=asc")

            async with self.semaphore:
                try:
                    async with session.get(url) as response:
                        if response.status != 200:
                            self.logger.warning(f"⚠️ Error {response.status} fetching chapters for manga {manga_id}")
                            break

                        data = await response.json()
                        if "data" not in data or not data["data"]:
                            break

                        if total is None:
                            total = data.get("total", 0)

                        for chap in data["data"]:
                            chapter_number = chap["attributes"].get("chapter", "Unknown")
                            lang = chap["attributes"]["translatedLanguage"]

                            if (
                                chapter_number not in chapter_dict
                                or lang == self.preferred_language
                            ):
                                chapter_dict[chapter_number] = chap

                        offset += limit
                        if self.is_original:
                            if offset >= total:
                                break
                        else:
                            if len(data["data"]) < limit:
                                break

                        await asyncio.sleep(random.uniform(0.25, 0.35))

                except Exception as e:
                    self.logger.error(f"❌ Network error: {e}, retrying.")
                    await asyncio.sleep(5)
                    continue

        self.logger.info(f"Manga {manga_id} fetched {len(chapter_dict)} unique chapters.")
        progress_bar.update(1)
        return manga_id, list(chapter_dict.values())

    async def fetch_all_chapters(
        self,
        manga_ids: List[str]
    ) -> Dict[str, List[Dict]]:
        """
        Fetch chapters for multiple manga IDs concurrently
        """
        async with aiohttp.ClientSession() as session:
            progress_bar = tqdm(
                total=len(manga_ids),
                desc="📚 Fetching chapters" if self.is_original else "📚 Fetching chapters 2 weeks ago",
                leave=True,
                dynamic_ncols=True,
            )
            tasks = [self.get_chapters(session, manga_id, progress_bar)
                     for manga_id in manga_ids]
            results = await asyncio.gather(*tasks)
            progress_bar.close()
            return {k: v for k, v in results}

    @staticmethod
    def extract_chapter_info(
        manga_id: str,
        chapter_full_data: Dict
    ) -> Dict[str, Any]:
        """
        Extract detailed information from a chapter
        """
        attributes = chapter_full_data.get("attributes", {})

        return {
            "chapter_id": chapter_full_data.get("id"),
            "manga_id": manga_id,
            "chapter_number": attributes.get("chapter"),
            "volume": attributes.get("volume"),
            "title": attributes.get("title"),
            "lang": attributes.get("translatedLanguage"),
            "pages": attributes.get("pages"),
            "created_at": attributes.get("createdAt"),
        }

    def save_to_csv(
            self,
            chapters_list_processed: Dict[str, List[Dict[str, Any]]],
            output_format: str = "csv"
    ) -> pd.DataFrame:
        """
        Save chapter data to a CSV or Parquet file.
        """
        df = pd.DataFrame(
            [chapter for chapters in chapters_list_processed.values() for chapter in chapters]
        )

        if output_format == "csv":
            out_path = f"{self.output_data_dir}/chapter_data.csv"
            df.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL, encoding="utf-8-sig")
        elif output_format == "parquet":
            out_path = f"{self.output_data_dir}/chapter_data.parquet"
            df.to_parquet(out_path, index=False)
        else:
            self.logger.warning("⚠️ Invalid format! Supports 'csv' or 'parquet'.")
            return df

        self.logger.info(f"✅ Saved chapter data to '{out_path}'")
        return df


class MangaDexImageCrawler:
    """
    A class to crawl manga chapter images from MangaDex API
    """

    def __init__(self,
                 max_workers=2,
                 timeout=6,
                 is_original=True):
        """
        Initialize the Image Crawler

        :param max_workers: Maximum number of concurrent workers
        :param timeout: Request timeout in seconds
        """
        self.is_original = is_original

        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1,
                        status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.max_workers = max_workers
        self.timeout = timeout

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.output_data_dir = os.path.join(project_root, "data")

        if self.is_original:
            self.output_log_dir = os.path.join(project_root, "logs")
            log_path = os.path.join(self.output_log_dir, "crawler.log")
            self.logger = setup_logger(log_path)
        else:
            self.output_log_dir = os.path.join(project_root, "logs")
            log_path = os.path.join(self.output_log_dir, "update_db.log")
            self.logger = setup_logger(log_path)

    def fetch_chapter_images(self, chapter_id):
        """
        Fetch image URLs for a specific chapter

        :param chapter_id: ID of the chapter
        :return: Tuple of chapter_id and list of image URLs
        """
        url = f"https://api.mangadex.org/at-home/server/{chapter_id}"
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            if data.get("result") != "ok":
                raise ValueError(f"Invalid response for chapter {chapter_id}")

            base_url = data["baseUrl"]
            hash_code = data["chapter"]["hash"]
            image_files = data["chapter"]["data"]

            self.logger.info(f"Fetched {len(image_files)} images for chapter {chapter_id}")
            return chapter_id, [f"{base_url}/data/{hash_code}/{img}" for img in image_files]
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch chapter {chapter_id}: {e}")
            return chapter_id, []

    def fetch_all_chapter_images(self, chapter_ids):
        """
        Fetch images for all chapters
        """
        all_images = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_chapter = {
                executor.submit(self.fetch_chapter_images, chap_id): chap_id
                for chap_id in chapter_ids
            }

            with tqdm(total=len(chapter_ids),
                      desc="Fetching chapters",
                      ncols=100,
                      ascii=True,
                      bar_format="{l_bar}{bar:40}{r_bar}") as progress:
                for future in as_completed(future_to_chapter):
                    chap_id, images = future.result()
                    all_images[chap_id] = images
                    progress.update(1)
                    time.sleep(random.uniform(0.1, 0.3))  # optional backoff

        return all_images

    def save_image_urls(self, all_images, output_file="chapter_images.json"):
        """
        Save image url associated with chapter_id to a file

        :param all_images: Dictionary of chapter_id to image URLs
        :param output_file: File path to save JSON
        """
        try:
            out_path = f"{self.output_data_dir}/{output_file}"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(all_images, f, indent=4, ensure_ascii=False)
            self.logger.info(f"Saved results to {out_path}")
        except Exception as e:
            self.logger.error(f"Failed to save image URLs: {e}")
