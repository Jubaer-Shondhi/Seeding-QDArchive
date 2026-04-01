"""
scrapers/base_scraper.py — Abstract base class for all repository scrapers.
"""

import logging
import time
import requests
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from config import OPEN_LICENSES, QDA_EXTENSIONS, PRIMARY_DATA_EXTENSIONS, DRYAD_API_TOKEN

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Abstract base class for repository scrapers.
    All scrapers must implement search() and get_dataset_files().
    """

    SOURCE_NAME = "Unknown"
    REQUEST_DELAY = 1.0   # seconds between requests (be polite!)

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "QDArchive-Seeding-Pipeline/1.0 (FAU Erlangen; research project)",
            "Authorization": f"Bearer {DRYAD_API_TOKEN}",
        })

    # ── Abstract methods ──────────────────────────────────────────────────────

    @abstractmethod
    def search(self, keyword: str, page: int = 1) -> List[Dict]:
        """
        Search the repository for a keyword.
        Returns a list of project metadata dicts.
        """
        ...

    @abstractmethod
    def get_dataset_files(self, project_metadata: Dict) -> List[Dict]:
        """
        Given a project metadata dict, return a list of file metadata dicts.
        """
        ...

    # ── Shared helpers ────────────────────────────────────────────────────────

    def get(self, url: str, params: dict = None, **kwargs) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(1, 4):
            try:
                time.sleep(self.REQUEST_DELAY)
                resp = self.session.get(url, params=params, timeout=30, **kwargs)
                resp.raise_for_status()
                return resp
            except requests.exceptions.HTTPError as e:
                # Don't retry on 404 (not found) or 410 (gone) — will never succeed
                if e.response is not None and e.response.status_code in (404, 410):
                    logger.debug("Skipping %s (HTTP %s)", url, e.response.status_code)
                    return None
                logger.warning("GET %s attempt %d failed: %s", url, attempt, e)
                time.sleep(attempt * 2)
            except requests.RequestException as e:
                logger.warning("GET %s attempt %d failed: %s", url, attempt, e)
                time.sleep(attempt * 2)
        logger.error("All attempts failed for %s", url)
        return None

    def is_open_license(self, license_str: str) -> bool:
        """Return True if license_str matches a known open license."""
        if not license_str:
            return False
        lower = license_str.lower()
        return any(lic in lower for lic in OPEN_LICENSES)

    def classify_file(self, filename: str) -> Dict[str, bool]:
        """
        Classify a filename as QDA, primary data, or additional.
        Returns dict: {is_qda_file, is_primary_data, is_additional}
        """
        ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        is_qda = ext in QDA_EXTENSIONS
        is_primary = (not is_qda) and (ext in PRIMARY_DATA_EXTENSIONS)
        is_additional = not is_qda and not is_primary
        return {
            "is_qda_file": is_qda,
            "is_primary_data": is_primary,
            "is_additional": is_additional,
            "file_type": ext.lstrip("."),
        }

    def scrape_all(self, keywords: List[str], max_pages: int = 10) -> List[Dict]:
        """
        Run full search across all keywords and all pages.
        Returns deduplicated list of project metadata dicts.
        """
        seen_dois = set()
        all_projects = []

        for keyword in keywords:
            logger.info("[%s] Searching: '%s'", self.SOURCE_NAME, keyword)
            for page in range(1, max_pages + 1):
                logger.info("[%s] Page %d ...", self.SOURCE_NAME, page)
                projects = self.search(keyword, page)

                if not projects:
                    logger.info("[%s] No more results for '%s'.", self.SOURCE_NAME, keyword)
                    break

                for proj in projects:
                    doi = proj.get("doi")
                    key = doi if doi else proj.get("source_url", "")
                    if key and key in seen_dois:
                        continue
                    seen_dois.add(key)
                    # Tag each project with the keyword that found it
                    proj["query_string"] = keyword
                    all_projects.append(proj)

        logger.info("[%s] Total unique projects found: %d", self.SOURCE_NAME, len(all_projects))
        return all_projects
