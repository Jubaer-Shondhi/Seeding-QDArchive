"""
pipeline.py — Main entry point for QDArchive Seeding Pipeline (Part 1: Acquisition).

Usage:
    python pipeline.py --source dryad    # Only Dryad (repo #2)
    python pipeline.py --source fsd      # Only FSD (repo #11)
    python pipeline.py --no-download     # Metadata only, skip downloads
    python pipeline.py --stats           # Print database stats and exit
    python pipeline.py --export          # Export CSVs and exit

Repositories:
    Dryad (#2)  — https://datadryad.org
    FSD   (#11) — https://www.fsd.tuni.fi
"""

import argparse
import logging
import os
import sys
import time
import requests
from downloader import download_file, download_file_post, create_session, polite_delay, DOWNLOAD_DELAY
from downloader import download_file, download_file_post, create_session, polite_delay, DOWNLOAD_DELAY, _safe_filename

from config import (
    SEARCH_KEYWORDS,
    DRYAD_MAX_PAGES,
    FSD_MAX_PAGES,
    LOG_FILE,
    DATA_DIR,
    DRYAD_API_TOKEN,
    FILES_DIR
)
from database import (
    init_db,
    insert_project,
    insert_file,
    update_file_status,
    export_all,
    print_stats,
    SUCCEEDED,
    FAILED_SERVER_UNRESPONSIVE,
    FAILED_LOGIN_REQUIRED,
)
from scrapers.dryad_scraper import DryadScraper
from scrapers.fsd_scraper import FSDScraper
from downloader import download_file

# ── Logging setup ─────────────────────────────────────────────────────────────
os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline")

DOWNLOAD_DELAY = 3  # seconds between downloads


def create_session() -> requests.Session:
    """Create a requests session with default headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "QDArchive-Seeding-Pipeline/1.0 (FAU Erlangen; research project)",
        "Authorization": f"Bearer {DRYAD_API_TOKEN}",
    })
    return session


def run_scraper(scraper, keywords, max_pages, download: bool = True):
    """Search, insert projects, insert files, optionally download."""
    session = create_session()
    all_projects = scraper.scrape_all(keywords, max_pages=max_pages)
    logger.info("[%s] Inserting %d projects into DB...",
                scraper.SOURCE_NAME, len(all_projects))

    inserted_projects = 0
    inserted_files    = 0

    for project_meta in all_projects:
        project_id = insert_project(project_meta)
        if not project_id:
            continue
        inserted_projects += 1

        try:
            files = scraper.get_dataset_files(project_meta)
        except Exception as e:
            logger.warning("Failed to get files for '%s': %s",
                           project_meta.get("project_title", "?"), e)
            files = []

        for file_meta in files:
            file_meta["project_id"] = project_id
            file_id = insert_file({
                "project_id": project_id,
                "file_name":  file_meta.get("file_name", ""),
                "file_type":  file_meta.get("file_type", ""),
                "status":     file_meta.get("status", FAILED_SERVER_UNRESPONSIVE),
            })
            inserted_files += 1

            if download:
                file_url = file_meta.get("file_url", "")
                if not file_url:
                    update_file_status(file_id, FAILED_LOGIN_REQUIRED)
                    continue

                time.sleep(DOWNLOAD_DELAY)

                project_folder = (
                    project_meta.get("study_number")
                    or (project_meta.get("doi", "").split("/")[-1])
                    or str(project_id)
                )

                result = download_file(
                    file_url=file_url,
                    file_name=file_meta.get("file_name", ""),
                    source=scraper.SOURCE_NAME,
                    project_folder=project_folder,
                    session=session,
                    referrer_url=file_meta.get("file_url_referrer", ""),
                )
                update_file_status(file_id, result["status"])

    logger.info("[%s] Done. Projects: %d, Files: %d",
                scraper.SOURCE_NAME, inserted_projects, inserted_files)

def run_fsd(download: bool = True):
    """Run FSD scraper using OAI-PMH full harvest."""
    scraper = FSDScraper()
    session = create_session()

    logger.info("Starting FSD full OAI-PMH harvest...")
    all_projects = scraper.harvest_oai_pmh()
    logger.info("OAI-PMH returned %d records.", len(all_projects))

    for project_meta in all_projects:
        # Insert project into database
        project_id = insert_project(project_meta)
        if not project_id:
            continue

        files = scraper.get_dataset_files(project_meta)
        for file_meta in files:
            file_id = insert_file({
                "project_id": project_id,
                "file_name":  file_meta.get("file_name", ""),
                "file_type":  file_meta.get("file_type", ""),
                "status":     file_meta.get("status", FAILED_SERVER_UNRESPONSIVE),
            })

            # CREATE FOLDER even if download fails
            study_number = project_meta.get("study_number", str(project_id))
            folder = os.path.join(FILES_DIR, "FSD", _safe_filename(study_number))
            os.makedirs(folder, exist_ok=True)
            logger.debug("[FSD] Created folder: %s", folder)

            if download:
                file_url = file_meta.get("file_url", "")
                if not file_url:
                    update_file_status(file_id, FAILED_LOGIN_REQUIRED)
                    continue

                time.sleep(DOWNLOAD_DELAY)

                result = download_file(
                    file_url=file_url,
                    file_name=file_meta.get("file_name", ""),
                    source="FSD",
                    project_folder=study_number,
                    session=session,
                    referrer_url=file_meta.get("file_url_referrer", ""),
                )
                update_file_status(file_id, result["status"])


def main():
    parser = argparse.ArgumentParser(
        description="QDArchive Seeding Pipeline — Part 1: Acquisition"
    )
    parser.add_argument("--source", choices=["dryad", "fsd", "both"],
                        default="both", help="Which repository to scrape")
    parser.add_argument("--no-download", action="store_true",
                        help="Metadata only, skip downloads")
    parser.add_argument("--stats",  action="store_true",
                        help="Print stats and exit")
    parser.add_argument("--export", action="store_true",
                        help="Export CSVs and exit")
    args = parser.parse_args()

    init_db()

    if args.stats:
        print_stats()
        return

    if args.export:
        export_all()
        print_stats()
        return

    download = not args.no_download
    source   = args.source

    # ── Dryad (Repository #2) ──────────────────────────────────────────────────
    if source in ("dryad", "both"):
        logger.info("=" * 60)
        logger.info("Starting Dryad scraper (Repository #2)...")
        logger.info("=" * 60)
        run_scraper(DryadScraper(), SEARCH_KEYWORDS, DRYAD_MAX_PAGES, download=download)

    # ── FSD (Repository #11) ───────────────────────────────────────────────────
    if source in ("fsd", "both"):
        logger.info("=" * 60)
        logger.info("Starting FSD scraper (Repository #11)...")
        logger.info("=" * 60)
        run_fsd(download=download)

    # ── Export ─────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Pipeline complete. Exporting to CSV...")
    export_all()
    print_stats()
    logger.info("Done! Check data/ folder for results.")
    logger.info("Next: git tag part-1-release && git push origin part-1-release")


if __name__ == "__main__":
    main()
