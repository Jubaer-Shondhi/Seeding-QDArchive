"""
scrapers/dryad_scraper.py — Scraper for Dryad Digital Repository (datadryad.org)

Uses the Dryad public REST API v2:
  https://datadryad.org/api/v2/

Key endpoints used:
  GET /search?q=<keyword>&page=<n>&per_page=<n>  → list datasets
  GET /datasets/<encoded_doi>/versions            → get latest version
  GET /versions/<id>/files                        → list files in version
  GET /files/<id>/download                        → download file
"""

import json
import logging
import urllib.parse
from typing import List, Dict, Optional

from scrapers.base_scraper import BaseScraper
from config import DRYAD_API_BASE, DRYAD_PAGE_SIZE, DRYAD_MAX_PAGES

logger = logging.getLogger(__name__)


class DryadScraper(BaseScraper):

    SOURCE_NAME = "Dryad"
    REQUEST_DELAY = 1.0

    def search(self, keyword: str, page: int = 1) -> List[Dict]:
        params = {
            "q": keyword,
            "page": page,
            "per_page": DRYAD_PAGE_SIZE,
        }
        resp = self.get(f"{DRYAD_API_BASE}/search", params=params)
        if not resp:
            return []

        data = resp.json()
        embedded = data.get("_embedded", {})
        datasets = embedded.get("stash:datasets", [])

        if not datasets:
            return []

        results = []
        for ds in datasets:
            meta = self._normalize_dataset(ds)
            if meta:
                results.append(meta)
        return results

    def get_dataset_files(self, project_metadata: Dict) -> List[Dict]:
        """
        Fetch file list using version-aware strategy to avoid 404s on old DOIs.
        Strategy:
          1. Try _links from raw_metadata (fastest)
          2. Try /datasets/<doi>/versions -> latest version -> files
          3. Fallback to /datasets/<doi>/files
        """
        doi = project_metadata.get("doi", "")
        if not doi:
            return []

        # Strategy 1: Use _links from raw metadata
        raw = project_metadata.get("raw_metadata", "")
        if raw:
            try:
                ds = json.loads(raw)
                files = self._files_from_links(ds, project_metadata)
                if files is not None:
                    return files
            except Exception:
                pass

        # Strategy 2: Get latest version then fetch its files
        encoded_doi = urllib.parse.quote(doi, safe="")
        versions_url = f"{DRYAD_API_BASE}/datasets/{encoded_doi}/versions"
        resp = self.get(versions_url)
        if resp:
            try:
                data = resp.json()
                embedded = data.get("_embedded", {})
                versions = embedded.get("stash:versions", [])
                if versions:
                    version = versions[0]
                    version_links = version.get("_links", {})
                    if "stash:files" in version_links:
                        files_href = version_links["stash:files"].get("href", "")
                        if files_href:
                            files_url = files_href if files_href.startswith("http") else f"https://datadryad.org{files_href}"
                            resp2 = self.get(files_url)
                            if resp2:
                                return self._parse_files_response(resp2, project_metadata)
            except Exception as e:
                logger.debug("Version strategy failed for %s: %s", doi, e)

        # Strategy 3: Direct files endpoint (fallback)
        files_url = f"{DRYAD_API_BASE}/datasets/{encoded_doi}/files"
        resp = self.get(files_url)
        if resp:
            return self._parse_files_response(resp, project_metadata)

        logger.debug("[Dryad] Could not retrieve files for DOI: %s", doi)
        return []

    def _files_from_links(self, ds: dict, project_metadata: dict) -> Optional[List[Dict]]:
        """Try to get files using _links embedded in dataset metadata."""
        links = ds.get("_links", {})

        if "stash:files" in links:
            href = links["stash:files"].get("href", "")
            if href:
                url = href if href.startswith("http") else f"https://datadryad.org{href}"
                resp = self.get(url)
                if resp:
                    return self._parse_files_response(resp, project_metadata)

        if "stash:version" in links:
            href = links["stash:version"].get("href", "")
            if href:
                url = href if href.startswith("http") else f"https://datadryad.org{href}"
                resp = self.get(url)
                if resp:
                    try:
                        version_data = resp.json()
                        version_links = version_data.get("_links", {})
                        if "stash:files" in version_links:
                            files_href = version_links["stash:files"].get("href", "")
                            if files_href:
                                files_url = files_href if files_href.startswith("http") else f"https://datadryad.org{files_href}"
                                resp2 = self.get(files_url)
                                if resp2:
                                    return self._parse_files_response(resp2, project_metadata)
                    except Exception:
                        pass
        return None

    def _parse_files_response(self, resp, project_metadata: dict) -> List[Dict]:
        """Parse a Dryad files API response into our file metadata format."""
        try:
            data = resp.json()
        except Exception:
            return []

        embedded = data.get("_embedded", {})
        raw_files = embedded.get("stash:files", [])

        files = []
        for f in raw_files:
            filename = f.get("path", "") or f.get("name", "")
            if not filename:
                continue

            classification = self.classify_file(filename)

            links = f.get("_links", {})
            download_url = ""
            for key in ("stash:file-download", "stash:download", "self"):
                if key in links:
                    href = links[key].get("href", "")
                    if href:
                        download_url = href if href.startswith("http") else f"https://datadryad.org{href}"
                        break

            file_meta = {
                "source": self.SOURCE_NAME,
                "project_id": None,
                "file_name": filename,
                "file_url": download_url,
                "file_type": classification.get("file_type", ""),
                "status": "FAILED_SERVER_UNRESPONSIVE",
            }
            files.append(file_meta)

        logger.debug("[Dryad] Found %d files for %s", len(files), project_metadata.get("doi", ""))
        return files

    def _normalize_dataset(self, ds: dict) -> Optional[Dict]:
        """Normalize raw Dryad API dataset. Only accept open license datasets."""
        license_info = ds.get("license", "") or ""
        if isinstance(license_info, dict):
            license_str = license_info.get("uri", "") or license_info.get("name", "")
        else:
            license_str = str(license_info)

        if not self.is_open_license(license_str):
            logger.debug("Skipping dataset (no open license): %s", ds.get("title", ""))
            return None

        authors_raw = ds.get("authors", [])
        if isinstance(authors_raw, list):
            authors = "; ".join(
                f"{a.get('lastName', '')}, {a.get('firstName', '')}".strip(", ")
                for a in authors_raw
            )
        else:
            authors = str(authors_raw)

        keywords_raw = ds.get("keywords", [])
        keywords = "; ".join(keywords_raw) if isinstance(keywords_raw, list) else str(keywords_raw)

        doi = ds.get("identifier", "")
        if doi and not doi.startswith("http"):
            doi_url = f"https://doi.org/{doi}"
        else:
            doi_url = doi
            doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")

        links = ds.get("_links", {})
        source_url = ""
        if "self" in links:
            href = links["self"].get("href", "")
            if href:
                source_url = href if href.startswith("http") else f"https://datadryad.org{href}"

        return {
            "source": self.SOURCE_NAME,
            "source_url": source_url or doi_url,
            "project_title": ds.get("title", ""),
            "project_description": ds.get("abstract", "") or ds.get("description", ""),
            "authors": authors,
            "publication_date": ds.get("publicationDate") or ds.get("lastModificationDate", ""),
            "doi": doi,
            "license": license_str,
            "license_url": license_str if license_str.startswith("http") else "",
            "keywords": keywords,
            "subject_area": "; ".join(ds.get("fieldOfScience", [])) if isinstance(ds.get("fieldOfScience"), list) else ds.get("fieldOfScience", ""),
            "language": "",
            "has_qda_file": False,
            "has_primary_data": False,
            "raw_metadata": json.dumps(ds, default=str),
        }
