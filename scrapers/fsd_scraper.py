"""
scrapers/fsd_scraper.py — Scraper for Finnish Social Science Data Archive (FSD)
                           https://www.fsd.tuni.fi/en

Strategy:
  1. Use FSD catalogue search with dissemination_policy_string_facet=A
     to find Level A (openly available, CC BY 4.0) qualitative datasets
  2. Harvest ALL qualitative metadata via OAI-PMH
  3. For Level A projects only: download data ZIP (actual qualitative data files)
  4. Do NOT download XML metadata files (they are not the actual research data)

Repositories assigned: FSD (#11)
"""

import json
import logging
import re
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional, Tuple

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc":     "http://purl.org/dc/elements/1.1/",
}

FSD_OAI_BASE        = "https://services.fsd.tuni.fi/v0/oai"
FSD_AILA_BASE       = "https://services.fsd.tuni.fi"
FSD_QUALITATIVE_SET = "data_kind:Qualitative"

# Level A search URL — returns only openly available qualitative datasets
FSD_LEVEL_A_SEARCH = (
    "https://services.fsd.tuni.fi/catalogue/index"
    "?limit=50"
    "&study_language=en"
    "&lang=en"
    "&page={page}"
    "&field=publishing_date"
    "&direction=descending"
    "&dissemination_policy_string_facet=A"
    "&data_kind_string_facet=Qualitative"
)


class FSDScraper(BaseScraper):

    SOURCE_NAME = "FSD"
    REQUEST_DELAY = 1.5

    def search(self, keyword: str, page: int = 1) -> List[Dict]:
        """FSD uses OAI-PMH harvest — keyword search not used here."""
        return []

    def get_level_a_study_numbers(self) -> List[str]:
        """
        Scrape FSD catalogue to find all Level A qualitative study numbers.
        Returns list like ['FSD1249', 'FSD3208', ...]
        """
        study_numbers = []
        page = 0

        while True:
            url = FSD_LEVEL_A_SEARCH.format(page=page)
            logger.info("[FSD] Fetching Level A studies page %d...", page)

            resp = self.get(url, headers={"Accept": "text/html"})
            if not resp:
                break

            # Extract study numbers from HTML links
            found = re.findall(r'/catalogue/(FSD\d+)\?', resp.text, re.IGNORECASE)
            for sn in found:
                sn = sn.upper()
                if sn not in study_numbers:
                    study_numbers.append(sn)

            # Check pagination
            showing_match = re.search(r'Showing\s+(\d+)\s*/\s*(\d+)', resp.text)
            if showing_match:
                shown = int(showing_match.group(1))
                total = int(showing_match.group(2))
                logger.info("[FSD] Level A page %d: %d/%d studies found",
                            page, len(study_numbers), total)
                if shown >= total:
                    break
            else:
                break

            page += 1

        # If scraping found nothing, use hardcoded list from manual check
        if not study_numbers:
            logger.warning("[FSD] Scraping found 0 Level A studies. Using hardcoded list from manual check.")
            study_numbers = ["FSD3892", "FSD3847"]
            # Add more Level A study numbers here as you discover them

        logger.info("[FSD] Found %d Level A qualitative study numbers: %s", 
                    len(study_numbers), study_numbers)
        return study_numbers

    def harvest_oai_pmh(self) -> List[Dict]:
        """
        Full harvest of ALL qualitative records via OAI-PMH.
        Marks Level A studies using catalogue search results.
        """
        # Step 1: Get Level A study numbers from catalogue
        logger.info("[FSD] Finding Level A studies from catalogue search...")
        level_a_numbers = set(self.get_level_a_study_numbers())
        logger.info("[FSD] Level A study numbers: %s", level_a_numbers)

        # Step 2: Harvest ALL qualitative metadata via OAI-PMH
        all_projects = []
        resumption_token = None
        page = 0

        while True:
            page += 1
            params = (
                {"verb": "ListRecords", "resumptionToken": resumption_token}
                if resumption_token else
                {
                    "verb": "ListRecords",
                    "metadataPrefix": "oai_dc",
                    "set": FSD_QUALITATIVE_SET,
                }
            )

            logger.info("[FSD OAI-PMH] Fetching page %d...", page)
            resp = self.get(FSD_OAI_BASE, params=params)
            if not resp:
                break

            projects, resumption_token, total = self._parse_oai_response(resp.text)
            all_projects.extend(projects)
            logger.info("[FSD OAI-PMH] Page %d: %d records (total: %s)",
                        page, len(all_projects), total or "?")

            if not resumption_token:
                logger.info("[FSD OAI-PMH] Harvest complete.")
                break

        # Step 3: Mark Level A projects
        level_a_count = 0
        for project in all_projects:
            study_number = project.get("study_number", "")
            if study_number in level_a_numbers:
                project["license"]      = "CC BY 4.0"
                project["license_url"]  = "https://creativecommons.org/licenses/by/4.0/"
                project["access_class"] = "A"
                level_a_count += 1
            else:
                project["access_class"] = "B"

        # Tag all with query string
        for proj in all_projects:
            if not proj.get("query_string"):
                proj["query_string"] = "oai-pmh:data_kind:Qualitative"

        logger.info("[FSD] Total qualitative projects: %d (%d Level A)",
                    len(all_projects), level_a_count)
        return all_projects

    def get_dataset_files(self, project_metadata: Dict) -> List[Dict]:
        """
        Get files for an FSD dataset.
        ALL 402 projects get a file record in the database.
        
        Level A (7 projects):
        - URL recorded for manual download
        - status: FAILED_LOGIN_REQUIRED (server requires browser session)
        
        Level B/C/D (395 projects):
        - No URL (requires login/permission)
        - status: FAILED_LOGIN_REQUIRED
        """
        study_number = project_metadata.get("study_number", "")
        source_url   = project_metadata.get("source_url", "")

        if not study_number:
            match = re.search(r'FSD\d+', source_url, re.IGNORECASE)
            if match:
                study_number = match.group(0).upper()

        if not study_number:
            return []

        is_open = project_metadata.get("access_class", "") == "A"

        # Level A — record download URL for manual download
        if is_open:
            zip_url = (
                f"{FSD_AILA_BASE}/catalogue/download"
                f"?lang=en&study_language=en"
            )
            download_tab_url = (
                f"{FSD_AILA_BASE}/catalogue/{study_number}"
                f"?tab=download&lang=en&study_language=en"
            )
            logger.info("[FSD] Level A: %s — %s", study_number, zip_url)
        else:
            zip_url          = ""
            download_tab_url = ""

        # ALL projects get a file record
        return [{
            "source":            self.SOURCE_NAME,
            "project_id":        None,
            "file_name":         f"{study_number}.zip",
            "file_url":          zip_url,
            "file_url_referrer": download_tab_url,
            "file_type":         "zip",
            "status":            "FAILED_LOGIN_REQUIRED",
        }]

    # ── OAI-PMH parsing ───────────────────────────────────────────────────────

    def _parse_oai_response(self, xml_text: str) -> Tuple[List[Dict], Optional[str], Optional[str]]:
        """Parse OAI-PMH ListRecords XML. Returns (projects, resumption_token, total)."""
        projects = []
        resumption_token = None
        total = None

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.error("[FSD] XML parse error: %s", e)
            return projects, None, None

        error_el = root.find("oai:error", NS)
        if error_el is not None:
            logger.error("[FSD] OAI-PMH error: %s — %s",
                         error_el.get("code"), error_el.text)
            return projects, None, None

        list_records = root.find("oai:ListRecords", NS)
        if list_records is None:
            return projects, None, None

        for record in list_records.findall("oai:record", NS):
            header   = record.find("oai:header", NS)
            metadata = record.find("oai:metadata", NS)

            if header is None or metadata is None:
                continue
            if header.get("status", "") == "deleted":
                continue

            identifier = self._text(header.find("oai:identifier", NS))
            dc = metadata.find("oai_dc:dc", NS)
            if dc is None:
                continue

            def dc_values(tag):
                return [el.text.strip() for el in dc.findall(f"dc:{tag}", NS)
                        if el.text and el.text.strip()]

            title       = "; ".join(dc_values("title")) or ""
            description = " ".join(dc_values("description")) or ""
            creators    = "; ".join(dc_values("creator")) or ""
            subjects    = "; ".join(dc_values("subject")) or ""
            date        = dc_values("date")[0] if dc_values("date") else ""
            language    = "; ".join(dc_values("language")) or ""
            rights_list = dc_values("rights")
            identifiers = dc_values("identifier")

            doi          = ""
            source_url   = ""
            study_number = ""

            for ident in identifiers:
                if "doi.org" in ident:
                    doi = ident.replace("https://doi.org/", "").replace("http://doi.org/", "")
                elif "fsd.tuni.fi" in ident and ident.startswith("http"):
                    source_url = ident
                elif ident.startswith("http") and not source_url:
                    source_url = ident
                match = re.search(r'FSD\d+', ident, re.IGNORECASE)
                if match and not study_number:
                    study_number = match.group(0).upper()

            if not study_number:
                match = re.search(r'FSD\d+', identifier, re.IGNORECASE)
                if match:
                    study_number = match.group(0).upper()

            if not source_url and study_number:
                source_url = (
                    f"https://services.fsd.tuni.fi/catalogue"
                    f"/{study_number}?lang=en"
                )

            projects.append({
                "source":              self.SOURCE_NAME,
                "source_url":          source_url,
                "project_title":       title,
                "project_description": description,
                "authors":             creators,
                "publication_date":    date,
                "doi":                 doi,
                "license":             " | ".join(rights_list) or "Unknown",
                "license_url":         "",
                "keywords":            subjects,
                "subject_area":        subjects,
                "language":            language,
                "study_number":        study_number,
                "access_class":        "",
                "has_qda_file":        False,
                "has_primary_data":    False,
                "raw_metadata":        json.dumps({
                    "identifier": identifier,
                    "rights":     rights_list,
                }, default=str),
            })

        rt_el = list_records.find("oai:resumptionToken", NS)
        if rt_el is not None:
            if rt_el.text and rt_el.text.strip():
                resumption_token = rt_el.text.strip()
            total = rt_el.get("completeListSize")

        return projects, resumption_token, total

    def _text(self, element) -> str:
        return element.text.strip() if element is not None and element.text else ""