"""
downloader.py — Download files from URLs with size checks and logging.
Uses DOWNLOAD_RESULT enum values from schema.
"""

import os
import logging
import time
import requests
from typing import Optional

from config import (
    FILES_DIR,
    DOWNLOAD_TIMEOUT,
    DOWNLOAD_CHUNK_SIZE,
    MAX_FILE_SIZE_MB,
    RETRY_ATTEMPTS,
    DRYAD_API_TOKEN,
)
from database import (
    SUCCEEDED,
    FAILED_SERVER_UNRESPONSIVE,
    FAILED_LOGIN_REQUIRED,
    FAILED_TOO_LARGE,
)

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
DOWNLOAD_DELAY = 3  # seconds between downloads


def create_session() -> requests.Session:
    """Create a requests session with default headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "QDArchive-Seeding-Pipeline/1.0 (FAU Erlangen; research project)",
        "Authorization": f"Bearer {DRYAD_API_TOKEN}",
    })
    return session


def polite_delay(delay_seconds: int = DOWNLOAD_DELAY):
    """Politely wait between requests."""
    time.sleep(delay_seconds)


def _safe_filename(name: str) -> str:
    """Sanitize filename and truncate to avoid Windows path limit."""
    keep = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._- ()")
    name = "".join(c if c in keep else "_" for c in name)
    if "." in name:
        base, ext = name.rsplit(".", 1)
        return base[:95] + "." + ext
    return name[:100]


def _is_html_content(content: bytes, filename: str = "") -> bool:
    """
    Check if content appears to be HTML.
    
    Args:
        content: Bytes content to check
        filename: Optional filename for extension-based checks
    
    Returns:
        True if content appears to be HTML
    """
    # Check file extension first
    if filename.endswith(('.xml', '.zip', '.pdf', '.docx', '.xlsx')):
        # For known file types, HTML is definitely wrong
        lower_content = content[:500].lower()
        if b"<html" in lower_content or b"<!doctype" in lower_content:
            return True
        # For XML files, check if it starts with XML declaration
        if filename.endswith('.xml'):
            return not (content[:100].startswith(b'<?xml') or content[:100].startswith(b'<'))
        return False
    
    # For unknown types, check content
    lower_content = content[:500].lower()
    return b"<html" in lower_content or b"<!doctype" in lower_content


def download_file(
    file_url: str,
    file_name: str,
    source: str,
    project_folder: str,
    session: Optional[requests.Session] = None,
    referrer_url: str = "",
) -> dict:
    """
    Download a single file and save it to disk.

    Args:
        file_url:      URL to download from
        file_name:     Name to save the file as
        source:        Repository name (e.g. "Dryad", "FSD")
        project_folder: Folder name for this project
        session:       Optional requests session to reuse
        referrer_url:  Optional URL to visit first (needed for FSD downloads)

    Returns:
        dict with:
            status    : DOWNLOAD_RESULT enum value
            file_path : local path where file was saved (empty if failed)
    """
    if not file_url:
        return {"status": FAILED_SERVER_UNRESPONSIVE, "file_path": ""}

    # Build local save path
    folder = os.path.join(
        FILES_DIR,
        _safe_filename(source),
        _safe_filename(project_folder),
    )
    os.makedirs(folder, exist_ok=True)

    local_path = os.path.join(folder, _safe_filename(file_name) or "file")

    # Skip if already downloaded and file is valid (not HTML)
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        # Check if existing file is actually HTML (bad download from previous run)
        try:
            with open(local_path, "rb") as f:
                header = f.read(500)
                if _is_html_content(header, file_name):
                    logger.warning("Existing file is HTML, will re-download: %s", local_path)
                    os.remove(local_path)
                else:
                    logger.info("Already downloaded: %s", local_path)
                    return {"status": SUCCEEDED, "file_path": local_path}
        except Exception:
            pass

    sess = session or requests.Session()
    sess.headers.update({
        "User-Agent": "QDArchive-Seeding-Pipeline/1.0 (FAU Erlangen; research project)",
        "Authorization": f"Bearer {DRYAD_API_TOKEN}",
    })

    # Visit referrer URL first if provided (FSD needs this to set cookies)
    if referrer_url:
        logger.info("Visiting referrer page first: %s", referrer_url)
        try:
            # This sets session cookies and accepts terms automatically
            referrer_resp = sess.get(referrer_url, timeout=30)
            # Check if referrer page indicates login required
            if _is_html_content(referrer_resp.content[:500]):
                logger.warning("Referrer page is HTML login page")
        except Exception as e:
            logger.warning("Referrer visit failed: %s", e)

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            logger.info("Downloading [%s]: %s (attempt %d)", source, file_url, attempt)
            resp = sess.get(file_url, stream=True, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
            
            # Check final URL after redirects
            final_url = resp.url
            logger.debug("Final URL after redirects: %s", final_url)
            
            # If redirected to login page, mark as login required
            if "login" in final_url.lower() or "signin" in final_url.lower() or "auth" in final_url.lower():
                logger.warning("Redirected to login page: %s", final_url)
                return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}
            
            resp.raise_for_status()

            # Check content type header
            content_type = resp.headers.get("Content-Type", "").lower()
            
            # For FSD, aggressively check for HTML
            if source == "FSD":
                # If content type is HTML, definitely login required
                if "text/html" in content_type:
                    logger.warning("Got HTML response instead of file — login required")
                    return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}
                
                # If content type is missing or suspicious, check first bytes
                first_chunk = next(resp.iter_content(chunk_size=500), b'')
                if _is_html_content(first_chunk, file_name):
                    logger.warning("First bytes indicate HTML — login required")
                    return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}
                
                # Reset the response stream if we consumed part of it
                resp.close()
                resp = sess.get(file_url, stream=True, timeout=DOWNLOAD_TIMEOUT)
            else:
                # For other repositories (Dryad), use standard detection
                if "text/html" in content_type and not file_name.endswith(('.html', '.htm')):
                    logger.warning("Got HTML response but expected file")
                    return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}

            # Check content length against size limit
            content_length = int(resp.headers.get("Content-Length", 0))
            if content_length and content_length > MAX_FILE_SIZE_BYTES:
                logger.warning("File too large: %.1f MB", content_length / 1e6)
                return {"status": FAILED_TOO_LARGE, "file_path": ""}

            # Stream write to disk
            downloaded = 0
            is_html = False
            first_chunk = True
            
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        # Check first chunk for HTML
                        if first_chunk and _is_html_content(chunk, file_name):
                            is_html = True
                            break
                        first_chunk = False
                        f.write(chunk)
                        downloaded += len(chunk)
                        if downloaded > MAX_FILE_SIZE_BYTES:
                            os.remove(local_path)
                            return {"status": FAILED_TOO_LARGE, "file_path": ""}
            
            if is_html:
                os.remove(local_path)
                logger.warning("Downloaded file is HTML — login required")
                return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}

            # Final check — read first few bytes of saved file
            with open(local_path, "rb") as f:
                header = f.read(500)
                if _is_html_content(header, file_name):
                    os.remove(local_path)
                    logger.warning("Saved file is HTML — login required")
                    return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}
                
                # Special check for XML files
                if file_name.endswith('.xml') and not (header[:100].startswith(b'<?xml') or header[:100].startswith(b'<')):
                    os.remove(local_path)
                    logger.warning("Saved file is not valid XML — probably error page")
                    return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}
                
                # Special check for ZIP files
                if file_name.endswith('.zip') and not header.startswith(b'PK'):
                    os.remove(local_path)
                    logger.warning("Saved file is not valid ZIP — probably error page")
                    return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}

            logger.info("Saved: %s (%d bytes)", local_path, downloaded)
            return {"status": SUCCEEDED, "file_path": local_path}

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0

            if code in (401, 403):
                logger.error("HTTP %d — authentication required", code)
                return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}

            elif code == 404:
                logger.warning("File not found (404): %s", file_url)
                return {"status": FAILED_SERVER_UNRESPONSIVE, "file_path": ""}

            elif code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 0))
                wait = retry_after + 5 if retry_after else min(attempt * 30, 300)
                logger.warning("Rate limited (429). Waiting %ds...", wait)
                time.sleep(wait)

            elif code == 503:
                wait = attempt * 15
                logger.warning("Service unavailable (503). Waiting %ds...", wait)
                time.sleep(wait)

            else:
                logger.warning("HTTP %s on attempt %d", code, attempt)
                if os.path.exists(local_path):
                    os.remove(local_path)

        except requests.RequestException as e:
            logger.warning("Attempt %d failed: %s", attempt, e)
            if os.path.exists(local_path):
                os.remove(local_path)

    

    logger.error("All %d attempts failed for %s", RETRY_ATTEMPTS, file_url)
    return {"status": FAILED_SERVER_UNRESPONSIVE, "file_path": ""}

def download_file_post(
    file_url: str,
    file_name: str,
    source: str,
    project_folder: str,
    session: Optional[requests.Session] = None,
    referrer_url: str = "",
    form_data: dict = None,
) -> dict:
    """
    Download a file using POST request (for FSD form submission).
    """
    if not file_url:
        return {"status": FAILED_SERVER_UNRESPONSIVE, "file_path": ""}

    # Build local save path
    folder = os.path.join(
        FILES_DIR,
        _safe_filename(source),
        _safe_filename(project_folder),
    )
    os.makedirs(folder, exist_ok=True)

    local_path = os.path.join(folder, _safe_filename(file_name) or "file")

    # Skip if already downloaded and valid
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        with open(local_path, "rb") as f:
            header = f.read(500)
            if not _is_html_content(header, file_name):
                logger.info("Already downloaded: %s", local_path)
                return {"status": SUCCEEDED, "file_path": local_path}
            else:
                os.remove(local_path)

    sess = session or requests.Session()
    sess.headers.update({
        "User-Agent": "QDArchive-Seeding-Pipeline/1.0 (FAU Erlangen; research project)",
        "Accept": "application/zip, application/octet-stream, */*",
    })

    # Step 1: Visit referrer page to get cookies
    if referrer_url:
        logger.info("Visiting referrer page: %s", referrer_url)
        try:
            referrer_resp = sess.get(referrer_url, timeout=30)
            
            # Try to extract CSRF token if present
            import re
            csrf_match = re.search(r'name="csrf_token"\s+value="([^"]+)"', referrer_resp.text)
            if csrf_match and form_data:
                form_data["csrf_token"] = csrf_match.group(1)
                logger.info("Found CSRF token")
                
            # Also try other common token names
            token_match = re.search(r'name="_token"\s+value="([^"]+)"', referrer_resp.text)
            if token_match and form_data:
                form_data["_token"] = token_match.group(1)
                
        except Exception as e:
            logger.warning("Referrer visit failed: %s", e)

    # Step 2: Make POST request to download
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            logger.info("POST download [%s]: %s (attempt %d)", source, file_url, attempt)
            
            # Make POST request with form data
            if form_data:
                logger.debug("Form data: %s", form_data)
                resp = sess.post(file_url, data=form_data, stream=True, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
            else:
                resp = sess.post(file_url, stream=True, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
            
            # Check final URL after redirects
            final_url = resp.url
            logger.info("Final URL after redirects: %s", final_url)
            
            # If redirected to a download URL, that's good
            if final_url != file_url and '.zip' in final_url:
                logger.info("Redirected to ZIP URL: %s", final_url)
            
            resp.raise_for_status()

            # Check if response is HTML (error)
            content_type = resp.headers.get("Content-Type", "").lower()
            if "text/html" in content_type:
                logger.warning("Got HTML response - login required")
                return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}

            # Check content length
            content_length = int(resp.headers.get("Content-Length", 0))
            if content_length and content_length > MAX_FILE_SIZE_BYTES:
                logger.warning("File too large: %.1f MB", content_length / 1e6)
                return {"status": FAILED_TOO_LARGE, "file_path": ""}

            # Stream write to disk
            downloaded = 0
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if downloaded > MAX_FILE_SIZE_BYTES:
                            os.remove(local_path)
                            return {"status": FAILED_TOO_LARGE, "file_path": ""}

            # Validate downloaded file
            with open(local_path, "rb") as f:
                header = f.read(500)
                if _is_html_content(header, file_name):
                    os.remove(local_path)
                    logger.warning("Downloaded file is HTML - login required")
                    return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}
                
                if file_name.endswith('.zip') and not header.startswith(b'PK'):
                    os.remove(local_path)
                    logger.warning("Downloaded file is not valid ZIP")
                    return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}

            logger.info("Saved: %s (%d bytes)", local_path, downloaded)
            return {"status": SUCCEEDED, "file_path": local_path}

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code == 404:
                logger.warning("File not found (404): %s", file_url)
                return {"status": FAILED_SERVER_UNRESPONSIVE, "file_path": ""}
            elif code in (401, 403):
                logger.warning("HTTP %d - authentication required", code)
                return {"status": FAILED_LOGIN_REQUIRED, "file_path": ""}
            else:
                logger.warning("HTTP %s on attempt %d", code, attempt)
                time.sleep(attempt * 2)
        except Exception as e:
            logger.warning("Attempt %d failed: %s", attempt, e)
            time.sleep(attempt * 2)

    logger.error("All %d attempts failed for %s", RETRY_ATTEMPTS, file_url)
    return {"status": FAILED_SERVER_UNRESPONSIVE, "file_path": ""}