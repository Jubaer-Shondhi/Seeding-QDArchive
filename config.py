"""
config.py — Central configuration for QDArchive pipeline.
Adjust paths, search terms, and API settings here.
"""

import os

# ─── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
FILES_DIR = os.path.join(DATA_DIR, "files")
DB_PATH = os.path.join(DATA_DIR, "archive.db")
CSV_EXPORT_PATH = os.path.join(DATA_DIR, "metadata_export.csv")

# Create directories if they don't exist
os.makedirs(FILES_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# ─── Search Terms ─────────────────────────────────────────────────────────────
SEARCH_KEYWORDS = [
    # ── REFI-QDA Standard ──
    "qdpx",
    "qdc",
    # ── MAXQDA ──
    "mqda",
    "mqex",
    "mx24",
    "mx22",
    "mx20",
    "mx18",
    "mx12",
    # ── NVivo ──
    "nvivo",
    "nvp",
    "nvpx",
    # ── ATLAS.ti ──
    "atlasti",
    "atlasproj",
    "hpr7",
    # ── QDA Miner ──
    "ppj",
    "pprj",
    "qlt",
    # ── f4analyse ──
    "f4p",
    # ── Quirkos ──
    "qpd",
    # ── Broader fallback queries ──
    "qualitative research",
    "interview study",
    "interview transcript",
    "qualitative data analysis",
    "thematic analysis",
    "grounded theory",
]

# ─── QDA File Extensions ──

QDA_EXTENSIONS = {
    # REFI-QDA Standard
    ".qdpx",
    ".qdc",
    # MAXQDA
    ".mqda",
    ".mqbac",
    ".mqtc",
    ".mqex",
    ".mqmtr",
    ".mx24",
    ".mx24bac",
    ".mc24",
    ".mex24",
    ".mx22",
    ".mex22",
    ".mx20",
    ".mx18",
    ".mx12",
    ".mx11",
    ".mx5",
    ".mx4",
    ".mx3",
    ".mx2",
    ".m2k",
    ".loa",
    ".sea",
    ".mtr",
    ".mod",
    # NVivo
    ".nvp",
    ".nvpx",
    # ATLAS.ti
    ".atlasproj",
    ".hpr7",
    # QDA Miner
    ".ppj",
    ".pprj",
    ".qlt",
    # f4analyse
    ".f4p",
    # Quirkos
    ".qpd",
}

# ─── Primary Data File Extensions ─────────────────────────────────────────────

PRIMARY_DATA_EXTENSIONS = {
    ".pdf", ".txt", ".rtf", ".docx", ".doc",
    ".odt", ".mp3", ".mp4", ".wav", ".m4a",
    ".jpg", ".jpeg", ".png", ".tiff",
    ".xlsx", ".xls", ".csv",
}

# ─── Open Licenses (accept these) ─────────────────────────────────────────────

OPEN_LICENSES = [
    "cc0",
    "cc by",
    "cc-by",
    "creative commons",
    "public domain",
    "open data commons",
    "odc",
    "pddl",
    "odbl",
    "mit",
    "apache",
    "gpl",
]

# ─── Dryad API ────────────────────────────────────────────────────────────────

DRYAD_API_BASE  = "https://datadryad.org/api/v2"
DRYAD_SEARCH_URL = f"{DRYAD_API_BASE}/search"
DRYAD_PAGE_SIZE  = 20
DRYAD_MAX_PAGES  = 50
DRYAD_API_TOKEN  = "Your_Dryad_API_Token_Here" # Replace with your actual Dryad API token  

# ─── FSD API ──────────────────────────────────────────────────────────────────

FSD_CATALOGUE_URL = "https://services.fsd.tuni.fi/catalogue/index"
FSD_API_BASE      = "https://services.fsd.tuni.fi/api/v0"
FSD_PAGE_SIZE     = 50
FSD_MAX_PAGES     = 30

# ─── Downloader ───────────────────────────────────────────────────────────────

DOWNLOAD_TIMEOUT    = 60      # seconds
DOWNLOAD_CHUNK_SIZE = 8192    # bytes
MAX_FILE_SIZE_MB    = 500     # skip files larger than this
RETRY_ATTEMPTS      = 3       # number of retry attempts per file

# ─── Logging ──────────────────────────────────────────────────────────────────

LOG_FILE = os.path.join(DATA_DIR, "pipeline.log")
