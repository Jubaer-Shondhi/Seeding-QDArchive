# QDArchive Seeding Pipeline - Part 1: Acquisition

## Project Overview
This pipeline collects qualitative research projects from open data repositories for the QDArchive project. It scrapes metadata, downloads available files, and stores everything in a structured SQLite database.

## Repository Coverage
- **Dryad (Repository #2)**: Open data publishing platform
- **FSD (Repository #11)**: Finnish Social Science Data Archive

## Query Strategy

Following the project's recommendation to use file extension queries:

### QDA File Extension Queries (Primary)
- `qdpx`
- `mqda`
- `nvivo`, `nvp`
- `atlasti`, `atlasproj`
- `f4p`
- `qlt`, `ppj`
- `qpd`

### Broader Qualitative Research Queries (Secondary)
- `qualitative research`, `interview study`
- `interview transcript`, `thematic analysis`
- `grounded theory`, `qualitative data analysis`

## Getting Started

### Clone the Repository
```bash
git clone https://github.com/Jubaer-Shondhi/Seeding-QDArchive.git
cd Seeding-QDArchive
```

### Set Up Virtual Environment (Recommended)
```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On Mac/Linux:
source venv/bin/activate
```

## Project Structure

```
Seeding-QDArchive/
├── scrapers/
│   ├── __init__.py
│   ├── base_scraper.py     # Abstract base class for all scrapers
│   ├── dryad_scraper.py    # Scraper for Dryad (repo #2)
│   └── fsd_scraper.py      # Scraper for FSD Finland (repo #11)
├── 23453618-seeding.db     # DB file
├── config.py               # Configuration (paths, API keys, search terms)
├── database.py             # SQLite database setup and helpers
├── downloader.py           # File downloader with resume support
├── pipeline.py             # Main pipeline orchestrator
├── export_csv.py           # Export database to CSV
├── README.md               # This file
├── .gitignore
└── requirements.txt        # Python dependencies
```
## Requirements

- **Python 3.8 or higher** (tested with Python 3.8.1)
- Dependencies listed in `requirements.txt`

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

### Dryad API Token

The Dryad scraper requires an API token for authentication. Follow these steps to obtain one:

1. **Go to** https://datadryad.org/
2. **Click "Login"** and log in with your ORCID iD (or create an account first)
3. **Enter your password** when prompted
4. **After login**, click on the **person icon** in the top-right corner
5. **Select "My Account"** from the dropdown menu
6. **Click "Get API Token"** in your profile settings
7. **Copy the token** (it will only be shown once)

### Set the Token

**Option 1: Edit config.py (simple)**
```python
# In config.py, replace the placeholder with your actual token:
DRYAD_API_TOKEN  = "Your_Dryad_API_Token_Here" # Replace with your actual Dryad API token  
```

## Running the Pipeline

### Important Note Before Running

The pipeline **appends** data to existing files rather than overwriting them. Running multiple times without cleaning will create duplicate records.

If you want a **fresh start** (clean database and new CSV exports), delete the existing data files first:

```bash
# Delete existing database and CSV exports
rm -f data/archive.db
rm -f data/*.csv

# Or on Windows:
del data\archive.db
del data\*.csv
```

```bash
# Run only Dryad files download (requires valid API token)
python pipeline.py --source dryad
```

```bash
# Run only Dryad (metadata only, no downloads)
python pipeline.py --source dryad --no-download
```

```bash
# Run only FSD (files download)
python pipeline.py --source fsd
```

```bash
# Run only FSD (metadata only, no downloads)
python pipeline.py --source fsd --no-download
```

```bash
# View database statistics
python pipeline.py --stats
```

```bash
# Export CSVs only
python pipeline.py --export
```

## Results

### FSD Repository

- **Projects**: 402 qualitative datasets
- **Level A (Open Access)**: 7 projects and files identified
- **Metadata**: Complete (titles, descriptions, authors, keywords, licenses, etc.)

### Dryad Repository

- **Projects**: 247
- **Downloads**: 1096 files
- **Metadata**: Complete (titles, descriptions, authors, keywords, licenses, etc.)

## Output and Data Location

After running the pipeline, the following files and folders will be created:

- **Database**: `data/archive.db` - Contains all project metadata in SQLite format
- **CSV Exports**: `data/*.csv` - All 5 tables exported for easy viewing
- **Downloaded Files**: `data/files/` - Contains subfolders for each repository:
  - `data/files/Dryad/` - Downloaded files from Dryad (with also empty files/folders, reason is given in archive.db file)
  - `data/files/FSD/` - Downloaded files from FSD (with also empty files/folders, reason is given in archive.db file)

**Note**: The `data/` folder is excluded from Git due to size (>6 GB). See [Submission Contents](#submission-contents) for access to the complete data.

### Project Structure
```
Seeding-QDArchive/
├── data/
│   ├── files
│   │   ├── Dryad
│   │   │   ├── D9402J
│   │   │   ├── dryad.0cfxpnwfq
│   │   │   └── ....
│   │   └── FSD
│   │       ├── FSD1249
│   │       ├── FSD1251
│   │       └── ....
│   ├── archive.db
│   ├── files_export.csv
│   ├── keywords_export.csv
│   ├── licenses_export.csv
│   ├── person_role_export.csv
│   └── projects_export.csv
├── scrapers/
│   ├── __init__.py
│   ├── base_scraper.py     # Abstract base class for all scrapers
│   ├── dryad_scraper.py    # Scraper for Dryad (repo #2)
│   └── fsd_scraper.py      # Scraper for FSD Finland (repo #11)
├── config.py               # Configuration (paths, API keys, search terms)
├── database.py             # SQLite database setup and helpers
├── downloader.py           # File downloader with resume support
├── pipeline.py             # Main pipeline orchestrator
├── export_csv.py           # Export database to CSV
├── README.md               # This file
├── .gitignore
└── requirements.txt        # Python dependencies
```

## Technical Problems/Limitations and Solutions

### Dryad Repository

- **API Token Expiration**: The Dryad API token is valid for only 10 hours. After running the pipeline for 10 hours, the token expired, interrupting downloads. Metadata was successfully collected before expiration, but file downloads were incomplete. Future runs require generating a new token.

- **API Token Authentication**: Dryad API requires a valid API token for authenticated access. Without a proper token, downloads fail with 401 Unauthorized errors.

- **Rate Limiting**: Dryad enforces rate limits on API requests. The pipeline implements delays between requests to avoid being blocked. During execution, **429 (Too Many Requests)** errors were observed when requests were made too frequently. 
  - **Solution Implemented**: 
    - Added a 3-second delay (`DOWNLOAD_DELAY`) between download attempts
    - Implemented exponential backoff in the downloader: when 429 errors occur, the pipeline waits progressively longer (30-300 seconds) before retrying
    - Added retry logic with up to 3 attempts per request
    - These measures successfully reduced rate limit violations and improved download reliability

- **Download URLs**: File download URLs from Dryad API sometimes redirect or require specific headers. The downloader handles redirects but some files may still fail if authentication is insufficient.

### FSD Repository

- **OAI-PMH Harvesting**: FSD metadata was harvested using the OAI-PMH (Open Archives Initiative Protocol for Metadata Harvesting) protocol, which provides structured XML metadata for all 402 qualitative datasets. The endpoint used was `https://services.fsd.tuni.fi/v0/oai` with the set parameter `data_kind:Qualitative`.

- **Level A Detection**: Initial scraping of Level A study numbers failed due to incorrect URL pattern and HTML parsing. The regex pattern `/catalogue/(FSD\d+)\?` did not match FSD's actual HTML structure. Fixed by updating the URL to the correct catalogue endpoint and using broader regex pattern `FSD\d+` to find study numbers anywhere in the HTML.

- **Access Credentials for Advanced Levels Data**: FSD Level A datasets are openly available without login. However, more advanced levels (B, C, and D) require credentials. An attempt was made to obtain credentials using institutional email through Aila (FSD's customer service) for implementing automated downloads. A response was received by email from them, but credentials were not provided.

- **ZIP Download Failure**: The actual files for **Level A** projects could not be downloaded programmatically because:
  - FSD requires users to click an "Download data" button before download
  - The download is triggered via a form POST with CSRF tokens (already tried)
  - Session cookies are required and cannot be easily replicated with requests library
  - Multiple URL patterns were tested (`/catalogue/{study_number}/download`, `/catalogue/export/{study_number}`, `/catalogue/download.php?study={study_number}`) but all returned 404 or HTML login pages
  - The download process is designed for browser interaction, not API access

- **Manual Download for Level A Projects**: Since automated download was not possible, the 7 "Level A" qualitative datasets were downloaded manually from the FSD website and placed in the corresponding project folders. These files are included in the submission.

- **Automated Download Alternatives**: Research indicated that FSD downloads can be automated using browser automation frameworks like **Playwright**, which can handle the terms acceptance, download buttons and session management. However, this still requires valid login credentials and was not implemented due to the lack of credentials.

- **Metadata XML Files**: Initially, the pipeline attempted to download DDI XML files for all 402 projects. These were being saved as HTML login pages and incorrectly marked as SUCCEEDED. Fixed by removing XML downloads entirely and only recording ZIP file entries with proper status.

## Part 1 Submission

This pipeline completes all Part 1 requirements:

- Find qualitative research projects from repositories
- Download files and metadata
- Store metadata in SQLite database following recommended schema
- Export to CSV format
- **Git tag `part-1-release` created**

## Submission Contents

### Database & CSV Exports

All metadata and database files are available in the FAUbox folder and can easily be downloaded:

- **Database**: `archive.db` - Complete SQLite database with all project metadata
- **CSV Exports**: All 5 tables exported for easy review
  - `projects_export.csv`
  - `files_export.csv`
  - `keywords_export.csv`
  - `person_role_export.csv`
  - `licenses_export.csv`

**FAUbox Link**: https://faubox.rrze.uni-erlangen.de/getlink/fi21EF1g2h2wXo6K5qLD2S/data
*(FAU login required)*

**Folder Structure**:
```
data/
 ├── files
 │   ├── Dryad
 │   │   ├── D9402J
 │   │   ├── dryad.0cfxpnwfq
 │   │   └── ....
 │   └── FSD
 │       ├── FSD1249
 │       ├── FSD1251
 │       └── ....
 ├── 23453618-seeding.db
 ├── files_export.csv
 ├── keywords_export.csv
 ├── licenses_export.csv
 ├── person_role_export.csv
 └── projects_export.csv
```

### What's Included

- Complete metadata for **402 FSD projects** + **247 Dryad projects**
- All 5 database tables exported as CSV
- All files (1,096 Dryad + 7 FSD)
- Complete source code in this repository

**Note**: The data/files folder and csv files are not included in the repository due to size (more than 6 GB). 

## License

This project is licensed under the **MIT License**. The project is part of the QDArchive seeding effort at FAU Erlangen.