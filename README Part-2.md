# QDArchive Seeding Pipeline - Part 2: Data Classification

## Project Overview
This pipeline classifies qualitative research projects collected from open data repositories (Dryad and FSD) using the **ISIC Rev. 5 taxonomy**. Projects are classified into:

- **Project Types**: QDA_PROJECT, QD_PROJECT, OTHER_PROJECT, NOT_A_PROJECT
- **ISIC Divisions**: 2 levels deep (Section + Division)

**Part 2 builds upon Part 1** — it uses the metadata already collected and stored in the SQLite database.

---

## Repository Coverage
- **Dryad (Repository #2)**: Scientific data repository
- **FSD (Repository #11)**: Finnish Social Science Data Archive

---

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
├── classification_output/  # Part 2 reports and histograms   
│   ├── classification_report.csv
│   ├── 23453618-sq26-classification.xlsx
│   ├── statistics_report.txt
│   ├── top20_classes.csv
│   ├── histogram_dryad.svg
│   ├── histogram_dryad.png
│   ├── histogram_fsd.svg
│   └── histogram_fsd.png
├── 23453618-seeding.db     # DB file
├── 23453618-sq26-classification.db # Part 2 database
├── config.py               # Configuration (paths, API keys, search terms)
├── database.py             # SQLite database setup and helpers
├── downloader.py           # File downloader with resume support
├── pipeline.py             # Main pipeline orchestrator
├── export_csv.py           # Export database to CSV
├── classifier.py           # Rule-based classifier
├── generate_reports.py     # Report generation script
├── README_PART2.md         # This file
├── README.md               # Part-1 README.md
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

## Running the Pipeline

### Important Note Before Running

The classifier **uses the existing Part 1 database** (`23453618-seeding.db`) as input and **adds new columns** (`project_type`, `isic_code`, `isic_division_code`, `isic_division_name`) to it. If this file is not present in the root folder, you need to run the Part 1 pipeline first.

If you want to **run the classifier from scratch** (fresh classification):

1. **Start with the Part 1 database**: Ensure `23453618-seeding.db` is present in the root folder.
2. **Remove old classification outputs** (if any):
   ```bash
   rm -f 23453618-sq26-classification.db
   rm -rf classification_output/
   ```

### Step 1: Run the Classifier

```bash
python classifier.py
```

This will:
- Add project_type and ISIC columns to the database
- Classify each project into one of four project types
- Assign an ISIC Rev. 5 division (2 levels deep) to each project

### Step 2: Generate Reports

```bash
python generate_reports.py
```

This will generate:
- classification_report.csv — Full classification table
- 23453618-classification-table.xlsx — XLSX table for submission
- statistics_report.txt — Detailed statistics
- top20_classes.csv — Top 20 ISIC divisions per repository
- Histograms (SVG + PNG) for Dryad and FSD

## Classification Methodology

### Project Type Classification

| Rule | Criteria | Project Type |
|------|----------|--------------|
| 1 | Has QDA analysis file (`.qdpx`, `.mx24`, etc.) | QDA_PROJECT |
| 2 | Has qualitative data file (`.pdf`, `.mp3`, etc.) OR qualitative keywords in metadata | QD_PROJECT |
| 3 | No title, description, or files | NOT_A_PROJECT |
| 4 | None of the above | OTHER_PROJECT |

### ISIC Rev. 5 Classification

Projects are classified into ISIC Rev. 5 divisions (2 levels deep) using a rule-based keyword matching approach. The classifier matches keywords from project titles, descriptions, and metadata against 72 ISIC divisions relevant to qualitative research.

## Results

### Project Type Distribution (Overall)

| Project Type | Count | Percentage |
|--------------|-------|------------|
| QD_PROJECT | 526 | 81.0% |
| OTHER_PROJECT | 121 | 18.6% |
| QDA_PROJECT | 2 | 0.3% |
| NOT_A_PROJECT | 0 | 0% |
| **Total** | **649** | **100%** |

### Project Type Distribution by Repository

#### Dryad Repository

| Project Type | Count | Percentage |
|--------------|-------|------------|
| QD_PROJECT | 244 | 98.8% |
| OTHER_PROJECT | 1 | 0.4% |
| QDA_PROJECT | 2 | 0.8% |
| NOT_A_PROJECT | 0 | 0% |
| **Total** | **247** | **100%** |

#### FSD Repository

| Project Type | Count | Percentage |
|--------------|-------|------------|
| QD_PROJECT | 282 | 70.1% |
| OTHER_PROJECT | 120 | 29.9% |
| QDA_PROJECT | 0 | 0% |
| NOT_A_PROJECT | 0 | 0% |
| **Total** | **402** | **100%** |

---

### Top 5 ISIC Divisions (Overall)

| Rank | ISIC Code | Division Name | Count |
|------|-----------|---------------|-------|
| 1 | N72 | Scientific research and development | 231 |
| 2 | Q85 | Education | 103 |
| 3 | R86 | Human health activities | 62 |
| 4 | B07 | Mining of metal ores | 46 |
| 5 | K62 | Computer programming, consultancy | 31 |

### Top 5 ISIC Divisions by Repository

#### Dryad Repository

| Rank | ISIC Code | Division Name | Count |
|------|-----------|---------------|-------|
| 1 | R86 | Human health activities | 47 |
| 2 | B07 | Mining of metal ores | 45 |
| 3 | N72 | Scientific research and development | 29 |
| 4 | K62 | Computer programming, consultancy | 27 |
| 5 | A01 | Crop and animal production | 20 |

#### FSD Repository

| Rank | ISIC Code | Division Name | Count |
|------|-----------|---------------|-------|
| 1 | N72 | Scientific research and development | 202 |
| 2 | Q85 | Education | 91 |
| 3 | S91 | Library, archives, museum and other cultural activities | 36 |
| 4 | R86 | Human health activities | 15 |
| 5 | R88 | Social work activities without accommodation | 9 |

---

### Most Common ISIC Class

**Overall**: N72 - Scientific research and development (231 projects)

**By Repository**:
- **Dryad**: R86 - Human health activities (47 projects)
- **FSD**: N72 - Scientific research and development (202 projects)

## Technical Problems/Limitations and Solutions

### FSD Repository

- **Finnish Language Metadata**: FSD metadata is primarily in Finnish, while the initial classifier used only English keywords. This resulted in many FSD projects being misclassified as OTHER_PROJECT instead of QD_PROJECT.
  - **Solution**: Added Finnish keywords to both `QUALITATIVE_KEYWORDS` and `ISIC_MAPPING` lists (e.g., `laadullinen`, `haastattelu`, `koulutus`, etc.). This increased FSD QD_PROJECT classification from 253 to 282 projects.

- **Download Limitations**: Only 7 Level A FSD datasets were downloadable; the remaining 395 projects (Levels B, C, D) could not be accessed due to authentication and terms acceptance requirements.
  - **Solution**: All 402 FSD projects were classified using their metadata (titles, descriptions, and keywords) under the Tier-1 approach, which was sufficient for project type and ISIC classification.

### Dryad Repository

- **Misclassification Due to "Data" Keyword**: Many Dryad project titles start with "Data from: ...", causing them to be misclassified under K63 (Computing infrastructure and data processing) instead of their actual research topics.
  - **Solution**: Removed the keyword "data" from the K63 ISIC mapping. This resulted in K63 dropping from #1 (111 projects) to #4, and R86 (Human health activities) becoming the most common class with 47 projects.

### ISIC Classification

- **Keyword Matching Limitations**: The rule-based approach relies on keyword presence in metadata. Projects without clear keywords may be misclassified or left unclassified.
  - **Solution**: The keyword list was carefully curated based on ISIC Rev. 5 division descriptions and reviewed for relevance to qualitative research. The keyword lists are bilingual (English + Finnish) to handle both repository languages.

- **Division Coverage**: The classifier covers 72 ISIC divisions most relevant to qualitative research. Projects outside these divisions are left unclassified.
  - **Solution**: The 72 divisions were selected based on their relevance to typical qualitative research topics (education, health, social sciences, etc.). The classifier intentionally focuses on these areas to minimize false positives.

### Project Type Classification

- **QDA_PROJECT Detection**: Projects are classified as QDA_PROJECT only if they contain explicit QDA file extensions (e.g., `.qdpx`, `.mx24`). Some projects may contain QDA data within other file formats (e.g., embedded in PDFs) which are not detected.
  - **Solution**: The classifier prioritizes explicit QDA file extensions as the most reliable indicator of QDA_PROJECT status. Additional file formats can be added to the `QDA_EXTENSIONS` set if identified.

- **Metadata Quality**: The accuracy of classification depends on the quality and completeness of metadata collected in Part 1. Incomplete or inconsistent metadata may affect classification results.
  - **Solution**: The metadata collection process in Part 1 was designed to capture as much information as possible. Incomplete fields are handled gracefully by the classifier.

- **Subjectivity in ISIC Mapping**: The mapping of research topics to ISIC divisions involves some subjectivity. Different classifiers might assign different ISIC codes to the same project based on interpretation of keywords.
  - **Solution**: The keyword-to-ISIC mapping is based on official ISIC Rev. 5 division descriptions. While some subjectivity is unavoidable, this approach ensures consistency and transparency in classification decisions.

- **Secondary ISIC Classification**: Projects with a clear second research topic were assigned a secondary ISIC division. This was done using a threshold-based approach (secondary score ≥ 50% of primary score) to avoid weak or meaningless secondary assignments.
  - **Solution**: The threshold ensures that only meaningful secondary topics are recorded, as per the professor's requirement "secondary_class // if any".

## Part 2 Submission

This pipeline completes all Part 2 requirements:

- Classify projects into project types (QDA_PROJECT, QD_PROJECT, OTHER_PROJECT, NOT_A_PROJECT)
- Classify projects into ISIC Rev. 5 divisions (2 levels deep)
- Generate histograms and top-20 lists per repository
- SQLite file named 23453618-sq26-classification.db in root folder
- Git tag classification-results created
- Reports saved in classification_output/

## Submission Contents

### Database

The classification SQLite database file is located in the root of this repository:

- **Database**: `23453618-sq26-classification.db` — Complete database with project_type and ISIC columns

**Reports**:

All reports are saved in the classification_output/ folder:

| File | Description |
|------|-------------|
| `classification_report.csv` | Full classification table |
| `23453618-classification-table.xlsx` | XLSX table for submission |
| `statistics_report.txt` | Detailed statistics |
| `top20_classes.csv` | Top 20 ISIC divisions per repository |
| `histogram_dryad.svg` / `histogram_dryad.png` | Dryad histogram |
| `histogram_fsd.svg` / `histogram_fsd.png` | FSD histogram |

### What's Included

- Classification results for 649 projects **(402 FSD projects** + **247 Dryad projects)**
- All reports and histograms
- Complete source code in this repository

## License

This project is licensed under the **MIT License**. The project is part of the QDArchive seeding effort at FAU Erlangen.