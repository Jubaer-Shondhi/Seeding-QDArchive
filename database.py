import csv
import os
import re
import sqlite3
import logging
from config import DB_PATH, DATA_DIR

logger = logging.getLogger(__name__)

# ── Repository info ───────────────────────────────────────────────────────────
REPOSITORY_IDS = {
    "Dryad": 2,
    "FSD":   11,
}

REPOSITORY_URLS = {
    "Dryad": "https://datadryad.org",
    "FSD":   "https://www.fsd.tuni.fi",
}

# ── DOWNLOAD_RESULT enum values ───────────────────────────────────────────────
SUCCEEDED                  = "SUCCEEDED"
FAILED_SERVER_UNRESPONSIVE = "FAILED_SERVER_UNRESPONSIVE"
FAILED_LOGIN_REQUIRED      = "FAILED_LOGIN_REQUIRED"
FAILED_TOO_LARGE           = "FAILED_TOO_LARGE"


def get_connection():
    """Return a SQLite connection with row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables matching schema exactly."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── PROJECTS ──────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id                         INTEGER PRIMARY KEY AUTOINCREMENT,
            query_string               TEXT,
            repository_id              INTEGER NOT NULL,
            repository_url             TEXT NOT NULL,
            project_url                TEXT,
            version                    TEXT,
            title                      TEXT,
            description                TEXT,
            language                   TEXT,
            doi                        TEXT,
            upload_date                TEXT,
            download_date              TEXT,
            download_repository_folder TEXT,
            download_project_folder    TEXT,
            download_version_folder    TEXT,
            download_method            TEXT DEFAULT 'API-CALL'
        )
    """)

    # ── FILES ─────────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            file_name  TEXT NOT NULL,
            file_type  TEXT,
            status     TEXT DEFAULT 'FAILED_SERVER_UNRESPONSIVE'
        )
    """)

    # ── KEYWORDS ──────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            keyword    TEXT NOT NULL
        )
    """)

    # ── PERSON_ROLE ───────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS person_role (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            name       TEXT NOT NULL,
            role       TEXT NOT NULL DEFAULT 'UNKNOWN'
        )
    """)

    # ── LICENSES ──────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS licenses (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            license    TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()
    logger.info("Database initialized at %s", DB_PATH)


def insert_project(data: dict) -> int:
    """Insert a project and its keywords, authors and license. Returns project id."""
    conn = get_connection()
    cur  = conn.cursor()

    source   = data.get("source", "")
    repo_id  = REPOSITORY_IDS.get(source, 0)
    repo_url = REPOSITORY_URLS.get(source, "")
    doi      = data.get("doi") or ""

    project_folder = (
        data.get("study_number")
        or (doi.split("/")[-1] if doi else "")
        or ""
    )

    cur.execute("""
        INSERT INTO projects (
            query_string, repository_id, repository_url,
            project_url, version, title, description,
            language, doi, upload_date, download_date,
            download_repository_folder, download_project_folder,
            download_version_folder, download_method
        ) VALUES (
            :query_string, :repository_id, :repository_url,
            :project_url, :version, :title, :description,
            :language, :doi, :upload_date, datetime('now'),
            :download_repository_folder, :download_project_folder,
            :download_version_folder, :download_method
        )
    """, {
        "query_string":               data.get("query_string", ""),
        "repository_id":              repo_id,
        "repository_url":             repo_url,
        "project_url":                data.get("source_url") or data.get("project_url", ""),
        "version":                    data.get("version", ""),
        "title":                      data.get("project_title") or data.get("title", ""),
        "description":                data.get("project_description") or data.get("description", ""),
        "language":                   data.get("language", ""),
        "doi":                        doi or None,
        "upload_date":                data.get("publication_date", ""),
        "download_repository_folder": source.lower(),
        "download_project_folder":    project_folder,
        "download_version_folder":    data.get("version", ""),
        "download_method":            data.get("download_method", "API-CALL"),
    })
    project_id = cur.lastrowid

    # Keywords
    for kw in str(data.get("keywords", "")).split(";"):
        kw = kw.strip()
        if kw:
            cur.execute(
                "INSERT INTO keywords (project_id, keyword) VALUES (?, ?)",
                (project_id, kw)
            )

    # Authors
    for author in str(data.get("authors", "")).split(";"):
        author = author.strip()
        if author:
            cur.execute(
                "INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)",
                (project_id, author, "AUTHOR")
            )

    # License
    license_raw = data.get("license", "")
    if license_raw:
        cur.execute(
            "INSERT INTO licenses (project_id, license) VALUES (?, ?)",
            (project_id, _normalize_license(license_raw))
        )

    conn.commit()
    conn.close()
    return project_id


def insert_file(data: dict) -> int:
    """Insert a file record. Returns file id."""
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO files (project_id, file_name, file_type, status)
        VALUES (:project_id, :file_name, :file_type, :status)
    """, {
        "project_id": data.get("project_id"),
        "file_name":  data.get("file_name", ""),
        "file_type":  data.get("file_type", ""),
        "status":     data.get("status", FAILED_SERVER_UNRESPONSIVE),
    })
    file_id = cur.lastrowid
    conn.commit()
    conn.close()
    return file_id


def update_file_status(file_id: int, status: str):
    """Update download status of a file."""
    conn = get_connection()
    conn.execute("UPDATE files SET status = ? WHERE id = ?", (status, file_id))
    conn.commit()
    conn.close()


def get_pending_files():
    """Return files that still need downloading."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT f.id, f.file_name, f.file_type, f.status,
               p.id as project_id, p.repository_url,
               p.download_repository_folder, p.download_project_folder,
               p.project_url
        FROM files f
        JOIN projects p ON f.project_id = p.id
        WHERE f.status != 'SUCCEEDED'
        AND   f.status != 'FAILED_LOGIN_REQUIRED'
        AND   f.status != 'FAILED_TOO_LARGE'
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats() -> dict:
    """Return summary statistics from the database."""
    conn = get_connection()
    stats = {
        "total_projects": conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0],
        "total_files":    conn.execute("SELECT COUNT(*) FROM files").fetchone()[0],
        "succeeded":      conn.execute("SELECT COUNT(*) FROM files WHERE status='SUCCEEDED'").fetchone()[0],
        "failed_server":  conn.execute("SELECT COUNT(*) FROM files WHERE status='FAILED_SERVER_UNRESPONSIVE'").fetchone()[0],
        "failed_login":   conn.execute("SELECT COUNT(*) FROM files WHERE status='FAILED_LOGIN_REQUIRED'").fetchone()[0],
        "failed_large":   conn.execute("SELECT COUNT(*) FROM files WHERE status='FAILED_TOO_LARGE'").fetchone()[0],
        "total_keywords": conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0],
        "total_persons":  conn.execute("SELECT COUNT(*) FROM person_role").fetchone()[0],
        "total_licenses": conn.execute("SELECT COUNT(*) FROM licenses").fetchone()[0],
        "by_source":      {},
    }
    for row in conn.execute("SELECT repository_url, COUNT(*) FROM projects GROUP BY repository_url"):
        stats["by_source"][row[0]] = row[1]
    conn.close()
    return stats


def export_all():
    """Export all 5 tables to CSV files."""
    print("\nExporting all tables to CSV...")
    _export_table("projects",    os.path.join(DATA_DIR, "projects_export.csv"))
    _export_table("files",       os.path.join(DATA_DIR, "files_export.csv"))
    _export_table("keywords",    os.path.join(DATA_DIR, "keywords_export.csv"))
    _export_table("person_role", os.path.join(DATA_DIR, "person_role_export.csv"))
    _export_table("licenses",    os.path.join(DATA_DIR, "licenses_export.csv"))


def print_stats():
    """Print summary statistics."""
    stats = get_stats()
    print("\n" + "=" * 50)
    print("  QDArchive Pipeline — Database Stats")
    print("=" * 50)
    print(f"  Total projects  : {stats['total_projects']}")
    print(f"  Total files     : {stats['total_files']}")
    print(f"  SUCCEEDED       : {stats['succeeded']}")
    print(f"  FAILED (server) : {stats['failed_server']}")
    print(f"  FAILED (login)  : {stats['failed_login']}")
    print(f"  FAILED (large)  : {stats['failed_large']}")
    print(f"  Total keywords  : {stats['total_keywords']}")
    print(f"  Total persons   : {stats['total_persons']}")
    print(f"  Total licenses  : {stats['total_licenses']}")
    print("\n  By repository:")
    for src, cnt in stats["by_source"].items():
        print(f"    {src:<40} {cnt}")
    print("=" * 50 + "\n")


def _export_table(table: str, output_path: str):
    """Export a single table to CSV."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
    conn.close()
    if not rows:
        print(f"No data in '{table}' table yet.")
        return
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])
    print(f"Exported {len(rows)} rows from '{table}' to {output_path}")


def _normalize_license(license_str: str) -> str:
    """Normalize license in standard format."""
    s = license_str.lower().strip()

    def version(text):
        m = re.search(r'(\d+\.\d+)', text)
        return f" {m.group(1)}" if m else ""

    if "cc0" in s or "public domain" in s:      return "CC0"
    elif "cc by-nc-nd" in s:                    return f"CC BY-NC-ND{version(s)}"
    elif "cc by-nc-sa" in s:                    return f"CC BY-NC-SA{version(s)}"
    elif "cc by-nc"    in s:                    return f"CC BY-NC{version(s)}"
    elif "cc by-nd"    in s:                    return f"CC BY-ND{version(s)}"
    elif "cc by-sa"    in s:                    return f"CC BY-SA{version(s)}"
    elif "cc by"       in s or "cc-by" in s:    return f"CC BY{version(s)}"
    elif "odbl"        in s:                    return "ODbL"
    elif "odc-by"      in s:                    return "ODC-By"
    elif "pddl"        in s:                    return "PDDL"
    else:                                       return license_str
