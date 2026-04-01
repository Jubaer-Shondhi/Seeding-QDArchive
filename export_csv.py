"""
export_csv.py — Export all database tables to CSV files for submission.
"""

import csv
import os
import sqlite3
from config import DB_PATH, DATA_DIR


def export_table(table: str, output_path: str):
    """Export a single table to CSV."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
    conn.close()

    if not rows:
        print(f"No data in '{table}' table yet.")
        return 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])

    print(f"Exported {len(rows)} rows from '{table}' to {output_path}")
    return len(rows)


def export_all():
    """Export all 5 tables to CSV."""
    print("\nExporting all tables to CSV...")
    export_table("projects",    os.path.join(DATA_DIR, "projects_export.csv"))
    export_table("files",       os.path.join(DATA_DIR, "files_export.csv"))
    export_table("keywords",    os.path.join(DATA_DIR, "keywords_export.csv"))
    export_table("person_role", os.path.join(DATA_DIR, "person_role_export.csv"))
    export_table("licenses",    os.path.join(DATA_DIR, "licenses_export.csv"))


def export_projects_csv(output_path=None):
    output_path = output_path or os.path.join(DATA_DIR, "projects_export.csv")
    return export_table("projects", output_path)


def export_files_csv():
    return export_table("files", os.path.join(DATA_DIR, "files_export.csv"))


def print_stats():
    """Print summary statistics."""
    from database import get_stats
    stats = get_stats()
    print("\n" + "="*50)
    print("  QDArchive Pipeline — Database Stats")
    print("="*50)
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
    print("="*50 + "\n")


if __name__ == "__main__":
    export_all()
    print_stats()
