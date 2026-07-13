"""
generate_reports.py - Generate all reports for Part 2 submission
"""

import sqlite3
import csv
from pathlib import Path
import matplotlib.pyplot as plt

# Configuration
DB_PATH = Path("23453618-seeding.db")
OUTPUT_DIR = Path("classification_output")
OUTPUT_DIR.mkdir(exist_ok=True)


def get_connection():
    return sqlite3.connect(DB_PATH)


def generate_classification_report():
    """Generate CSV report for XLSX submission"""
    conn = get_connection()
    
    cursor = conn.execute("""
        SELECT 
            p.repository_id,
            p.project_type,
            p.title,
            p.isic_code as primary_class,
            p.secondary_isic_code as secondary_class,
            (SELECT COUNT(*) FROM files f WHERE f.project_id = p.id) as no_project_files
        FROM projects p
        WHERE p.project_type IS NOT NULL
        ORDER BY p.repository_id, p.id
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    csv_path = OUTPUT_DIR / "classification_report.csv"
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['repository_id', 'project_type', 'project_title', 'primary_class', 'secondary_class', 'no_project_files'])
        for row in rows:
            writer.writerow([row[0], row[1], row[2], row[3], row[4] if row[4] else '', row[5]])
    print(f"CSV report saved: {csv_path}")


def generate_xlsx():
    """Generate XLSX file from classification report"""
    try:
        import pandas as pd
    except ImportError:
        print("pandas not installed. Run: pip install pandas openpyxl")
        return
    
    csv_path = OUTPUT_DIR / "classification_report.csv"
    xlsx_path = OUTPUT_DIR / "23453618-sq26-classification.xlsx"
    
    df = pd.read_csv(csv_path)
    df.to_excel(xlsx_path, index=False, sheet_name="Classification")
    print(f"XLSX table saved: {xlsx_path}")
    print(f"Total rows: {len(df)}")


def generate_statistics_report():
    """Generate statistics report with repository-wise and total ISIC"""
    conn = get_connection()
    
    stats_path = OUTPUT_DIR / "statistics_report.txt"
    with open(stats_path, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("PART 2: CLASSIFICATION STATISTICS REPORT\n")
        f.write("=" * 70 + "\n\n")
        
        # Project type distribution
        f.write("PROJECT TYPE DISTRIBUTION (All Repositories):\n")
        f.write("-" * 50 + "\n")
        cursor = conn.execute("SELECT project_type, COUNT(*) FROM projects GROUP BY project_type")
        for row in cursor:
            f.write(f"  {row[0]}: {row[1]} projects\n")
        
        total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        f.write(f"\n  TOTAL: {total} projects\n\n")
        
        # ISIC distribution (All repositories)
        f.write("=" * 70 + "\n")
        f.write("ISIC DIVISION DISTRIBUTION (All Repositories):\n")
        f.write("-" * 50 + "\n")
        cursor = conn.execute("""
            SELECT isic_code, isic_division_name, COUNT(*) as count
            FROM projects
            WHERE isic_code IS NOT NULL
            GROUP BY isic_code, isic_division_name
            ORDER BY COUNT(*) DESC
        """)
        total_isic = 0
        for row in cursor:
            f.write(f"  {row[0]} - {row[1]}: {row[2]} projects\n")
            total_isic += row[2]
        f.write(f"\n  TOTAL projects with ISIC: {total_isic}\n\n")
        
        # Secondary ISIC distribution
        f.write("=" * 70 + "\n")
        f.write("SECONDARY ISIC DISTRIBUTION (All Repositories):\n")
        f.write("-" * 50 + "\n")
        cursor = conn.execute("""
            SELECT secondary_isic_code, secondary_isic_division_name, COUNT(*) as count
            FROM projects
            WHERE secondary_isic_code IS NOT NULL
            GROUP BY secondary_isic_code, secondary_isic_division_name
            ORDER BY COUNT(*) DESC
        """)
        total_secondary = 0
        for row in cursor:
            f.write(f"  {row[0]} - {row[1]}: {row[2]} projects\n")
            total_secondary += row[2]
        f.write(f"\n  TOTAL projects with secondary ISIC: {total_secondary}\n\n")
        
        # Repository-wise ISIC distribution
        f.write("=" * 70 + "\n")
        f.write("ISIC DISTRIBUTION BY REPOSITORY:\n")
        f.write("-" * 50 + "\n")
        
        repos = conn.execute("""
            SELECT DISTINCT 
                CASE WHEN repository_url LIKE '%dryad%' THEN 'Dryad' ELSE 'FSD' END as repo,
                repository_id
            FROM projects
        """).fetchall()
        
        for repo_name, repo_id in repos:
            f.write(f"\n  {repo_name} Repository:\n")
            cursor = conn.execute("""
                SELECT isic_code, isic_division_name, COUNT(*) as count
                FROM projects
                WHERE repository_id = ? AND isic_code IS NOT NULL
                GROUP BY isic_code, isic_division_name
                ORDER BY COUNT(*) DESC
            """, (repo_id,))
            
            rows = cursor.fetchall()
            if rows:
                for code, name, count in rows:
                    f.write(f"    {code} - {name}: {count} projects\n")
                repo_total = sum(row[2] for row in rows)
                f.write(f"    Total: {repo_total} projects with ISIC\n")
            else:
                f.write("    No ISIC data found\n")
        
        # Most common class
        f.write("\n" + "=" * 70 + "\n")
        f.write("MOST COMMON CLASS (All Repositories):\n")
        f.write("-" * 50 + "\n")
        cursor = conn.execute("""
            SELECT isic_code, isic_division_name, COUNT(*) as count
            FROM projects
            WHERE isic_code IS NOT NULL
            GROUP BY isic_code, isic_division_name
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            f.write(f"  {row[0]} - {row[1]}: {row[2]} projects\n")
        
        f.write("\n" + "=" * 70 + "\n")
    
    conn.close()
    print(f"Statistics report saved: {stats_path}")


def generate_top20_csv():
    """Generate top 20 classes CSV"""
    conn = get_connection()
    
    top20_path = OUTPUT_DIR / "top20_classes.csv"
    with open(top20_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['rank', 'isic_code', 'class_name', 'count'])
        cursor = conn.execute("""
            SELECT isic_code, isic_division_name, COUNT(*) as count
            FROM projects
            WHERE isic_code IS NOT NULL
            GROUP BY isic_code, isic_division_name
            ORDER BY COUNT(*) DESC
            LIMIT 20
        """)
        for i, row in enumerate(cursor, 1):
            writer.writerow([i, row[0], row[1], row[2]])
    print(f"Top 20 classes saved: {top20_path}")
    conn.close()


def generate_histograms():
    """Generate histograms per repository"""
    conn = get_connection()
    
    repos = conn.execute("""
        SELECT DISTINCT 
            CASE WHEN repository_url LIKE '%dryad%' THEN 'Dryad' ELSE 'FSD' END as repo,
            repository_id
        FROM projects
    """).fetchall()
    
    for repo_name, repo_id in repos:
        cursor = conn.execute("""
            SELECT isic_code, isic_division_name, COUNT(*) as count
            FROM projects
            WHERE repository_id = ? AND isic_code IS NOT NULL
            GROUP BY isic_code
            ORDER BY COUNT(*) DESC
            LIMIT 20
        """, (repo_id,))
        
        rows = cursor.fetchall()
        
        if not rows:
            print(f"No ISIC data for {repo_name}")
            continue
        
        # Reverse for display (largest at top)
        rows = rows[::-1]
        labels = [f"{row[0]} - {row[1]}" for row in rows]
        counts = [row[2] for row in rows]
        
        fig, ax = plt.subplots(figsize=(14, max(8, len(labels) * 0.5)))
        bars = ax.barh(labels, counts, color='steelblue', edgecolor='black')
        
        for bar, count in zip(bars, counts):
            ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                    str(count), ha='left', va='center', fontsize=9, fontweight='bold')
        
        ax.set_xlabel('Number of Projects', fontsize=12)
        ax.set_title(f'Top 20 ISIC Divisions - {repo_name} Repository', fontsize=16, fontweight='bold')
        ax.tick_params(axis='y', labelsize=8)
        plt.tight_layout()
        
        svg_path = OUTPUT_DIR / f"histogram_{repo_name.lower()}.svg"
        plt.savefig(svg_path, format='svg', bbox_inches='tight')
        print(f"Histogram (SVG) for {repo_name}: {svg_path}")
        
        png_path = OUTPUT_DIR / f"histogram_{repo_name.lower()}.png"
        plt.savefig(png_path, dpi=300, bbox_inches='tight')
        print(f"Histogram (PNG) for {repo_name}: {png_path}")
        
        plt.close()
        
        # Print top 20 list
        print(f"\nTop 20 ISIC Classes - {repo_name}:")
        print("-" * 70)
        for i, (code, name, count) in enumerate(reversed(rows), 1):
            print(f"{i:2}. {code} - {name[:55]}: {count} projects")
    
    conn.close()


def print_stats():
    """Print classification statistics"""
    conn = get_connection()
    
    print("\n" + "=" * 50)
    print("PROJECT TYPE DISTRIBUTION")
    print("=" * 50)
    
    cursor = conn.execute("SELECT project_type, COUNT(*) FROM projects GROUP BY project_type")
    for row in cursor:
        print(f"  {row[0]}: {row[1]} projects")
    
    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    print("=" * 50)
    print(f"  TOTAL: {total} projects")
    print("=" * 50)
    conn.close()


def print_repo_stats():
    """Print project type statistics by repository"""
    conn = get_connection()
    
    print("\n" + "=" * 60)
    print("PROJECT TYPE DISTRIBUTION BY REPOSITORY")
    print("=" * 60)
    
    cursor = conn.execute("""
        SELECT 
            CASE WHEN repository_url LIKE '%dryad%' THEN 'Dryad' ELSE 'FSD' END as repo,
            COUNT(*) as total,
            SUM(CASE WHEN project_type = 'QDA_PROJECT' THEN 1 ELSE 0 END) as QDA,
            SUM(CASE WHEN project_type = 'QD_PROJECT' THEN 1 ELSE 0 END) as QD,
            SUM(CASE WHEN project_type = 'OTHER_PROJECT' THEN 1 ELSE 0 END) as OTHER,
            SUM(CASE WHEN project_type = 'NOT_A_PROJECT' THEN 1 ELSE 0 END) as NOT_A
        FROM projects
        GROUP BY repo
        ORDER BY repo DESC
    """)
    
    for row in cursor:
        print(f"\n  {row[0]}:")
        print(f"    Total: {row[1]} projects")
        print(f"    QDA_PROJECT: {row[2]}")
        print(f"    QD_PROJECT: {row[3]}")
        print(f"    OTHER_PROJECT: {row[4]}")
        print(f"    NOT_A_PROJECT: {row[5]}")
    
    conn.close()


def main():
    print("=" * 60)
    print("Generating Part 2 Reports")
    print("=" * 60)
    
    print("\nReading from database...")
    print_stats()
    print_repo_stats()
    
    print("\nGenerating report files...")
    generate_classification_report()
    generate_xlsx()
    generate_statistics_report()
    generate_top20_csv()
    generate_histograms()
    
    print("\n" + "=" * 60)
    print(f"All reports saved in: {OUTPUT_DIR}/")
    print("=" * 60)
    print("\nFor Google Form, select the most common class shown above.")


if __name__ == "__main__":
    main()