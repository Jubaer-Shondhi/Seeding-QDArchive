"""
generate_llm_reports.py - Generate visualizations from LLM classification results
Run this after llm_classifier.py has populated the database.
"""

import sqlite3
import matplotlib.pyplot as plt
from pathlib import Path

# Configuration
DB_PATH = Path("23453618-sq26-classification.db")
OUTPUT_DIR = Path("classification_output")
OUTPUT_DIR.mkdir(exist_ok=True)


def get_connection():
    return sqlite3.connect(DB_PATH)


def visualize_llm_histogram():
    """Create histogram of top ISIC divisions from LLM classification"""
    conn = get_connection()
    
    cursor = conn.execute("""
        SELECT llm_division_code, llm_division_name, COUNT(*) as count
        FROM projects
        WHERE llm_division_code IS NOT NULL
        GROUP BY llm_division_code, llm_division_name
        ORDER BY COUNT(*) DESC
        LIMIT 15
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print(" No LLM classification data found!")
        return
    
    # Prepare data
    codes = [f"{row[0]} - {row[1][:25]}" for row in rows]
    counts = [row[2] for row in rows]
    
    # Create horizontal bar chart (easier to read long names)
    fig, ax = plt.subplots(figsize=(12, max(6, len(codes) * 0.5)))
    
    bars = ax.barh(codes, counts, color='steelblue', edgecolor='black')
    
    # Add count labels on bars
    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                str(count), ha='left', va='center', fontsize=10, fontweight='bold')
    
    ax.set_xlabel('Number of Projects', fontsize=12)
    ax.set_title('Top 15 ISIC Divisions - LLM Classification', fontsize=16, fontweight='bold')
    ax.tick_params(axis='y', labelsize=9)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'llm_histogram.png', dpi=300, bbox_inches='tight')
    print(f" LLM histogram saved to {OUTPUT_DIR / 'llm_histogram.png'}")
    plt.close()


def visualize_llm_confidence():
    """Visualize confidence distribution of LLM classifications"""
    conn = get_connection()
    
    cursor = conn.execute("""
        SELECT llm_confidence, COUNT(*) as count
        FROM projects
        WHERE llm_confidence IS NOT NULL
        GROUP BY llm_confidence
        ORDER BY count DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print(" No confidence data found!")
        return
    
    labels = [row[0] for row in rows]
    counts = [row[1] for row in rows]
    colors = {'high': '#2ecc71', 'medium': '#f39c12', 'low': '#e74c3c'}
    bar_colors = [colors.get(label, 'gray') for label in labels]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Pie chart
    ax1.pie(counts, labels=labels, autopct='%1.1f%%', colors=bar_colors, startangle=90)
    ax1.set_title('LLM Confidence Distribution', fontsize=14, fontweight='bold')
    
    # Bar chart
    bars = ax2.bar(labels, counts, color=bar_colors, edgecolor='black')
    ax2.set_xlabel('Confidence Level', fontsize=12)
    ax2.set_ylabel('Number of Projects', fontsize=12)
    ax2.set_title('LLM Confidence Counts', fontsize=14, fontweight='bold')
    
    for bar, count in zip(bars, counts):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                str(count), ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'llm_confidence.png', dpi=300, bbox_inches='tight')
    print(f" LLM confidence visualization saved to {OUTPUT_DIR / 'llm_confidence.png'}")
    plt.close()


def visualize_llm_by_repository():
    """Visualize LLM classification results by repository"""
    conn = get_connection()
    
    repos = conn.execute("""
        SELECT 
            CASE WHEN repository_url LIKE '%dryad%' THEN 'Dryad' ELSE 'FSD' END as repo,
            llm_division_code,
            llm_division_name,
            COUNT(*) as count
        FROM projects
        WHERE llm_division_code IS NOT NULL
        GROUP BY repo, llm_division_code, llm_division_name
        ORDER BY repo, count DESC
    """).fetchall()
    
    conn.close()
    
    if not repos:
        print("⚠️ No LLM data found!")
        return
    
    # Separate by repository
    dryad_data = [(r[1], r[2], r[3]) for r in repos if r[0] == 'Dryad']
    fsd_data = [(r[1], r[2], r[3]) for r in repos if r[0] == 'FSD']
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Dryad
    if dryad_data:
        codes = [f"{d[0]}" for d in dryad_data[:10]]
        counts = [d[2] for d in dryad_data[:10]]
        bars = ax1.barh(codes, counts, color='steelblue', edgecolor='black')
        for bar, count in zip(bars, counts):
            ax1.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                    str(count), ha='left', va='center', fontsize=9, fontweight='bold')
        ax1.set_title('Dryad - Top 10 ISIC Divisions (LLM)', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Number of Projects')
        ax1.tick_params(axis='y', labelsize=8)
    
    # FSD
    if fsd_data:
        codes = [f"{d[0]}" for d in fsd_data[:10]]
        counts = [d[2] for d in fsd_data[:10]]
        bars = ax2.barh(codes, counts, color='forestgreen', edgecolor='black')
        for bar, count in zip(bars, counts):
            ax2.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                    str(count), ha='left', va='center', fontsize=9, fontweight='bold')
        ax2.set_title('FSD - Top 10 ISIC Divisions (LLM)', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Number of Projects')
        ax2.tick_params(axis='y', labelsize=8)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'llm_by_repository.png', dpi=300, bbox_inches='tight')
    print(f" LLM by repository saved to {OUTPUT_DIR / 'llm_by_repository.png'}")
    plt.close()


def print_stats():
    """Print LLM classification statistics"""
    conn = get_connection()
    
    # Total classified
    cursor = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN llm_division_code IS NOT NULL THEN 1 ELSE 0 END) as classified
        FROM projects
    """)
    row = cursor.fetchone()
    
    print("\n" + "=" * 50)
    print("LLM CLASSIFICATION STATISTICS")
    print("=" * 50)
    print(f"  Total projects: {row[0]}")
    print(f"  Classified with LLM: {row[1]}")
    print(f"  Remaining: {row[0] - row[1]}")
    
    # Confidence distribution
    cursor = conn.execute("""
        SELECT llm_confidence, COUNT(*) 
        FROM projects 
        WHERE llm_confidence IS NOT NULL 
        GROUP BY llm_confidence
    """)
    print("\n  Confidence Distribution:")
    for row in cursor:
        print(f"    {row[0]}: {row[1]} projects")
    
    # Top 5 ISIC divisions
    cursor = conn.execute("""
        SELECT llm_division_code, llm_division_name, COUNT(*) 
        FROM projects 
        WHERE llm_division_code IS NOT NULL 
        GROUP BY llm_division_code, llm_division_name 
        ORDER BY COUNT(*) DESC 
        LIMIT 5
    """)
    print("\n  Top 5 ISIC Divisions (LLM):")
    for row in cursor:
        print(f"    {row[0]} - {row[1]}: {row[2]} projects")
    
    conn.close()


def main():
    print("=" * 60)
    print(" LLM Classification Reports")
    print("=" * 60)
    
    print("\n Reading from database...")
    print_stats()
    
    print("\n Generating visualizations...")
    visualize_llm_histogram()
    visualize_llm_confidence()
    visualize_llm_by_repository()
    
    print("\n" + "=" * 60)
    print(f" All LLM reports saved in: {OUTPUT_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()