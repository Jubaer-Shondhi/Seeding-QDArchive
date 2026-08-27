"""
llm_classifier.py - LLM-based ISIC classification using Groq API
"""

import sqlite3
import re
import json
import time
from pathlib import Path
from openai import OpenAI

# Configuration
DB_PATH = Path("23453618-sq26-classification.db")
GROQ_API_KEY = ""  # ← Paste your API key here

# Initialize Groq client
client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=GROQ_API_KEY
)

def add_llm_columns():
    """Add LLM columns to projects table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    columns = [
        ('llm_section_code', 'TEXT'),
        ('llm_division_code', 'TEXT'),
        ('llm_division_name', 'TEXT'),
        ('llm_confidence', 'TEXT'),
    ]
    
    for col_name, col_type in columns:
        try:
            cursor.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
            print(f"Added {col_name}")
        except sqlite3.OperationalError:
            print(f"{col_name} already exists")
    
    conn.commit()
    conn.close()

def classify_with_llm(title, description, keywords):
    """Classify a project using Groq LLM"""
    
    # Clean and truncate text to avoid token limits
    MAX_LENGTH = 1000
    title = (title or "").strip()[:MAX_LENGTH]
    description = (description or "").strip()[:MAX_LENGTH]
    keywords = (keywords or "").strip()[:MAX_LENGTH]
    
    prompt = f"""You are an expert classifier. Classify this qualitative research project into ISIC Rev. 5 division (2 levels deep).

Project Title: {title}
Project Description: {description}
Keywords: {keywords}

Return ONLY a valid JSON object with these exact fields:
{{
    "section_code": "R",
    "division_code": "86",
    "division_name": "Human health activities",
    "confidence": "high"
}}

IMPORTANT RULES:
1. section_code must be a single letter (A-V) - for example: A, B, C, ... V
2. division_code must be a 2-digit number as a string (e.g., "85", "86", "72")
3. Choose the MOST SPECIFIC division that fits the project topic
4. If unsure, use section N (Professional, scientific) and division 72 (Scientific research and development)
5. confidence must be one of: "high", "medium", "low"
6. DO NOT include any other text, explanation, or markdown. ONLY the JSON object.

ISIC Divisions Reference (most common for qualitative research):
- Division 85 (Q): Education, teaching, schools, universities
- Division 86 (R): Human health, medical, hospitals, patients
- Division 72 (N): Scientific research, development, experiments
- Division 62 (K): Computer programming, software, IT, coding
- Division 88 (R): Social work, welfare, counseling, community
- Division 90 (S): Arts, music, performing arts, creative work
- Division 91 (S): Libraries, archives, museums, cultural activities
- Division 01 (A): Agriculture, farming, crops, livestock
- Division 02 (A): Forestry, logging, trees
- Division 07 (B): Mining, ores, metals
- Division 06 (B): Oil, gas, petroleum extraction
- Division 10 (C): Food manufacturing, processing
- Division 20 (C): Chemicals, chemical products
- Division 21 (C): Pharmaceuticals, drugs, medicines
- Division 22 (C): Rubber, plastics
- Division 24 (C): Metals, steel, iron
- Division 26 (C): Computers, electronics, optics
- Division 27 (C): Electrical equipment
- Division 28 (C): Machinery, equipment
- Division 29 (C): Motor vehicles, cars
- Division 30 (C): Transport equipment, ships, aircraft
- Division 35 (D): Electricity, gas, steam
- Division 36 (E): Water collection, treatment
- Division 38 (E): Waste collection, recycling
- Division 41 (F): Construction of buildings
- Division 46 (G): Wholesale trade
- Division 47 (G): Retail trade
- Division 49 (H): Land transport, pipelines
- Division 55 (I): Accommodation, hotels
- Division 56 (I): Food and beverage services
- Division 58 (J): Publishing, books, journals
- Division 61 (K): Telecommunications
- Division 64 (L): Financial services, banking
- Division 68 (M): Real estate
- Division 69 (N): Legal, accounting
- Division 70 (N): Management consultancy
- Division 71 (N): Architecture, engineering
- Division 73 (N): Advertising, market research
- Division 74 (N): Professional, technical activities
- Division 78 (O): Employment, recruitment
- Division 79 (O): Travel, tourism
- Division 80 (O): Security, investigation
- Division 84 (P): Public administration, government
- Division 87 (R): Residential care
- Division 93 (S): Sports, recreation
- Division 94 (T): Membership organizations
- Division 96 (T): Personal services
"""
    
    try:
        response = client.chat.completions.create(
            model="qwen/qwen3.8-27b",  # Working model
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.1
        )
        
        content = response.choices[0].message.content.strip()
        
        # Extract JSON from response
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            content = json_match.group(0)
        
        try:
            result = json.loads(content)
            
            # Validate required fields
            required = ['section_code', 'division_code', 'division_name', 'confidence']
            for field in required:
                if field not in result:
                    print(f"⚠️ Missing field: {field}")
                    raise ValueError(f"Missing field: {field}")
            
            # Validate section_code is a single letter
            section_code = result.get('section_code', '')
            if not isinstance(section_code, str) or len(section_code) != 1 or not section_code.isalpha():
                print(f"⚠️ Invalid section_code: {section_code}")
                result['section_code'] = 'N'
            
            # Validate division_code is a 2-digit number
            division_code = str(result.get('division_code', ''))
            if not division_code.isdigit() or len(division_code) != 2:
                print(f"⚠️ Invalid division_code: {division_code}")
                result['division_code'] = '72'
            else:
                result['division_code'] = division_code
            
            # Validate confidence
            confidence = result.get('confidence', 'low')
            if confidence not in ['high', 'medium', 'low']:
                print(f"⚠️ Invalid confidence: {confidence}")
                result['confidence'] = 'low'
            
            # Ensure division_name is a string
            result['division_name'] = str(result.get('division_name', 'Scientific research and development'))
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"⚠️ JSON parsing error: {e}")
            print(f"   Response was: {content[:200]}...")
            # Fallback
            return {
                "section_code": "N",
                "division_code": "72",
                "division_name": "Scientific research and development",
                "confidence": "low"
            }
            
    except Exception as e:
        print(f"⚠️ API error: {e}")
        return {
            "section_code": "N",
            "division_code": "72",
            "division_name": "Scientific research and development",
            "confidence": "low"
        }
        
def run_llm_classifier(limit=None):
    """Classify projects using LLM"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get projects without LLM classification
    cursor.execute("""
        SELECT p.id, p.title, p.description, 
               GROUP_CONCAT(DISTINCT k.keyword) as keywords
        FROM projects p
        LEFT JOIN keywords k ON p.id = k.project_id
        WHERE p.llm_division_code IS NULL
        GROUP BY p.id
        LIMIT ?
    """, (limit,))
    
    projects = cursor.fetchall()
    total = len(projects)
    
    if total == 0:
        print("✅ No projects to classify!")
        conn.close()
        return
    
    print(f"📊 Classifying {total} projects...")
    print("=" * 50)
    
    success_count = 0
    fail_count = 0
    
    for i, project in enumerate(projects, 1):
        project_id = project[0]
        title = project[1] or ""
        description = project[2] or ""
        keywords = project[3] or ""
        
        print(f"\n🔍 [{i}/{total}] Processing project {project_id}...")
        print(f"   Title: {title[:50]}...")
        
        try:
            result = classify_with_llm(title, description, keywords)
            print(f"   Result: {result}")
            
            # Force all values to strings
            section_code = str(result.get('section_code', 'N'))
            division_code = str(result.get('division_code', '72'))
            division_name = str(result.get('division_name', 'Unknown'))
            confidence = str(result.get('confidence', 'low'))
            
            # Try to update with error handling
            try:
                cursor.execute("""
                    UPDATE projects 
                    SET llm_section_code = ?,
                        llm_division_code = ?,
                        llm_division_name = ?,
                        llm_confidence = ?
                    WHERE id = ?
                """, (section_code, division_code, division_name, confidence, project_id))
                
                conn.commit()
                print(f"✅ [{i}/{total}] Project {project_id}: {division_code} (Conf: {confidence})")
                success_count += 1
                
            except sqlite3.IntegrityError as e:
                print(f"❌ [{i}/{total}] Project {project_id} integrity error: {e}")
                conn.rollback()
                fail_count += 1
                
        except Exception as e:
            print(f"❌ [{i}/{total}] Project {project_id} failed: {e}")
            fail_count += 1
            conn.rollback()
        
        # Delay to avoid rate limits
        time.sleep(0.5)
    
    conn.close()
    
    print("=" * 50)
    print(f"🎉 LLM classification complete!")
    print(f"   ✅ Successfully classified: {success_count} projects")
    print(f"   ❌ Failed: {fail_count} projects")
    print(f"   📊 Total processed: {success_count + fail_count} projects")

def compare_results():
    """Compare rule-based vs LLM results"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("""
        SELECT id, title,
               isic_code as rule_based,
               llm_division_code as llm_based,
               llm_confidence
        FROM projects
        WHERE isic_code IS NOT NULL 
        AND llm_division_code IS NOT NULL
        LIMIT 20
    """)
    
    print("\n📊 Rule-Based vs LLM Comparison:")
    print("=" * 60)
    
    matches = 0
    total = 0
    for row in cursor:
        total += 1
        rule = row[2]
        llm = row[3]
        
        # Normalize for comparison: extract just the division code from rule-based
        rule_division = rule[1:] if rule and len(rule) > 1 and rule[0].isalpha() else rule
        
        match = "✅" if rule_division == llm else "❌"
        if rule_division == llm:
            matches += 1
        
        print(f"{match} Project {row[0]}: Rule={rule}, LLM={llm}, Conf={row[4]}")
    
    if total > 0:
        print(f"\n🎯 Match rate: {matches}/{total} ({matches/total*100:.1f}%)")
    conn.close()

def get_stats():
    """Get LLM classification stats"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN llm_division_code IS NOT NULL THEN 1 ELSE 0 END) as classified
        FROM projects
    """)
    row = cursor.fetchone()
    print(f"   LLM Classification Stats:")
    print(f"   Total projects: {row[0]}")
    print(f"   Classified with LLM: {row[1]}")
    print(f"   Remaining: {row[0] - row[1]}")
    conn.close()

if __name__ == "__main__":
    print("=" * 50)
    print("LLM Classifier with Groq")
    print("=" * 50)
    
    # Step 1: Add columns
    add_llm_columns()
    
    # Step 2: Process in batches of 10
    batch_size = 10
    total_processed = 0
    
    while total_processed < 648:
        remaining = 648 - total_processed
        current_batch = min(batch_size, remaining)
        
        print(f"\n📦 Processing batch of {current_batch} projects...")
        run_llm_classifier(limit=current_batch)
        
        total_processed += current_batch
        print(f"📊 Total processed so far: {total_processed}/648")
        
        # Wait between batches to avoid rate limits
        if total_processed < 648:
            print("⏳ Waiting 3 seconds before next batch...")
            time.sleep(3)
    
    # Step 3: Show stats
    get_stats()
    
    # Step 4: Compare results
    compare_results()