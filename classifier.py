"""
classifier.py - Rule-based classifier for Part 2
"""

import sqlite3
from pathlib import Path

# Configuration
DB_PATH = Path("23453618-seeding.db")

# QDA File Extensions (analysis files)
QDA_EXTENSIONS = {
    '.qdpx', '.qdc',         
    '.mx24', '.mx22', '.mx20',  
    '.nvp', '.nvpx',            
    '.atlasproj', '.hpr7',      
    '.ppj', '.qlt',             
    '.f4p',                     
    '.qpd',                     
}

# Primary Data Extensions (qualitative data files)
PRIMARY_EXTENSIONS = {
    '.pdf', '.txt', '.rtf', '.doc', '.docx', '.odt',
    '.mp3', '.mp4', '.wav', '.m4a',
    '.jpg', '.jpeg', '.png', '.tiff',
    '.xlsx', '.xls', '.csv',
}

QUALITATIVE_KEYWORDS = [
    # English keywords
    'qualitative', 'interview', 'transcript', 'thematic analysis',
    'grounded theory', 'focus group', 'ethnography', 'case study',
    'phenomenology', 'narrative', 'content analysis', 'discourse analysis',
    
    # Finnish keywords (for FSD metadata)
    'laadullinen',          # qualitative
    'haastattelu',          # interview
    'haastattelut',         # interviews
    'haastattelujen',       # of interviews
    'litterointi',          # transcription
    'litteroida',           # to transcribe
    'teemahaastattelu',     # thematic interview
    'aineistolähtöinen',    # data-driven / grounded theory
    'ryhmähaastattelu',     # focus group
    'etnografia',           # ethnography
    'etnografinen',         # ethnographic
    'tapaustutkimus',       # case study
    'tapaustutkimuksen',    # of case study
    'fenomenologia',        # phenomenology
    'fenomenologinen',      # phenomenological
    'kerronta',             # narrative
    'kerronnallinen',       # narrative (adj)
    'sisällönanalyysi',     # content analysis
    'diskurssianalyysi',    # discourse analysis
    'diskursiivinen',       # discursive
    'havainnointi',         # observation
    'osallistuva',          # participatory
    'toimintatutkimus',     # action research
    'eläytymismenetelmä',   # empathy-based method
    'kysely',               # survey
    'kyselytutkimus',       # survey research
    'haastattelututkimus',  # interview study
    'kvalitatiivinen',      # qualitative (alternative)
]

# ========== ISIC Rev. 5 Division Mapping (with Finnish keywords) ==========
ISIC_MAPPING = {
    "01": {"code": "A01", "name": "Crop and animal production, hunting and related service activities", "keywords": ["agriculture", "farming", "crop", "livestock", "animal", "hunting", "maatalous", "viljely", "sato", "karja", "eläin", "metsästys"]},
    "02": {"code": "A02", "name": "Forestry and logging", "keywords": ["forest", "logging", "timber", "tree", "metsä", "hakkuu", "puu", "metsätalous"]},
    "03": {"code": "A03", "name": "Fishing and aquaculture", "keywords": ["fish", "fishing", "aquaculture", "kala", "kalastus", "vesiviljely"]},
    "05": {"code": "B05", "name": "Mining of coal and lignite", "keywords": ["coal", "mining", "lignite", "hiili", "kaivos", "ruskohiili"]},
    "06": {"code": "B06", "name": "Extraction of crude petroleum and natural gas", "keywords": ["petroleum", "oil", "gas", "extraction", "öljy", "kaasu", "poraus"]},
    "07": {"code": "B07", "name": "Mining of metal ores", "keywords": ["metal", "ore", "mining", "metalli", "malmi", "kaivos"]},
    "08": {"code": "B08", "name": "Other mining and quarrying", "keywords": ["quarry", "mining", "stone", "louhos", "kivi"]},
    "09": {"code": "B09", "name": "Mining support service activities", "keywords": ["mining support", "kaivostuki"]},
    "10": {"code": "C10", "name": "Manufacture of food products", "keywords": ["food", "manufacturing", "processing", "ruoka", "valmistus", "jalostus"]},
    "11": {"code": "C11", "name": "Manufacture of beverages", "keywords": ["beverage", "drink", "juoma"]},
    "12": {"code": "C12", "name": "Manufacture of tobacco products", "keywords": ["tobacco", "cigarette", "tupakka"]},
    "13": {"code": "C13", "name": "Manufacture of textiles", "keywords": ["textile", "fabric", "cloth", "tekstiili", "kangas"]},
    "14": {"code": "C14", "name": "Manufacture of wearing apparel", "keywords": ["apparel", "clothing", "fashion", "vaate", "muoti"]},
    "15": {"code": "C15", "name": "Manufacture of leather and related products", "keywords": ["leather", "shoe", "footwear", "nahka", "kenkä"]},
    "16": {"code": "C16", "name": "Manufacture of wood and of products of wood and cork", "keywords": ["wood", "cork", "furniture", "puu", "korkki", "huonekalu"]},
    "17": {"code": "C17", "name": "Manufacture of paper and paper products", "keywords": ["paper", "pulp", "paperi", "massa"]},
    "18": {"code": "C18", "name": "Printing and reproduction of recorded media", "keywords": ["print", "printing", "media", "paino", "julkaisu"]},
    "19": {"code": "C19", "name": "Manufacture of coke and refined petroleum products", "keywords": ["coke", "petroleum", "refining", "koksaus", "jalostus"]},
    "20": {"code": "C20", "name": "Manufacture of chemicals and chemical products", "keywords": ["chemical", "chemistry", "pharmaceutical", "kemia", "lääke"]},
    "21": {"code": "C21", "name": "Manufacture of basic pharmaceutical products", "keywords": ["pharmaceutical", "drug", "medicine", "lääke", "lääketiede"]},
    "22": {"code": "C22", "name": "Manufacture of rubber and plastic products", "keywords": ["rubber", "plastic", "kumi", "muovi"]},
    "23": {"code": "C23", "name": "Manufacture of other non-metallic mineral products", "keywords": ["mineral", "cement", "glass", "mineraali", "sementti", "lasi"]},
    "24": {"code": "C24", "name": "Manufacture of basic metals", "keywords": ["metal", "steel", "iron", "metalli", "teräs", "rauta"]},
    "25": {"code": "C25", "name": "Manufacture of fabricated metal products", "keywords": ["metal products", "fabrication", "metallituote", "valmistus"]},
    "26": {"code": "C26", "name": "Manufacture of computer, electronic and optical products", "keywords": ["computer", "electronic", "optical", "tietokone", "elektroniikka", "optiikka"]},
    "27": {"code": "C27", "name": "Manufacture of electrical equipment", "keywords": ["electrical", "equipment", "sähkö", "laite"]},
    "28": {"code": "C28", "name": "Manufacture of machinery and equipment n.e.c.", "keywords": ["machinery", "equipment", "kone", "laite"]},
    "29": {"code": "C29", "name": "Manufacture of motor vehicles", "keywords": ["motor", "vehicle", "car", "auto", "ajoneuvo"]},
    "30": {"code": "C30", "name": "Manufacture of other transport equipment", "keywords": ["transport", "ship", "aircraft", "kuljetus", "laiva", "lentokone"]},
    "31": {"code": "C31", "name": "Manufacture of furniture", "keywords": ["furniture", "huonekalu"]},
    "32": {"code": "C32", "name": "Other manufacturing", "keywords": ["manufacturing", "valmistus"]},
    "33": {"code": "C33", "name": "Repair and installation of machinery", "keywords": ["repair", "maintenance", "installation", "korjaus", "huolto", "asennus"]},
    "35": {"code": "D35", "name": "Electricity, gas, steam and air conditioning supply", "keywords": ["electricity", "gas", "steam", "sähkö", "kaasu", "höyry"]},
    "36": {"code": "E36", "name": "Water collection, treatment and supply", "keywords": ["water", "treatment", "vesi", "käsittely"]},
    "37": {"code": "E37", "name": "Sewerage", "keywords": ["sewerage", "sewage", "viemäri", "jätevesi"]},
    "38": {"code": "E38", "name": "Waste collection, treatment and disposal", "keywords": ["waste", "recycling", "jäte", "kierrätys"]},
    "39": {"code": "E39", "name": "Remediation and other waste management", "keywords": ["remediation", "waste management", "kunnostus", "jätehuolto"]},
    "41": {"code": "F41", "name": "Construction of buildings", "keywords": ["building", "construction", "rakennus", "rakentaminen"]},
    "42": {"code": "F42", "name": "Civil engineering", "keywords": ["civil engineering", "infrastructure", "rakennustekniikka", "infrastruktuuri"]},
    "43": {"code": "F43", "name": "Specialized construction activities", "keywords": ["construction", "specialized", "erikoisrakentaminen"]},
    "46": {"code": "G46", "name": "Wholesale trade", "keywords": ["wholesale", "trade", "tukku", "kauppa"]},
    "47": {"code": "G47", "name": "Retail trade", "keywords": ["retail", "shop", "vähittäiskauppa", "myymälä"]},
    "49": {"code": "H49", "name": "Land transport and transport via pipelines", "keywords": ["transport", "pipeline", "kuljetus", "putki"]},
    "50": {"code": "H50", "name": "Water transport", "keywords": ["water transport", "shipping", "vesiliikenne", "laivaliikenne"]},
    "51": {"code": "H51", "name": "Air transport", "keywords": ["air transport", "aviation", "lentoliikenne"]},
    "52": {"code": "H52", "name": "Warehousing and support activities for transportation", "keywords": ["warehousing", "logistics", "varastointi", "logistiikka"]},
    "53": {"code": "H53", "name": "Postal and courier activities", "keywords": ["postal", "courier", "posti", "kuriiri"]},
    "55": {"code": "I55", "name": "Accommodation", "keywords": ["hotel", "accommodation", "hotelli", "majoitus"]},
    "56": {"code": "I56", "name": "Food and beverage service activities", "keywords": ["restaurant", "catering", "ravintola", "ruokapalvelu"]},
    "58": {"code": "J58", "name": "Publishing activities", "keywords": ["publishing", "book", "journal", "julkaisu", "kirja", "lehti"]},
    "59": {"code": "J59", "name": "Motion picture and sound recording", "keywords": ["film", "movie", "sound", "elokuva", "ääni"]},
    "60": {"code": "J60", "name": "Programming and broadcasting", "keywords": ["broadcasting", "tv", "radio", "lähetys", "televisio"]},
    "61": {"code": "K61", "name": "Telecommunications", "keywords": ["telecom", "telecommunication", "tietoliikenne"]},
    "62": {"code": "K62", "name": "Computer programming, consultancy and related activities", "keywords": ["computer", "programming", "software", "consultancy", "it", "code", "developer", "tietokone", "ohjelmointi", "ohjelmisto", "konsultointi", "koodi", "kehittäjä"]},
    "63": {"code": "K63", "name": "Computing infrastructure and data processing", "keywords": ["hosting", "cloud", "data", "pilvi"]},
    "64": {"code": "L64", "name": "Financial service activities", "keywords": ["finance", "bank", "insurance", "rahoitus", "pankki", "vakuutus"]},
    "65": {"code": "L65", "name": "Insurance and pension funding", "keywords": ["insurance", "pension", "vakuutus", "eläke"]},
    "66": {"code": "L66", "name": "Activities auxiliary to financial services", "keywords": ["financial services", "rahoituspalvelut"]},
    "68": {"code": "M68", "name": "Real estate activities", "keywords": ["real estate", "property", "kiinteistö"]},
    "69": {"code": "N69", "name": "Legal and accounting activities", "keywords": ["legal", "law", "accounting", "laki", "kirjanpito"]},
    "70": {"code": "N70", "name": "Management consultancy activities", "keywords": ["management", "consultancy", "johtaminen", "konsultointi"]},
    "71": {"code": "N71", "name": "Architectural and engineering activities", "keywords": ["architecture", "engineering", "technical", "arkkitehtuuri", "tekniikka"]},
    "72": {"code": "N72", "name": "Scientific research and development", "keywords": ["research", "science", "r&d", "laboratory", "experiment", "scientific", "tutkimus", "tiede", "laboratorio", "kokeilu", "tieteellinen"]},
    "73": {"code": "N73", "name": "Advertising and market research", "keywords": ["advertising", "marketing", "market research", "mainonta", "markkinointi"]},
    "74": {"code": "N74", "name": "Other professional and technical activities", "keywords": ["professional", "technical", "ammatillinen", "tekninen"]},
    "75": {"code": "N75", "name": "Veterinary activities", "keywords": ["veterinary", "animal", "eläinlääketiede", "eläin"]},
    "77": {"code": "O77", "name": "Rental and leasing activities", "keywords": ["rental", "leasing", "vuokraus"]},
    "78": {"code": "O78", "name": "Employment activities", "keywords": ["employment", "recruitment", "työllisyys", "rekrytointi"]},
    "79": {"code": "O79", "name": "Travel agency and tour operator activities", "keywords": ["travel", "tourism", "matka", "matkailu"]},
    "80": {"code": "O80", "name": "Investigation and security activities", "keywords": ["security", "investigation", "turvallisuus", "tutkinta"]},
    "81": {"code": "O81", "name": "Services to buildings and landscape activities", "keywords": ["building services", "landscape", "rakennuspalvelu", "maisema"]},
    "82": {"code": "O82", "name": "Office administrative and support activities", "keywords": ["administration", "support", "hallinto", "tuki"]},
    "84": {"code": "P84", "name": "Public administration and defence", "keywords": ["public administration", "government", "defence", "hallinto", "puolustus"]},
    "85": {"code": "Q85", "name": "Education", "keywords": ["education", "school", "university", "teaching", "student", "learning", "teacher", "classroom", "pedagogy", "koulutus", "koulu", "yliopisto", "opetus", "opiskelija", "oppilas", "opettaja", "luokka", "pedagogiikka"]},
    "86": {"code": "R86", "name": "Human health activities", "keywords": ["health", "medical", "hospital", "clinic", "doctor", "patient", "medicine", "terveys", "lääketiede", "sairaala", "klinikka", "lääkäri", "potilas", "hoito"]},
    "87": {"code": "R87", "name": "Residential care activities", "keywords": ["residential care", "nursing home", "asumispalvelu", "hoivakoti"]},
    "88": {"code": "R88", "name": "Social work activities without accommodation", "keywords": ["social work", "welfare", "community", "counseling", "sosiaalityö", "hyvinvointi", "yhteisö", "ohjaus", "neuvonta"]},
    "90": {"code": "S90", "name": "Arts creation and performing arts activities", "keywords": ["art", "performing arts", "music", "taide", "musiikki", "teatteri", "esitys"]},
    "91": {"code": "S91", "name": "Library, archives, museum and other cultural activities", "keywords": ["library", "archive", "museum", "cultural", "kirjasto", "arkisto", "museo", "kulttuuri"]},
    "92": {"code": "S92", "name": "Gambling and betting activities", "keywords": ["gambling", "betting", "uhkapeli", "vedonlyönti"]},
    "93": {"code": "S93", "name": "Sports activities and amusement", "keywords": ["sport", "amusement", "recreation", "urheilu", "virkistys"]},
    "94": {"code": "T94", "name": "Activities of membership organizations", "keywords": ["membership", "organization", "jäsenyys", "järjestö"]},
    "95": {"code": "T95", "name": "Repair and maintenance activities", "keywords": ["repair", "maintenance", "korjaus", "huolto"]},
    "96": {"code": "T96", "name": "Personal service activities", "keywords": ["personal service", "henkilökohtainen palvelu"]},
    "97": {"code": "U97", "name": "Activities of households as employers", "keywords": ["household", "kotitalous"]},
    "98": {"code": "U98", "name": "Undifferentiated goods- and services-producing activities", "keywords": ["household activities", "kotitaloustoiminta"]},
    "99": {"code": "V99", "name": "Activities of extraterritorial organizations", "keywords": ["extraterritorial", "ekstraterritoriaalinen"]},
}


def get_connection():
    return sqlite3.connect(DB_PATH)


def add_columns():
    """Add required columns if not exists"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(projects)")
    columns = [col[1] for col in cursor.fetchall()]
    
    columns_to_add = [
        ('project_type', 'TEXT'),
        ('isic_code', 'TEXT'),
        ('isic_division_code', 'TEXT'),
        ('isic_division_name', 'TEXT'),
        ('secondary_isic_code', 'TEXT'),
        ('secondary_isic_division_name', 'TEXT'),
    ]
    
    for col_name, col_type in columns_to_add:
        if col_name not in columns:
            cursor.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_type}")
            print(f"Added column: {col_name}")
    
    conn.commit()
    conn.close()


def update_project_type(project_id: int, project_type: str):
    """Update the project_type column"""
    conn = get_connection()
    conn.execute("UPDATE projects SET project_type = ? WHERE id = ?", (project_type, project_id))
    conn.commit()
    conn.close()


def update_isic(project_id: int, isic_code: str, division_code: str, division_name: str):
    """Update ISIC columns for a project"""
    conn = get_connection()
    conn.execute("""
        UPDATE projects 
        SET isic_code = ?, isic_division_code = ?, isic_division_name = ?
        WHERE id = ?
    """, (isic_code, division_code, division_name, project_id))
    conn.commit()
    conn.close()


def update_secondary_isic(project_id: int, isic_code: str, division_name: str):
    """Update secondary ISIC columns for a project"""
    conn = get_connection()
    conn.execute("""
        UPDATE projects 
        SET secondary_isic_code = ?, secondary_isic_division_name = ?
        WHERE id = ?
    """, (isic_code, division_name, project_id))
    conn.commit()
    conn.close()


def classify_project_type(project_id, title, description, files, keywords):
    """Classify project as QDA_PROJECT, QD_PROJECT, OTHER_PROJECT, NOT_A_PROJECT"""
    
    # Rule 1: Check for QDA files (highest priority)
    for file_name in files:
        ext = '.' + file_name.split('.')[-1].lower() if '.' in file_name else ''
        if ext in QDA_EXTENSIONS:
            return "QDA_PROJECT"
    
    # Rule 2: Check for primary data files
    has_primary_data = False
    for file_name in files:
        ext = '.' + file_name.split('.')[-1].lower() if '.' in file_name else ''
        if ext in PRIMARY_EXTENSIONS:
            has_primary_data = True
            break
    
    # Rule 3: Check metadata for qualitative keywords
    text = (title or "") + " " + (description or "") + " " + " ".join(keywords)
    text_lower = text.lower()
    has_qualitative_keyword = any(kw in text_lower for kw in QUALITATIVE_KEYWORDS)
    
    # Rule 4: Decide classification (using database only)
    if has_primary_data or has_qualitative_keyword:
        return "QD_PROJECT"
    
    # Rule 5: Check if it's a research project at all
    if not title and not description and len(files) == 0:
        return "NOT_A_PROJECT"
    
    return "OTHER_PROJECT"


def classify_isic(title, description, keywords):
    """Classify project into ISIC division (2 levels deep) - returns primary and secondary"""
    text = (title or "") + " " + (description or "") + " " + " ".join(keywords)
    text_lower = text.lower()
    
    # Get all matches with scores
    matches = []
    for division, info in ISIC_MAPPING.items():
        score = 0
        for keyword in info["keywords"]:
            if keyword in text_lower:
                score += 1
        if score > 0:
            matches.append({
                'score': score,
                'code': info["code"],
                'division': division,
                'name': info["name"]
            })
    
    # Sort by score descending
    matches.sort(key=lambda x: x['score'], reverse=True)
    
    # Primary = best match
    primary = matches[0] if matches else None
    
    # Secondary = second best match ONLY IF score is at least 50% of primary
    secondary = None
    if len(matches) > 1 and primary:
        # Calculate threshold (50% of primary score)
        threshold = primary['score'] * 0.5
        if matches[1]['score'] >= threshold:
            secondary = matches[1]
    
    if primary:
        primary_result = (primary['code'], primary['division'], primary['name'])
    else:
        primary_result = (None, None, None)
    
    if secondary:
        secondary_result = (secondary['code'], secondary['name'])
    else:
        secondary_result = (None, None)
    
    return primary_result, secondary_result


def classify_file_isic(file_name, file_type, title, description, keywords):
    """Classify a primary data file into ISIC division"""
    # Combine file name + project metadata
    text = (file_name or "") + " " + (title or "") + " " + (description or "") + " " + " ".join(keywords)
    text_lower = text.lower()
    
    best_match = None
    best_score = 0
    best_division = None
    best_name = None
    
    for division, info in ISIC_MAPPING.items():
        score = 0
        for keyword in info["keywords"]:
            if keyword in text_lower:
                score += 1
        if score > best_score:
            best_score = score
            best_match = info["code"]
            best_division = division
            best_name = info["name"]
    
    if best_match and best_score > 0:
        return best_match, best_division, best_name
    return None, None, None


def run_classifier():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Add columns if needed
    add_columns()
    
    # Add ISIC columns to FILES table
    cursor.execute("PRAGMA table_info(files)")
    file_columns = [col[1] for col in cursor.fetchall()]
    file_cols_to_add = [
        ('isic_code', 'TEXT'),
        ('isic_division_code', 'TEXT'),
        ('isic_division_name', 'TEXT'),
    ]
    for col_name, col_type in file_cols_to_add:
        if col_name not in file_columns:
            cursor.execute(f"ALTER TABLE files ADD COLUMN {col_name} {col_type}")
            print(f"Added column to files: {col_name}")
    
    # Get all projects
    cursor.execute("""
        SELECT p.id, p.title, p.description, p.repository_url,
               p.download_repository_folder,
               p.download_project_folder,
               GROUP_CONCAT(DISTINCT f.file_name) as files,
               GROUP_CONCAT(DISTINCT k.keyword) as keywords
        FROM projects p
        LEFT JOIN files f ON p.id = f.project_id
        LEFT JOIN keywords k ON p.id = k.project_id
        GROUP BY p.id
    """)
    
    projects = cursor.fetchall()
    
    for project in projects:
        project_id = project[0]
        title = project[1] or ""
        description = project[2] or ""
        repository_url = project[3] or ""
        repo_folder = project[4] or ""
        project_folder = project[5] or ""
        files = project[6].split(',') if project[6] else []
        keywords = project[7].split(',') if project[7] else []
        
        # Classify project type
        project_type = classify_project_type(project_id, title, description, files, keywords)
        
        # Update project_type using the SAME connection
        cursor.execute("UPDATE projects SET project_type = ? WHERE id = ?", (project_type, project_id))
        
        # Classify ISIC for project (primary and secondary)
        (isic_code, division_code, division_name), (secondary_code, secondary_name) = classify_isic(title, description, keywords)
        
        if isic_code:
            cursor.execute("""
                UPDATE projects 
                SET isic_code = ?, isic_division_code = ?, isic_division_name = ?
                WHERE id = ?
            """, (isic_code, division_code, division_name, project_id))
        
        if secondary_code:
            cursor.execute("""
                UPDATE projects 
                SET secondary_isic_code = ?, secondary_isic_division_name = ?
                WHERE id = ?
            """, (secondary_code, secondary_name, project_id))
        
        # Classify ISIC for each file (if project is QDA_PROJECT or QD_PROJECT)
        if project_type in ['QDA_PROJECT', 'QD_PROJECT']:
            for file_name in files:
                ext = '.' + file_name.split('.')[-1].lower() if '.' in file_name else ''
                file_type = ext.lstrip('.')
                
                # Only classify primary data files
                if ext in PRIMARY_EXTENSIONS:
                    file_isic_code, file_division_code, file_division_name = classify_file_isic(
                        file_name, file_type, title, description, keywords
                    )
                    if file_isic_code:
                        cursor.execute("""
                            UPDATE files 
                            SET isic_code = ?, isic_division_code = ?, isic_division_name = ?
                            WHERE project_id = ? AND file_name = ?
                        """, (file_isic_code, file_division_code, file_division_name, project_id, file_name))
    
    conn.commit()
    conn.close()
    print(f"Classified {len(projects)} projects and their primary data files")


def main():
    print("=" * 50)
    print("Part 2: Rule-Based Classifier")
    print("=" * 50)
    
    print("\nRunning classifier on all projects...\n")
    run_classifier()
    
    print("\nClassification complete!")


if __name__ == "__main__":
    main()