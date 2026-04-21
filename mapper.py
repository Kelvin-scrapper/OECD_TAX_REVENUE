import pandas as pd
import re
import os
from datetime import datetime

# --- 1. The Hardcoded Blueprint (Unchanged) ---
# This is the master list defining the exact final structure of our output file.
BLUEPRINT_HEADERS = [
    ('CHL.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Chile: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('COL.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Colombia: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('CRI.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Costa Rica: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('MEX.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Mexico: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('OECD.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"OECD - Average: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('ATG.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Antigua and Barbuda: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('ARG.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Argentina: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('BHS.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Bahamas: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('BRB.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Barbados: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('BLZ.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Belize: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('BOL.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Bolivia: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('BRA.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Brazil: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('CUB.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Cuba: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('DOM.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Dominican Republic: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('ECU.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Ecuador: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('SLV.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"El Salvador: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('GTM.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Guatemala: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('GUY.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Guyana: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('HND.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Honduras: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('JAM.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Jamaica: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('NIC.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Nicaragua: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('PAN.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Panama: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('PRY.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Paraguay: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('PER.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Peru: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('LCA.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Saint Lucia: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('TTO.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Trinidad and Tobago: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('URY.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Uruguay: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('VEN.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Venezuela: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('LAMCAR.RSLACT.TAX5000.PERCENTGDP.TOTAL', '"Latin America and the Caribbean: Revenue Statistics - Latin American Countries , 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('AUS.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Australia: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('AUT.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Austria: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('BEL.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Belgium: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('CAN.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Canada: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('CHL.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Chile: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('COL.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Colombia: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('CRI.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Costa Rica: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('CZE.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Czech Republic: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('DNK.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Denmark: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('EST.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Estonia: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('FIN.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Finland: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('FRA.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"France: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('DEU.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Germany: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('GRC.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Greece: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('HUN.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Hungary: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('ISL.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Iceland: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('IRL.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Ireland: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('ISR.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Israel: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('ITA.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Italy: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('JPN.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Japan: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('KOR.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Korea: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('LVA.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Latvia: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('LTU.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Lithuania: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('LUX.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Luxembourg: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('MEX.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Mexico: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('NLD.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Netherlands: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('NZL.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"New Zealand: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('NOR.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Norway: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('POL.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Poland: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('PRT.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Portugal: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('SVK.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Slovak Republic: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('SVN.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Slovenia: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('ESP.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Spain: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('SWE.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Sweden: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('CHE.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Switzerland: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('TUR.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"Türkiye: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('GBR.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"United Kingdom: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('USA.REVOECD.TAX5000.PERCENTGDP.TOTAL', '"United States: Revenue Statistics - OECD countries, 5000 Taxes on goods and services, Tax revenue as % of GDP, Goverment: Total"'),
    ('CHL.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Chile: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('COL.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Colombia: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('CRI.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Costa Rica: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('MEX.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Mexico: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('OECD.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"OECD - Average: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('ATG.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Antigua and Barbuda: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('ARG.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Argentina: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('BHS.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Bahamas: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('BRB.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Barbados: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('BLZ.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Belize: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('BOL.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Bolivia: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('BRA.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Brazil: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('CUB.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Cuba: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('DOM.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Dominican Republic: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('ECU.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Ecuador: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('SLV.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"El Salvador: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('GTM.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Guatemala: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('GUY.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Guyana: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('HND.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Honduras: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('JAM.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Jamaica: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('NIC.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Nicaragua: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('PAN.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Panama: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('PRY.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Paraguay: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('PER.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Peru: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('LCA.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Saint Lucia: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('TTO.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Trinidad and Tobago: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('URY.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Uruguay: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('VEN.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Venezuela: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"'),
    ('LAMCAR.RSLACT.TAX5124.PERCENTGDP.TOTAL', '"Latin America and the Caribbean: Revenue Statistics - Latin American Countries , 5124 Taxes on exports, Tax revenue as % of GDP, Goverment: Total"')
]

# --- 2. Processing Functions ---

def clean_name(text):
    """
    Cleans and standardizes country names from various source formats.
    Enhanced with fuzzy matching capabilities for robust country detection.
    """
    # First, clean the base text: remove quotes, strip whitespace, handle HTML entities
    cleaned = str(text).split(':')[0].replace('"', '').strip()
    cleaned = cleaned.replace('T&#252;rkiye', 'Türkiye')
    
    # Next, remove trailing numbers (e.g., from 'Chile 5000')
    cleaned = re.sub(r'\s*\d+\s*$', '', cleaned).strip()

    # Finally, apply specific standardizations for known inconsistencies
    if cleaned == 'OECD average country':
        return 'OECD - Average'
    if cleaned == 'Czechia':
        return 'Czech Republic'
        
    return cleaned

def robust_country_match(target_country, available_countries):
    """
    Performs robust country name matching using multiple strategies.
    Returns the best matching country name from available_countries or None.
    """
    if target_country in available_countries:
        return target_country
    
    # Strategy 1: Case-insensitive exact match
    target_lower = target_country.lower()
    for country in available_countries:
        if country.lower() == target_lower:
            return country
    
    # Strategy 2: Partial matching (target is subset of available country)
    for country in available_countries:
        if target_lower in country.lower() or country.lower() in target_lower:
            return country
    
    # Strategy 3: Common variations and aliases
    country_aliases = {
        'czech republic': ['czechia', 'czech'],
        'türkiye': ['turkey'],
        'oecd - average': ['oecd average country', 'oecd average', 'oecd'],
        'united states': ['usa', 'united states of america'],
        'united kingdom': ['uk', 'great britain'],
        'korea': ['south korea', 'republic of korea'],
        'slovak republic': ['slovakia'],
        'latin america and the caribbean': ['latin america', 'lac', 'lamcar']
    }
    
    # Check aliases for target country
    target_key = target_lower
    for standard_name, aliases in country_aliases.items():
        if target_lower == standard_name or target_lower in aliases:
            for country in available_countries:
                country_lower = country.lower()
                if country_lower == standard_name or country_lower in aliases:
                    return country
    
    # Check aliases for available countries
    for country in available_countries:
        country_lower = country.lower()
        for standard_name, aliases in country_aliases.items():
            if country_lower == standard_name or country_lower in aliases:
                if target_lower == standard_name or target_lower in aliases:
                    return country
    
    return None

def extract_data_from_file(filepath, year_set):
    """
    Reads a single source file and returns a clean dictionary mapping cleaned 
    country names to their data series. It also updates the global set of all years found.
    Also returns metadata about the file content for universal mapping.
    """
    country_data = {}
    file_metadata = {'tax_category': None, 'countries_found': []}
    
    try:
        df = pd.read_csv(filepath, skiprows=5, skipfooter=2, engine='python', header=0)
        
        # Read the header to determine tax category
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if len(lines) >= 3:
                tax_category_line = lines[2].strip()
                if 'Taxes on goods and services' in tax_category_line:
                    file_metadata['tax_category'] = 'TAX5000'
                elif 'Taxes on exports' in tax_category_line:
                    file_metadata['tax_category'] = 'TAX5124'
        
        df.rename(columns={df.columns[0]: 'Country'}, inplace=True)
        # Drop the second column which is always empty/junk
        if len(df.columns) > 1 and 'Unnamed: 1' in df.columns:
             df.drop(columns=df.columns[1], inplace=True)
        df.set_index('Country', inplace=True)
        
        df_transposed = df.transpose().dropna(how='all')
        
        valid_years = pd.to_numeric(df_transposed.index, errors='coerce').dropna().astype(int)
        year_set.update(valid_years)

        for original_country_col, series in df_transposed.items():
            cleaned_country_name = clean_name(original_country_col)
            country_data[cleaned_country_name] = series
            file_metadata['countries_found'].append(cleaned_country_name)

        print(f"  Successfully extracted data for {len(country_data)} entities from: {filepath}")
        print(f"  File contains tax category: {file_metadata['tax_category']}")
        return country_data, file_metadata

    except FileNotFoundError:
        print(f"Warning: Source file not found: {filepath}")
        return {}, file_metadata
    except Exception as e:
        print(f"An error occurred while processing {filepath}: {e}")
        return {}, file_metadata

def auto_discover_source_files():
    """
    Automatically discovers and categorizes source files based on their content.
    Scans all CSV files in the current directory — no hardcoded filenames or dates.
    Returns a mapping of data types to file paths for universal mapping.
    """
    import os

    source_mapping = {
        'RSLACT_TAX5000': None,
        'REVOECD_TAX5000': None,
        'RSLACT_TAX5124': None
    }

    # Scan downloads/ for intermediate CSVs; skip the final output file
    source_dir = 'downloads'
    output_files = {'OECD_TAX_REVENUE.csv'}
    candidate_files = [
        f for f in os.listdir(source_dir)
        if f.endswith('.csv') and f not in output_files
    ]

    print("Auto-discovering source files...")
    for filename in candidate_files:
        filepath = os.path.join(source_dir, filename)
        if not os.path.exists(filepath):
            continue
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if len(lines) >= 4:
                    tax_category_line = lines[2].strip()
                    institutional_sector = lines[1].strip() if len(lines) > 1 else ''
                    
                    # Determine the data type based on content
                    if 'Taxes on goods and services' in tax_category_line:
                        if 'Latin American' in institutional_sector or 'Revenue Statistics in Latin America' in lines[0]:
                            source_mapping['RSLACT_TAX5000'] = filepath
                            print(f"  Found RSLACT TAX5000 source: {filepath}")
                        elif 'OECD' in institutional_sector or any('OECD' in line for line in lines[:5]):
                            source_mapping['REVOECD_TAX5000'] = filepath
                            print(f"  Found REVOECD TAX5000 source: {filepath}")
                    elif 'Taxes on exports' in tax_category_line:
                        if 'Latin American' in institutional_sector or 'Revenue Statistics in Latin America' in lines[0]:
                            source_mapping['RSLACT_TAX5124'] = filepath
                            print(f"  Found RSLACT TAX5124 source: {filepath}")
                            
        except Exception as e:
            print(f"  Could not analyze {filepath}: {e}")
            continue
    
    return source_mapping

# --- 3. Main Execution Logic ---

def map_to_output() -> pd.DataFrame:
    """
    Run the full mapping pipeline and return the AfricaAI 2-header-row DataFrame.
    Row 0 = codes, Row 1 = descriptions, Row 2+ = annual data.
    Also writes output/OECD_TAX_REVENUE.csv as a side-effect.
    Called by main.py.
    """
    _pool = {}
    _years = set()

    print("Step 1: Auto-discovering and reading all source files...")
    _sources = auto_discover_source_files()

    required = ['RSLACT_TAX5000', 'REVOECD_TAX5000', 'RSLACT_TAX5124']
    missing = [s for s in required if _sources[s] is None]
    if missing:
        raise FileNotFoundError(f"Could not find source files for: {missing}")

    _source_data = {}
    for src_type, filepath in _sources.items():
        if filepath:
            data, _ = extract_data_from_file(filepath, _years)
            _source_data[src_type] = data
            print(f"  Loaded {src_type} from: {filepath}")

    print("\nStep 2: Universal mapping...")
    for code, desc in BLUEPRINT_HEADERS:
        country = clean_name(desc)
        matched = src = None
        if ".RSLACT.TAX5000." in code:
            matched = robust_country_match(country, list(_source_data['RSLACT_TAX5000'].keys()))
            src = 'RSLACT_TAX5000'
        elif ".REVOECD.TAX5000." in code:
            matched = robust_country_match(country, list(_source_data['REVOECD_TAX5000'].keys()))
            src = 'REVOECD_TAX5000'
        elif ".RSLACT.TAX5124." in code:
            matched = robust_country_match(country, list(_source_data['RSLACT_TAX5124'].keys()))
            src = 'RSLACT_TAX5124'
        if matched and src:
            _pool[code] = _source_data[src][matched]

    if not _years:
        raise RuntimeError("No valid year data found in source files.")

    min_year, max_year = min(_years), max(_years)
    master_index = pd.RangeIndex(start=min_year, stop=max_year + 1, name='Year')
    print(f"Year range: {min_year}–{max_year}")

    codes = [t[0] for t in BLUEPRINT_HEADERS]
    descs = [t[1] for t in BLUEPRINT_HEADERS]
    final_df = pd.DataFrame(index=master_index)
    for c in codes:
        if c in _pool:
            s = _pool[c].copy()
            s.index = pd.to_numeric(s.index, errors='coerce')
            final_df[c] = s.reindex(master_index)
        else:
            final_df[c] = pd.NA
    final_df = final_df.replace('..', pd.NA)

    # Build AfricaAI 2-header-row DataFrame
    clean_descs = [d.strip('"') for d in descs]
    header_codes = [None] + codes
    header_descs = [None] + clean_descs
    data_rows = []
    for year in final_df.index:
        row = [str(year)]
        for c in codes:
            val = final_df.loc[year, c]
            row.append(None if pd.isna(val) else float(val))
        data_rows.append(row)

    out_df = pd.DataFrame([header_codes, header_descs] + data_rows)

    # Also write the legacy CSV output
    os.makedirs('output', exist_ok=True)
    csv_path = os.path.join('output', 'OECD_TAX_REVENUE.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        f.write(','.join([''] + codes) + '\n')
        f.write(','.join([''] + [f'"{d}"' for d in clean_descs]) + '\n')
        for row in data_rows:
            f.write(','.join(['' if v is None else str(v) for v in row]) + '\n')
    print(f"CSV written: {csv_path}")

    return out_df


def build_metadata_rows() -> list:
    """Return metadata dicts for the META xlsx file."""
    meta = []
    for code, desc in BLUEPRINT_HEADERS:
        meta.append({
            "CODE":              code,
            "DESCRIPTION":       desc.strip('"'),
            "FREQUENCY":         "Annual",
            "UNIT":              "% of GDP",
            "SOURCE_NAME":       "OECD Revenue Statistics",
            "SOURCE_URL":        "https://data-explorer.oecd.org",
            "DATASET":           "OECD_TAXREVENUE",
            "NEXT_RELEASE_DATE": "",
        })
    return meta


if __name__ == "__main__":
 all_data_series_pool = {}
 all_years_found = set()

 print("Step 1: Auto-discovering and reading all source files...")
 source_files = auto_discover_source_files()

 # Verify we found all required sources
 required_sources = ['RSLACT_TAX5000', 'REVOECD_TAX5000', 'RSLACT_TAX5124']
 missing_sources = [src for src in required_sources if source_files[src] is None]
 if missing_sources:
     print(f"ERROR: Could not find source files for: {missing_sources}")
     exit()

 # Read data from discovered files
 source_data = {}
 for source_type, filepath in source_files.items():
     if filepath:
         data, metadata = extract_data_from_file(filepath, all_years_found)
         source_data[source_type] = data
         print(f"  Loaded {source_type} from: {filepath}")

 print("All source data extracted using universal discovery.")

 print("\nStep 2: Universal mapping using discovered sources with robust country matching...")
 mapping_stats = {'matched': 0, 'unmatched': 0, 'unmatched_countries': []}

 for identifier_code, description in BLUEPRINT_HEADERS:
     target_country_name = clean_name(description)
     matched_country = None
     source_to_use = None

     if ".RSLACT.TAX5000." in identifier_code:
         matched_country = robust_country_match(target_country_name, list(source_data['RSLACT_TAX5000'].keys()))
         source_to_use = 'RSLACT_TAX5000'
     elif ".REVOECD.TAX5000." in identifier_code:
         matched_country = robust_country_match(target_country_name, list(source_data['REVOECD_TAX5000'].keys()))
         source_to_use = 'REVOECD_TAX5000'
     elif ".RSLACT.TAX5124." in identifier_code:
         matched_country = robust_country_match(target_country_name, list(source_data['RSLACT_TAX5124'].keys()))
         source_to_use = 'RSLACT_TAX5124'

     if matched_country and source_to_use:
         all_data_series_pool[identifier_code] = source_data[source_to_use][matched_country]
         mapping_stats['matched'] += 1
     else:
         mapping_stats['unmatched'] += 1
         mapping_stats['unmatched_countries'].append(f"{target_country_name} ({identifier_code})")

 print(f"Universal source mapping complete: {mapping_stats['matched']} matched, {mapping_stats['unmatched']} unmatched.")
 if mapping_stats['unmatched'] > 0:
     print(f"Unmatched countries: {mapping_stats['unmatched_countries'][:5]}...")

 if not all_years_found:
     print("\nFATAL ERROR: No valid year data was found. Please check source files.")
     exit()

 print("\nStep 3: Dynamically determining the full range of years...")
 min_year, max_year = min(all_years_found), max(all_years_found)
 master_index = pd.RangeIndex(start=min_year, stop=max_year + 1, name='Year')
 print(f"Year range determined: {min_year} to {max_year}.")

 print("\nStep 4: Building the final table from the mapped data...")
 header_row1_codes = [t[0] for t in BLUEPRINT_HEADERS]
 header_row2_descs = [t[1] for t in BLUEPRINT_HEADERS]

 final_df = pd.DataFrame(index=master_index)

 for code in header_row1_codes:
     if code in all_data_series_pool:
         series = all_data_series_pool[code]
         series.index = pd.to_numeric(series.index, errors='coerce')
         final_df[code] = series.reindex(master_index)
     else:
         final_df[code] = pd.NA
 print("Data has been assembled into the blueprint structure.")

 print("\nStep 5: Creating custom CSV output to match desired format...")
 final_df = final_df.replace('..', 'NA')

 os.makedirs('output', exist_ok=True)
 output_filename = os.path.join('output', 'OECD_TAX_REVENUE.csv')
 with open(output_filename, 'w', newline='', encoding='utf-8') as f:
     f.write(','.join([''] + header_row1_codes) + '\n')
     clean_descriptions = [desc.strip('"') for desc in header_row2_descs]
     f.write(','.join([''] + [f'"{desc}"' for desc in clean_descriptions]) + '\n')
     for year in final_df.index:
         row_data = [str(year)]
         for code in header_row1_codes:
             if code in final_df.columns:
                 value = final_df.loc[year, code]
                 row_data.append('' if pd.isna(value) else str(value))
             else:
                 row_data.append('')
         f.write(','.join(row_data) + '\n')

 print("Custom CSV output created to match desired format.")
 print(f"\nProcessing finished successfully.")
 print(f"The final, self-contained data file has been saved to: {output_filename}")