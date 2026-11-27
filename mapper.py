import io
import zipfile
import pandas as pd
import re
import os
from datetime import datetime


def _read_xlsx_patched(path) -> pd.DataFrame:
    """
    Read an xlsx file whose styles.xml contains bare <fill/> elements that
    openpyxl cannot parse.  Patches the styles in-memory before handing the
    buffer to pandas so the rest of the logic is unchanged.
    Returns the full sheet as a DataFrame with header=None.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(path, 'r') as zin:
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename == 'xl/styles.xml':
                    # Replace empty <fill/> with a valid fill element
                    data = data.replace(
                        b'<fill/>',
                        b'<fill><patternFill patternType="none"/></fill>'
                    )
                zout.writestr(item, data)
    buf.seek(0)
    return pd.read_excel(buf, sheet_name=0, header=None)

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
    Reads a CSV or xlsx source file and returns a clean dictionary mapping
    cleaned country names to their data series.  Updates year_set in-place.
    xlsx files with broken styles.xml are patched transparently.
    """
    country_data = {}
    file_metadata = {'tax_category': None, 'countries_found': []}

    try:
        is_xlsx = filepath.lower().endswith('.xlsx')

        if is_xlsx:
            df_raw = _read_xlsx_patched(filepath)
            # OECD xlsx layout:
            #   Col 0: always empty; Col 1: labels / country names; Cols 3+: year values
            #   Row 0-4: metadata; Row 5: year header; Row 6: "Reference area"; Rows 7+: data
            l2 = str(df_raw.iloc[2, 1]) if len(df_raw) > 2 else ''
            if 'Taxes on goods and services' in l2:
                file_metadata['tax_category'] = 'TAX5000'
            elif 'Taxes on exports' in l2:
                file_metadata['tax_category'] = 'TAX5124'

            # Build col_index → year string from row 5 (years start at col 3)
            year_header_row = df_raw.iloc[5]
            year_map = {}
            for ci in range(3, len(year_header_row)):
                v = year_header_row.iloc[ci]
                if pd.notna(v):
                    try:
                        year_map[ci] = str(int(float(v)))
                    except (ValueError, TypeError):
                        pass

            # Data rows: row 7 to len-3 (skip row 6 sub-header; skip last 2 footer rows)
            for ri in range(7, len(df_raw) - 2):
                row = df_raw.iloc[ri]
                country_raw = str(row.iloc[1]).strip()
                if not country_raw or country_raw in ('nan', 'NaN', 'None', ''):
                    continue
                series_dict = {yr: (None if pd.isna(row.iloc[ci]) else float(row.iloc[ci]))
                               for ci, yr in year_map.items() if ci < len(row)}
                s = pd.Series(series_dict)
                cleaned = clean_name(country_raw)
                country_data[cleaned] = s
                file_metadata['countries_found'].append(cleaned)

            year_set.update(int(y) for y in year_map.values())

            print(f"  Successfully extracted data for {len(country_data)} entities from: {filepath}")
            print(f"  File contains tax category: {file_metadata['tax_category']}")
            return country_data, file_metadata

        else:
            df_data = pd.read_csv(filepath, skiprows=5, skipfooter=2, engine='python', header=0)
            with open(filepath, 'r', encoding='utf-8') as fh:
                lines = fh.readlines()
            l2 = lines[2].strip() if len(lines) > 2 else ''
            if 'Taxes on goods and services' in l2:
                file_metadata['tax_category'] = 'TAX5000'
            elif 'Taxes on exports' in l2:
                file_metadata['tax_category'] = 'TAX5124'

        df_data.rename(columns={df_data.columns[0]: 'Country'}, inplace=True)
        # Drop the second column (always empty / duplicate "Time period" / "Unnamed: 1")
        if len(df_data.columns) > 1:
            df_data.drop(columns=df_data.columns[1], inplace=True)
        df_data.set_index('Country', inplace=True)

        df_transposed = df_data.transpose().dropna(how='all')

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

def _classify_file(line0: str, line1: str, line2: str) -> str | None:
    """Return source type key from the first 3 metadata lines, or None."""
    is_lac = 'Latin America' in line0 or 'Latin America' in line1
    is_oecd = not is_lac and ('OECD' in line1 or 'OECD' in line0)
    if 'Taxes on goods and services' in line2:
        if is_lac:
            return 'RSLACT_TAX5000'
        if is_oecd:
            return 'REVOECD_TAX5000'
    elif 'Taxes on exports' in line2:
        if is_lac:
            return 'RSLACT_TAX5124'
    return None


def auto_discover_source_files():
    """
    Scan downloads/ for CSV and xlsx source files.
    Prioritizes the latest file for each category based on the timestamp in the
    filename. Falls back to file modification time if no timestamp is found.
    """
    source_mapping = {
        'RSLACT_TAX5000': None,
        'REVOECD_TAX5000': None,
        'RSLACT_TAX5124': None
    }

    source_dir = 'downloads'
    skip = {'OECD_TAX_REVENUE.csv'}

    all_candidates = []

    # Gather all potential files
    for filename in os.listdir(source_dir):
        if filename in skip:
            continue
        if not (filename.endswith('.csv') or filename.endswith('.xlsx')):
            continue
        
        filepath = os.path.join(source_dir, filename)
        try:
            src_type = None
            if filename.endswith('.csv'):
                with open(filepath, 'r', encoding='utf-8') as fh:
                    lines = [fh.readline() for _ in range(3)]
                if len(lines) >= 3:
                    src_type = _classify_file(lines[0], lines[1], lines[2])
            else:
                df_head = _read_xlsx_patched(filepath)
                col = 1 if df_head.shape[1] > 1 else 0
                l0 = str(df_head.iloc[0, col]) if len(df_head) > 0 else ''
                l1 = str(df_head.iloc[1, col]) if len(df_head) > 1 else ''
                l2 = str(df_head.iloc[2, col]) if len(df_head) > 2 else ''
                src_type = _classify_file(l0, l1, l2)
            
            if src_type:
                # Extract timestamp from filename if possible (YYYY-MM-DD HH-MM-SS or similar)
                # Examples:
                # ...2026-06-16 11-41-26.xlsx
                # ...2026-06-16_09-45-03_main.csv
                ts_match = re.search(r'(\d{4}-\d{2}-\d{2}[ _]\d{2}[-_]\d{2}[-_]\d{2})', filename)
                ts_val = ts_match.group(1).replace('_', ' ').replace('-', ':') if ts_match else ""
                
                # If no timestamp in name, use file mtime
                mtime = os.path.getmtime(filepath)
                
                all_candidates.append({
                    'type': src_type,
                    'path': filepath,
                    'timestamp': ts_val,
                    'mtime': mtime,
                    'is_csv': filename.endswith('.csv')
                })
        except Exception as e:
            print(f"  Could not analyze {filepath}: {e}")

    # For each type, pick the best candidate (latest timestamp, then latest mtime, prefer CSV if same time)
    for src_type in source_mapping.keys():
        type_candidates = [c for c in all_candidates if c['type'] == src_type]
        if not type_candidates:
            continue
        
        # Sort by timestamp string desc, then mtime desc, then is_csv desc
        type_candidates.sort(key=lambda x: (x['timestamp'], x['mtime'], x['is_csv']), reverse=True)
        best = type_candidates[0]
        source_mapping[src_type] = best['path']
        print(f"  Selected {src_type}: {best['path']} (TS: {best['timestamp'] or 'N/A'})")

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

    required = ['RSLACT_TAX5000', 'REVOECD_TAX5000']
    missing = [s for s in required if _sources[s] is None]
    if missing:
        raise FileNotFoundError(f"Could not find source files for: {missing}")
    if _sources.get('RSLACT_TAX5124') is None:
        print("  Warning: RSLACT_TAX5124 (Taxes on exports) not available — columns will be empty")

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
        elif ".RSLACT.TAX5124." in code and 'RSLACT_TAX5124' in _source_data:
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
    try:
        map_to_output()
        print("\nMapping finished successfully.")
    except PermissionError:
        print("\nERROR: Could not write output file (Permission Denied). "
              "Please ensure output/OECD_TAX_REVENUE.csv is not open in Excel or another program.")
    except Exception as e:
        print(f"\nERROR: Mapping failed: {e}")
        import traceback
        traceback.print_exc()