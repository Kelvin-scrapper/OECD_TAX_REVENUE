#!/usr/bin/env python3
"""
Universal Excel to CSV Converter for OECD Files
Handles various Excel formats and structures, captures all data comprehensively
"""

import xlwings as xw
import pandas as pd
from pathlib import Path
import logging
import time
import argparse
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UniversalExcelConverter:
    def __init__(self, source_dir=".", output_dir=None, file_patterns=None):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir) if output_dir else self.source_dir
        self.file_patterns = file_patterns or ['*.xlsx', '*.xls']
        
        # Ensure output directory exists
        self.output_dir.mkdir(exist_ok=True)
        
        # Conversion statistics
        self.stats = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'conversion_details': []
        }
    
    def find_excel_files(self):
        """Find all Excel files matching patterns"""
        excel_files = []
        
        for pattern in self.file_patterns:
            files = list(self.source_dir.rglob(pattern))
            excel_files.extend(files)
        
        # Filter out temporary/backup files
        excel_files = [f for f in excel_files if not f.name.startswith('~') and not f.name.startswith('.')]
        
        logger.info(f"Found {len(excel_files)} Excel files to process")
        return excel_files
    
    def analyze_workbook_structure(self, wb):
        """Analyze workbook structure to determine best conversion strategy"""
        structure_info = {
            'sheets': [],
            'main_sheet': None,
            'total_cells': 0,
            'data_density': 0
        }
        
        try:
            for sheet in wb.sheets:
                sheet_info = {
                    'name': sheet.name,
                    'used_range': None,
                    'rows': 0,
                    'cols': 0,
                    'data_points': 0
                }
                
                try:
                    # Get used range safely
                    used_range = sheet.used_range
                    if used_range:
                        sheet_info['used_range'] = f"{used_range.address}"
                        sheet_info['rows'] = used_range.rows.count
                        sheet_info['cols'] = used_range.columns.count
                        
                        # Estimate data density by sampling
                        sample_data = used_range.value
                        if sample_data:
                            if isinstance(sample_data, list):
                                # Count non-empty cells
                                data_points = 0
                                if isinstance(sample_data[0], list):
                                    # 2D array
                                    for row in sample_data:
                                        for cell in row:
                                            if cell is not None and str(cell).strip():
                                                data_points += 1
                                else:
                                    # 1D array
                                    data_points = sum(1 for cell in sample_data if cell is not None and str(cell).strip())
                                sheet_info['data_points'] = data_points
                            else:
                                # Single value
                                sheet_info['data_points'] = 1 if sample_data is not None else 0
                    
                except Exception as e:
                    logger.debug(f"Could not analyze sheet {sheet.name}: {e}")
                    sheet_info['error'] = str(e)
                
                structure_info['sheets'].append(sheet_info)
                structure_info['total_cells'] += sheet_info['data_points']
            
            # Determine main sheet (largest by data points, then by cell count)
            if structure_info['sheets']:
                main_sheet = max(structure_info['sheets'], 
                               key=lambda s: (s['data_points'], s['rows'] * s['cols']))
                structure_info['main_sheet'] = main_sheet['name']
                structure_info['data_density'] = structure_info['total_cells']
            
        except Exception as e:
            logger.error(f"Error analyzing workbook structure: {e}")
        
        return structure_info
    
    def convert_sheet_to_csv(self, sheet, output_path, sheet_name=""):
        """Convert a single sheet to CSV with comprehensive data capture"""
        try:
            logger.info(f"Converting sheet '{sheet_name}' to {output_path.name}")
            
            # Try to get the used range first
            used_range = sheet.used_range
            
            if not used_range:
                logger.warning(f"Sheet '{sheet_name}' appears to be empty")
                return False
            
            # Get all data from used range
            data = used_range.value
            
            if data is None:
                logger.warning(f"No data found in sheet '{sheet_name}'")
                return False
            
            # Convert to standardized 2D list format
            if not isinstance(data, list):
                # Single cell
                data = [[data]]
            elif len(data) > 0 and not isinstance(data[0], (list, tuple)):
                # Single row/column
                data = [data]
            
            # Write to CSV with comprehensive error handling
            import csv
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                
                rows_written = 0
                for row in data:
                    if row is None:
                        continue
                    
                    # Handle single values vs lists
                    if not isinstance(row, (list, tuple)):
                        row = [row]
                    
                    # Convert all values to strings, handling None values
                    clean_row = []
                    for cell in row:
                        if cell is None:
                            clean_row.append('')
                        elif isinstance(cell, (int, float)):
                            # Preserve numeric precision
                            if isinstance(cell, float) and cell.is_integer():
                                clean_row.append(str(int(cell)))
                            else:
                                clean_row.append(str(cell))
                        else:
                            clean_row.append(str(cell).strip())
                    
                    writer.writerow(clean_row)
                    rows_written += 1
                    
                    # Progress indicator for large files
                    if rows_written % 1000 == 0:
                        logger.debug(f"Written {rows_written} rows to {output_path.name}")
            
            logger.info(f"Successfully converted sheet '{sheet_name}': {rows_written} rows")
            return True
            
        except Exception as e:
            logger.error(f"Error converting sheet '{sheet_name}': {e}")
            return False
    
    def convert_excel_file(self, excel_path):
        """Convert Excel file with multiple strategies"""
        conversion_detail = {
            'file': excel_path.name,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'sheets_converted': [],
            'errors': [],
            'structure': None
        }
        
        try:
            logger.info(f"Processing Excel file: {excel_path.name}")
            
            # Create Excel application instance
            app = xw.App(visible=False, add_book=False)
            
            try:
                # Open workbook with error handling
                wb = app.books.open(str(excel_path))
                
                # Analyze structure
                structure = self.analyze_workbook_structure(wb)
                conversion_detail['structure'] = structure
                logger.info(f"Workbook structure: {len(structure['sheets'])} sheets, main sheet: {structure.get('main_sheet', 'None')}")
                
                # Create base output filename
                base_name = excel_path.stem
                # Clean filename for filesystem compatibility
                safe_name = "".join(c for c in base_name if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
                safe_name = safe_name.replace(' ', '_')
                
                sheets_converted = 0
                
                # Convert all sheets or just main sheet based on structure
                if len(structure['sheets']) == 1:
                    # Single sheet - convert directly
                    sheet = wb.sheets[0]
                    output_path = self.output_dir / f"{safe_name}.csv"
                    if self.convert_sheet_to_csv(sheet, output_path, sheet.name):
                        conversion_detail['sheets_converted'].append({
                            'name': sheet.name,
                            'output': output_path.name,
                            'success': True
                        })
                        sheets_converted += 1
                else:
                    # Multiple sheets - convert main sheet and optionally others
                    main_sheet_name = structure.get('main_sheet')
                    
                    # Convert main sheet
                    if main_sheet_name:
                        try:
                            main_sheet = wb.sheets[main_sheet_name]
                            output_path = self.output_dir / f"{safe_name}_main.csv"
                            if self.convert_sheet_to_csv(main_sheet, output_path, main_sheet_name):
                                conversion_detail['sheets_converted'].append({
                                    'name': main_sheet_name,
                                    'output': output_path.name,
                                    'success': True,
                                    'is_main': True
                                })
                                sheets_converted += 1
                        except Exception as e:
                            logger.error(f"Error converting main sheet '{main_sheet_name}': {e}")
                            conversion_detail['errors'].append(f"Main sheet error: {e}")
                    
                    # Convert other significant sheets
                    for sheet_info in structure['sheets']:
                        if sheet_info['name'] != main_sheet_name and sheet_info['data_points'] > 100:
                            try:
                                sheet = wb.sheets[sheet_info['name']]
                                sheet_safe_name = "".join(c for c in sheet_info['name'] if c.isalnum() or c in (' ', '-', '_')).strip().replace(' ', '_')
                                output_path = self.output_dir / f"{safe_name}_{sheet_safe_name}.csv"
                                if self.convert_sheet_to_csv(sheet, output_path, sheet_info['name']):
                                    conversion_detail['sheets_converted'].append({
                                        'name': sheet_info['name'],
                                        'output': output_path.name,
                                        'success': True
                                    })
                                    sheets_converted += 1
                            except Exception as e:
                                logger.debug(f"Error converting sheet '{sheet_info['name']}': {e}")
                
                conversion_detail['success'] = sheets_converted > 0
                
            finally:
                # Always clean up
                try:
                    wb.close()
                except:
                    pass
                try:
                    app.quit()
                except:
                    pass
                
                # Small delay to ensure cleanup
                time.sleep(0.5)
            
            if conversion_detail['success']:
                logger.info(f"Successfully converted {excel_path.name}: {sheets_converted} sheets")
                self.stats['successful'] += 1
            else:
                logger.error(f"Failed to convert any sheets from {excel_path.name}")
                self.stats['failed'] += 1
                
        except Exception as e:
            logger.error(f"Critical error processing {excel_path.name}: {e}")
            conversion_detail['errors'].append(f"Critical error: {e}")
            self.stats['failed'] += 1
        
        self.stats['conversion_details'].append(conversion_detail)
        return conversion_detail['success']
    
    def convert_all_files(self):
        """Convert all Excel files in the source directory"""
        logger.info(f"Starting universal Excel conversion from {self.source_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        
        excel_files = self.find_excel_files()
        self.stats['total_files'] = len(excel_files)
        
        if not excel_files:
            logger.warning("No Excel files found to convert")
            return self.stats
        
        # Convert each file
        for i, excel_file in enumerate(excel_files, 1):
            logger.info(f"Converting file {i}/{len(excel_files)}: {excel_file.name}")
            self.convert_excel_file(excel_file)
            
            # Progress update
            if i % 5 == 0 or i == len(excel_files):
                logger.info(f"Progress: {i}/{len(excel_files)} files processed")
        
        # Save conversion report
        self.save_conversion_report()
        
        logger.info(f"Conversion complete: {self.stats['successful']}/{self.stats['total_files']} files successful")
        return self.stats
    
    def save_conversion_report(self):
        """Save detailed conversion report"""
        report_path = self.output_dir / f"conversion_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, indent=2, default=str)
            logger.info(f"Conversion report saved to: {report_path}")
        except Exception as e:
            logger.error(f"Could not save conversion report: {e}")

def main():
    parser = argparse.ArgumentParser(description='Universal Excel to CSV Converter for OECD Files')
    parser.add_argument('--source', '-s', default='.', 
                       help='Source directory containing Excel files (default: current directory)')
    parser.add_argument('--output', '-o', 
                       help='Output directory for CSV files (default: same as source)')
    parser.add_argument('--patterns', '-p', nargs='*', default=['*.xlsx', '*.xls'],
                       help='File patterns to match (default: *.xlsx *.xls)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create converter and run
    converter = UniversalExcelConverter(
        source_dir=args.source,
        output_dir=args.output,
        file_patterns=args.patterns
    )
    
    stats = converter.convert_all_files()
    
    # Print summary
    print(f"\n=== CONVERSION SUMMARY ===")
    print(f"Total files: {stats['total_files']}")
    print(f"Successful: {stats['successful']}")
    print(f"Failed: {stats['failed']}")
    print(f"Success rate: {stats['successful']/stats['total_files']*100:.1f}%" if stats['total_files'] > 0 else "No files processed")
    
    # Print successful conversions
    if stats['successful'] > 0:
        print(f"\nSuccessfully converted files:")
        for detail in stats['conversion_details']:
            if detail['success']:
                print(f"  - {detail['file']}: {len(detail['sheets_converted'])} sheets")
                for sheet in detail['sheets_converted']:
                    print(f"    * {sheet['name']} -> {sheet['output']}")

if __name__ == "__main__":
    main()