#!/usr/bin/env python3
"""
OECD Tax Revenue Data Orchestrator

This script orchestrates the complete data pipeline:
1. Downloads OECD tax data (main.py)
2. Converts Excel files to CSV (universal_excel_converter.py)
3. Maps and processes the data (final_corrected_mapper.py)

Usage:
    python orchestrator.py [options]
"""

import os
import sys
import time
import json
import logging
import argparse
import subprocess
from datetime import datetime
from pathlib import Path

# ============================================================================
# DIRECTORY SETUP
# ============================================================================
os.makedirs('downloads', exist_ok=True)
os.makedirs('logs', exist_ok=True)
os.makedirs('output', exist_ok=True)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/orchestrator_{timestamp}.log'),
        logging.FileHandler('orchestrator.log'),   # keep rolling summary log
    ]
)
logger = logging.getLogger('Orchestrator')

class OECDDataOrchestrator:
    def __init__(self, base_dir=None, skip_download=False, skip_conversion=False, skip_mapping=False):
        """
        Initialize the orchestrator
        
        Args:
            base_dir: Base directory for operations (default: current directory)
            skip_download: Skip the download step
            skip_conversion: Skip the Excel to CSV conversion step
            skip_mapping: Skip the data mapping step
        """
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.download_dir = self.base_dir / "downloads"
        self.skip_download = skip_download
        self.skip_conversion = skip_conversion
        self.skip_mapping = skip_mapping
        
        # Execution tracking
        self.execution_log = {
            'start_time': datetime.now().isoformat(),
            'steps': {},
            'errors': [],
            'final_outputs': []
        }
        
        # Script paths
        self.scripts = {
            'downloader': self.base_dir / 'main.py',
            'converter': self.base_dir / 'universal_excel_converter.py',
            'mapper': self.base_dir / 'final_corrected_mapper.py'
        }
        
        # Validate script existence
        self.validate_scripts()
    
    def validate_scripts(self):
        """Validate that all required scripts exist"""
        missing_scripts = []
        for name, path in self.scripts.items():
            if not path.exists():
                missing_scripts.append(f"{name}: {path}")
        
        if missing_scripts:
            error_msg = f"Missing required scripts:\n" + "\n".join(missing_scripts)
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        logger.info("All required scripts found")
    
    def run_script(self, script_name, script_path, args=None, cwd=None):
        """
        Run a Python script and capture its output
        
        Args:
            script_name: Name for logging purposes
            script_path: Path to the script
            args: Additional command line arguments
            cwd: Working directory (default: base_dir)
        
        Returns:
            tuple: (success: bool, duration: float, output: str)
        """
        start_time = time.time()
        cwd = cwd or self.base_dir
        
        cmd = [sys.executable, str(script_path)]
        if args:
            cmd.extend(args)
        
        logger.info(f"Starting {script_name}...")
        logger.info(f"Command: {' '.join(cmd)}")
        logger.info(f"Working directory: {cwd}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )
            
            duration = time.time() - start_time
            
            if result.returncode == 0:
                logger.info(f"{script_name} completed successfully in {duration:.2f} seconds")
                if result.stdout:
                    logger.debug(f"{script_name} stdout:\n{result.stdout}")
                return True, duration, result.stdout
            else:
                logger.error(f"{script_name} failed with return code {result.returncode}")
                logger.error(f"{script_name} stderr:\n{result.stderr}")
                if result.stdout:
                    logger.debug(f"{script_name} stdout:\n{result.stdout}")
                return False, duration, result.stderr
                
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            error_msg = f"{script_name} timed out after {duration:.2f} seconds"
            logger.error(error_msg)
            return False, duration, error_msg
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"{script_name} failed with exception: {str(e)}"
            logger.error(error_msg)
            return False, duration, error_msg
    
    def step_1_download_data(self):
        """Step 1: Download OECD tax data"""
        if self.skip_download:
            logger.info("Skipping download step as requested")
            self.execution_log['steps']['download'] = {'status': 'skipped', 'reason': 'user_request'}
            return True
        
        logger.info("=" * 60)
        logger.info("STEP 1: DOWNLOADING OECD TAX DATA")
        logger.info("=" * 60)
        
        success, duration, output = self.run_script("Data Downloader", self.scripts['downloader'])
        
        self.execution_log['steps']['download'] = {
            'status': 'success' if success else 'failed',
            'duration': duration,
            'output': output[:1000]  # First 1000 chars
        }
        
        if success:
            # Check if download directory exists and has files
            if self.download_dir.exists():
                excel_files = list(self.download_dir.glob('*.xlsx')) + list(self.download_dir.glob('*.xls'))
                logger.info(f"Found {len(excel_files)} Excel files in download directory")
                self.execution_log['steps']['download']['files_downloaded'] = len(excel_files)
            else:
                logger.warning("Download directory not found")
                return False
        else:
            self.execution_log['errors'].append(f"Download step failed: {output}")
        
        return success
    
    def step_2_convert_excel_to_csv(self):
        """Step 2: Convert Excel files to CSV"""
        if self.skip_conversion:
            logger.info("Skipping conversion step as requested")
            self.execution_log['steps']['conversion'] = {'status': 'skipped', 'reason': 'user_request'}
            return True
        
        logger.info("=" * 60)
        logger.info("STEP 2: CONVERTING EXCEL FILES TO CSV")
        logger.info("=" * 60)
        
        # Intermediate CSVs go into downloads/ alongside the Excel files
        args = [
            '--source', str(self.download_dir),
            '--output', str(self.download_dir),
            '--verbose'
        ]
        
        success, duration, output = self.run_script("Excel Converter", self.scripts['converter'], args)
        
        self.execution_log['steps']['conversion'] = {
            'status': 'success' if success else 'failed',
            'duration': duration,
            'output': output[:1000]
        }
        
        if success:
            # Check for CSV files in downloads/
            csv_files = list(self.download_dir.glob('*.csv'))
            logger.info(f"Found {len(csv_files)} CSV files after conversion")
            self.execution_log['steps']['conversion']['csv_files_created'] = len(csv_files)
        else:
            self.execution_log['errors'].append(f"Conversion step failed: {output}")
        
        return success
    
    def step_3_map_and_process_data(self):
        """Step 3: Map and process the data"""
        if self.skip_mapping:
            logger.info("Skipping mapping step as requested")
            self.execution_log['steps']['mapping'] = {'status': 'skipped', 'reason': 'user_request'}
            return True
        
        logger.info("=" * 60)
        logger.info("STEP 3: MAPPING AND PROCESSING DATA")
        logger.info("=" * 60)
        
        success, duration, output = self.run_script("Data Mapper", self.scripts['mapper'])
        
        self.execution_log['steps']['mapping'] = {
            'status': 'success' if success else 'failed',
            'duration': duration,
            'output': output[:1000]
        }
        
        if success:
            # Check for the final output file
            final_output = self.base_dir / 'output' / 'OECD_TAX_REVENUE.csv'
            if final_output.exists():
                logger.info(f"Final output file created: {final_output}")
                self.execution_log['final_outputs'].append(str(final_output))
            else:
                logger.warning("Expected final output file not found")
        else:
            self.execution_log['errors'].append(f"Mapping step failed: {output}")
        
        return success
    
    def generate_execution_report(self):
        """Generate a comprehensive execution report"""
        self.execution_log['end_time'] = datetime.now().isoformat()
        
        start_time = datetime.fromisoformat(self.execution_log['start_time'])
        end_time = datetime.fromisoformat(self.execution_log['end_time'])
        total_duration = (end_time - start_time).total_seconds()
        
        self.execution_log['total_duration'] = total_duration
        self.execution_log['success'] = len(self.execution_log['errors']) == 0
        
        # Save execution log
        log_file = self.base_dir / "logs" / f"execution_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(self.execution_log, f, indent=2, ensure_ascii=False)
            logger.info(f"Execution log saved to: {log_file}")
        except Exception as e:
            logger.error(f"Failed to save execution log: {str(e)}")
        
        # Print summary
        logger.info("=" * 60)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total duration: {total_duration:.2f} seconds")
        logger.info(f"Overall success: {self.execution_log['success']}")
        
        for step_name, step_info in self.execution_log['steps'].items():
            status = step_info['status']
            duration = step_info.get('duration', 0)
            logger.info(f"  {step_name.capitalize()}: {status} ({duration:.2f}s)")
        
        if self.execution_log['errors']:
            logger.error(f"Errors encountered: {len(self.execution_log['errors'])}")
            for error in self.execution_log['errors']:
                logger.error(f"  - {error}")
        
        if self.execution_log['final_outputs']:
            logger.info("Final output files:")
            for output_file in self.execution_log['final_outputs']:
                logger.info(f"  ✓ {output_file}")
    
    def run_pipeline(self):
        """Run the complete data pipeline"""
        logger.info("Starting OECD Tax Revenue Data Pipeline")
        logger.info(f"Base directory: {self.base_dir}")
        logger.info(f"Download directory: {self.download_dir}")
        
        try:
            # Step 1: Download data
            if not self.step_1_download_data():
                logger.error("Pipeline failed at download step")
                return False
            
            # Step 2: Convert Excel to CSV
            if not self.step_2_convert_excel_to_csv():
                logger.error("Pipeline failed at conversion step")
                return False
            
            # Step 3: Map and process data
            if not self.step_3_map_and_process_data():
                logger.error("Pipeline failed at mapping step")
                return False
            
            logger.info("Pipeline completed successfully!")
            return True
            
        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted by user")
            self.execution_log['errors'].append("Pipeline interrupted by user (Ctrl+C)")
            return False
            
        except Exception as e:
            logger.error(f"Pipeline failed with unexpected error: {str(e)}")
            self.execution_log['errors'].append(f"Unexpected error: {str(e)}")
            return False
            
        finally:
            self.generate_execution_report()

def main():
    parser = argparse.ArgumentParser(
        description='OECD Tax Revenue Data Pipeline Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python orchestrator.py                    # Run complete pipeline
  python orchestrator.py --skip-download   # Skip download, use existing files
  python orchestrator.py --base-dir /path  # Use custom base directory
  python orchestrator.py --verbose         # Enable verbose logging
        """
    )
    
    parser.add_argument(
        '--base-dir', '-b',
        help='Base directory for operations (default: current directory)'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip the download step (use existing Excel files)'
    )
    parser.add_argument(
        '--skip-conversion',
        action='store_true',
        help='Skip the Excel to CSV conversion step'
    )
    parser.add_argument(
        '--skip-mapping',
        action='store_true',
        help='Skip the data mapping step'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create orchestrator
    orchestrator = OECDDataOrchestrator(
        base_dir=args.base_dir,
        skip_download=args.skip_download,
        skip_conversion=args.skip_conversion,
        skip_mapping=args.skip_mapping
    )
    
    # Run pipeline
    success = orchestrator.run_pipeline()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()