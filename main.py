import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import os
from datetime import datetime
import logging
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OECDTaxDataScraper:
    def __init__(self, download_dir=None):
        """
        Initialize the OECD Tax Data Scraper for Excel downloads
        """
        self.download_dir = download_dir or os.path.join(os.getcwd(), "downloads")
        self.driver = None
        self.wait = None
        
        os.makedirs(self.download_dir, exist_ok=True)
        
        self.urls = {
            'oecd_countries': 'https://data-explorer.oecd.org/vis?tm=Revenue%20Statistics&pg=0&snb=235&df%5bds%5d=dsDisseminateFinalDMZ&df%5bid%5d=DSD_REV_COMP_OECD%40DF_RSOECD&df%5bag%5d=OECD.CTP.TPS&df%5bvs%5d=1.0&dq=BEL%2BCAN%2BCHL%2BCOL%2BCRI%2BCZE%2BDNK%2BEST%2BFIN%2BFRA%2BDEU%2BGRC%2BHUN%2BISL%2BIRL%2BISR%2BITA%2BJPN%2BKOR%2BLVA%2BLTU%2BLUX%2BMEX%2BNLD%2BNZL%2BNOR%2BPOL%2BPRT%2BSVK%2BSVN%2BESP%2BSWE%2BCHE%2BTUR%2BGBR%2BUSA%2BOECD_REP%2BAUT%2BAUS..S13.T_5000..PT_B1GQ.A&to%5bTIME_PERIOD%5d=false&vw=tb&pd=%2C',
            'latin_america_goods': 'https://data-explorer.oecd.org/vis?lc=en&df%5bds%5d=dsDisseminateFinalDMZ&df%5bid%5d=DSD_REV_COMP_LAC%40DF_RSLAC&df%5bag%5d=OECD.CTP.TPS&dq=CHL%2BCOL%2BCRI%2BMEX%2BOECD_REP%2BATG%2BARG%2BBHS%2BBRB%2BBLZ%2BBOL%2BBRA%2BCUB%2BDOM%2BECU%2BSLV%2BGTM%2BGUY%2BHND%2BJAM%2BNIC%2BPAN%2BPRY%2BPER%2BLCA%2BTTO%2BURY%2BVEN%2BA9..S13.T_5000..PT_B1GQ.A&pd=1990%2C2022&to%5bTIME_PERIOD%5d=false&vw=tb',
            'latin_america_exports': 'https://data-explorer.oecd.org/vis?lc=en&df%5bds%5d=dsDisseminateFinalDMZ&df%5bid%5d=DSD_REV_COMP_LAC%40DF_RSLAC&df%5bag%5d=OECD.CTP.TPS&dq=CHL%2BCOL%2BCRI%2BMEX%2BOECD_REP%2BATG%2BARG%2BBHS%2BBRB%2BBLZ%2BBOL%2BBRA%2BCUB%2BDOM%2BECU%2BSLV%2BGTM%2BGUY%2BHND%2BJAM%2BNIC%2BPAN%2BPRY%2BPER%2BLCA%2BTTO%2BURY%2BVEN%2BA9..S13.T_5124..PT_B1GQ.A&pd=1990%2C2022&to%5bTIME_PERIOD%5d=false&vw=tb'
        }
    
    def setup_driver(self):
        """Setup undetected Chrome driver with download preferences for Excel files"""
        try:
            options = uc.ChromeOptions()
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "profile.default_content_settings.popups": 0,
                "profile.default_content_setting_values.automatic_downloads": 1
            }
            options.add_experimental_option("prefs", prefs)
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            
            try:
                self.driver = uc.Chrome(options=options, version_main=None)
            except Exception as e1:
                logger.warning(f"Undetected Chromedriver failed: {str(e1)}. Falling back.")
                from selenium import webdriver
                from selenium.webdriver.chrome.options import Options
                chrome_options = Options()
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                self.driver = webdriver.Chrome(options=chrome_options)

            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, 20)
            logger.info("Chrome driver initialized successfully for Excel downloads")
            
        except Exception as e:
            logger.error(f"Failed to setup driver: {str(e)}")
            raise
    
    def wait_for_page_load(self, timeout=30):
        try:
            self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
            time.sleep(3)
            logger.info("Page loaded successfully")
        except TimeoutException:
            logger.warning("Page load timeout, continuing anyway")

    def download_excel_table(self, filename_prefix="OECD_TaxRevenue"):
        """Download Excel table instead of CSV"""
        try:
            download_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Download')]")))
            self.driver.execute_script("arguments[0].click();", download_btn)
            logger.info("Clicked download button")
            
            # Wait longer for the download menu to fully load
            time.sleep(3)

            # Updated selectors for Excel download based on your provided HTML
            excel_selectors = [
                "//li[@data-testid='excel.selection-button']",  # Most specific - use first
                "//li[contains(@data-testid, 'excel.selection')]",
                "//li[contains(text(), 'Table in Excel')]",
                "//span[contains(text(), 'Table in Excel')]//ancestor::li[@role='menuitem']",
                "//div[contains(text(), 'Table in Excel')]//ancestor::li[@role='menuitem']",
            ]
            
            excel_option = None
            for selector in excel_selectors:
                try:
                    # Increased wait time to 10 seconds
                    wait_medium = WebDriverWait(self.driver, 10)
                    excel_option = wait_medium.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    logger.info(f"Found Excel option using selector: {selector}")
                    break
                except TimeoutException:
                    logger.warning(f"Selector failed: {selector}")
                    continue
            
            if not excel_option:
                # Debug: Log what elements are actually available
                self.debug_available_download_options()
                raise Exception("Excel download option not found with any known selector.")

            # Verify the element is actually clickable
            if not excel_option.is_displayed() or not excel_option.is_enabled():
                logger.warning("Excel option found but not clickable, attempting scroll")
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", excel_option)
                time.sleep(1)

            self.driver.execute_script("arguments[0].click();", excel_option)
            logger.info("Selected Excel download option")
            
            self.wait_for_download_completion()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            logger.info(f"Excel Download completed for {filename_prefix}_{timestamp}")
            
        except Exception as e:
            logger.error(f"Failed to download Excel table: {str(e)}")
            raise

    def debug_available_download_options(self):
        """Debug method to see what download options are actually available"""
        try:
            logger.info("=== DEBUGGING AVAILABLE DOWNLOAD OPTIONS ===")
            
            # Look for any download-related elements
            download_elements = self.driver.find_elements(By.XPATH, "//li[@role='menuitem'] | //a[contains(@href, 'oecd') or contains(@download, '.') or contains(text(), 'download') or contains(text(), 'Download')] | //span[contains(text(), 'Excel') or contains(text(), 'CSV')]")
            logger.info(f"Found {len(download_elements)} potential download elements:")
            
            for i, elem in enumerate(download_elements[:15]):  # Limit to first 15
                try:
                    text = elem.text.strip()[:100]  # First 100 chars
                    href = elem.get_attribute('href')
                    download_attr = elem.get_attribute('download')
                    elem_id = elem.get_attribute('id')
                    testid = elem.get_attribute('data-testid')
                    tag_name = elem.tag_name
                    logger.info(f"  {i+1}. <{tag_name}> Text: '{text}' | ID: '{elem_id}' | TestID: '{testid}' | Download: '{download_attr}'")
                    if href:
                        logger.info(f"      Href: {href[:100]}...")
                except:
                    logger.info(f"  {i+1}. Error getting element details")
                    
            logger.info("=== END DEBUG ===")
        except Exception as e:
            logger.error(f"Debug failed: {str(e)}")
    
    def wait_for_download_completion(self, timeout=120):
        """Wait for Excel download completion with longer timeout"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not any(f.endswith('.crdownload') for f in os.listdir(self.download_dir)):
                time.sleep(3)  # Wait a bit longer for Excel files
                logger.info("Excel download completed")
                return
            time.sleep(2)
        logger.warning("Excel download timeout")

    def scrape_oecd_data(self):
        """Main method to scrape OECD tax revenue data and download as Excel files"""
        try:
            logger.info("Starting OECD data scraping process for Excel downloads")
            self.setup_driver()
            
            # Download 1: OECD countries
            logger.info(">>> Processing OECD countries data...")
            self.driver.get(self.urls['oecd_countries'])
            self.wait_for_page_load()
            logger.info("Filters already configured in URL, downloading Excel file...")
            self.download_excel_table("OECD_Countries_TaxRevenue")
            
            # Download 2: Latin America - Taxes on goods and services
            logger.info(">>> Processing Latin America - Taxes on goods and services...")
            self.driver.get(self.urls['latin_america_goods'])
            self.wait_for_page_load()
            logger.info("Filters already configured in URL, downloading Excel file...")
            self.download_excel_table("LatinAmerica_TaxesOnGoods_TaxRevenue")
            
            # Download 3: Latin America - Taxes on exports
            logger.info(">>> Processing Latin America - Taxes on exports...")
            self.driver.get(self.urls['latin_america_exports'])
            self.wait_for_page_load()
            logger.info("Filters already configured in URL, downloading Excel file...")
            self.download_excel_table("LatinAmerica_TaxesOnExports_TaxRevenue")

            logger.info("OECD Excel data scraping completed successfully")
            
        except Exception as e:
            logger.error(f"A critical error occurred during scraping: {str(e)}")
            raise

            # Summary
            logger.info("=" * 60)
            logger.info("DOWNLOAD SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Successfully downloaded: {len(downloaded_files)}/3")
            for file in downloaded_files:
                logger.info(f"  ✓ {file}")
            
            if failed_downloads:
                logger.warning(f"Failed downloads: {len(failed_downloads)}")
                for file in failed_downloads:
                    logger.warning(f"  ✗ {file}")
            
            if downloaded_files:
                logger.info("OECD Excel data scraping completed with some success")
            else:
                raise Exception("No files were successfully downloaded")
            
        except Exception as e:
            logger.error(f"A critical error occurred during scraping: {str(e)}")
            raise
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed")
    
    def list_downloaded_files(self):
        """List all downloaded files"""
        files = os.listdir(self.download_dir)
        logger.info(f"Downloaded files in {self.download_dir}:")
        excel_files = [f for f in files if f.endswith(('.xlsx', '.xls'))]
        other_files = [f for f in files if not f.endswith(('.xlsx', '.xls'))]
        
        if excel_files:
            logger.info("Excel files:")
            for file in excel_files:
                logger.info(f"  ✓ {file}")
        
        if other_files:
            logger.info("Other files:")
            for file in other_files:
                logger.info(f"  - {file}")
                
        return files
    
    def create_metadata_file(self):
        """Create a JSON metadata file with scraping information"""
        metadata = {
            "last_scrape_date": datetime.now().isoformat(),
            "scrape_timestamp": int(time.time()),
            "urls_scraped": list(self.urls.keys()),
            "download_directory": str(self.download_dir),
            "total_urls": len(self.urls),
            "scraper_version": "1.0.0",
            "data_sources": {
                "oecd_countries": {
                    "url": self.urls['oecd_countries'],
                    "description": "OECD countries tax revenue data"
                },
                "latin_america_goods": {
                    "url": self.urls['latin_america_goods'],
                    "description": "Latin America taxes on goods and services"
                },
                "latin_america_exports": {
                    "url": self.urls['latin_america_exports'],
                    "description": "Latin America taxes on exports"
                }
            }
        }
        
        metadata_file = os.path.join(self.download_dir, "scrape_metadata.json")
        try:
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            logger.info(f"Metadata file created: {metadata_file}")
        except Exception as e:
            logger.error(f"Failed to create metadata file: {str(e)}")
        
        return metadata_file

def main():
    try:
        download_path = os.path.join(os.getcwd(), "oecd_tax_data_excel")
        scraper = OECDTaxDataScraper(download_dir=download_path)
        scraper.scrape_oecd_data()
        scraper.list_downloaded_files()
        scraper.create_metadata_file()
        logger.info("Excel download process completed successfully!")
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")

if __name__ == "__main__":
    main()