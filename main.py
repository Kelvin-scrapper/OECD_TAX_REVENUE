import os
import logging
import sys
import time
import traceback
import json
from datetime import datetime

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# ============================================================================
# DIRECTORY SETUP
# ============================================================================
os.makedirs('downloads', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/{timestamp}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# ASSERTION FUNCTIONS
# ============================================================================

def assert_with_log(condition, message):
    """Assert with logging - helps track exactly where failure occurred"""
    if not condition:
        logger.error(f"ASSERTION FAILED: {message}")
        raise AssertionError(message)
    logger.debug(f"Assertion passed: {message}")


def assert_element_exists(element, element_name, context=""):
    context_msg = f" in {context}" if context else ""
    if element is None:
        msg = f"Element '{element_name}' not found{context_msg}"
        logger.error(f"ASSERTION FAILED: {msg}")
        raise AssertionError(msg)
    logger.debug(f"Element '{element_name}' found successfully{context_msg}")
    return element


def assert_file_exists(filepath, file_description=""):
    desc = file_description or filepath
    if not os.path.exists(filepath):
        msg = f"File not found: {desc} at {filepath}"
        logger.error(f"ASSERTION FAILED: {msg}")
        raise AssertionError(msg)
    logger.info(f"File verified: {desc}")
    return filepath


def assert_driver_initialized(driver):
    assert_with_log(driver is not None, "WebDriver initialized")
    return driver


# ============================================================================
# ERROR HELPERS
# ============================================================================

def save_error_screenshot(driver, error_context=""):
    if driver:
        screenshot_path = f'logs/error_{timestamp}_{error_context}.png'
        try:
            driver.save_screenshot(screenshot_path)
            logger.error(f"Screenshot saved: {screenshot_path}")
            return screenshot_path
        except Exception as e:
            logger.error(f"Failed to save screenshot: {e}")
    return None


def save_page_source(driver, error_context=""):
    if driver:
        html_path = f'logs/error_{timestamp}_{error_context}.html'
        try:
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logger.error(f"Page source saved: {html_path}")
            return html_path
        except Exception as e:
            logger.error(f"Failed to save page source: {e}")
    return None


# ============================================================================
# SCRAPER CLASS
# ============================================================================

class OECDTaxDataScraper:
    def __init__(self, download_dir=None, start_year=1990):
        """
        Initialize the OECD Tax Data Scraper for Excel downloads.

        End year is intentionally left open (pd=start,) so the OECD Data Explorer
        returns all available data up to the latest published year.
        Hardcoding an end year beyond available data triggers an OAuth redirect.
        """
        self.download_dir = download_dir or os.path.join(os.getcwd(), "downloads")
        self.driver = None
        self.wait = None
        self.start_year = start_year

        os.makedirs(self.download_dir, exist_ok=True)
        self._build_urls()

    def _build_urls(self):
        """
        Build URLs with open-ended time period (pd=start_year,).
        Leaving the end year blank tells the OECD Data Explorer to return
        all available years — avoids the OAuth redirect that occurs when
        requesting a year beyond the published dataset range.
        """
        lac_pd   = f"{self.start_year}%2C"   # e.g. 1990, (open end)
        oecd_pd  = "1955%2C"                # OECD data starts 1955; open end
        self.urls = {
            'oecd_countries': (
                'https://data-explorer.oecd.org/vis?tm=Revenue%20Statistics&pg=0&snb=235'
                '&df%5bds%5d=dsDisseminateFinalDMZ&df%5bid%5d=DSD_REV_COMP_OECD%40DF_RSOECD'
                '&df%5bag%5d=OECD.CTP.TPS&df%5bvs%5d=1.0'
                '&dq=BEL%2BCAN%2BCHL%2BCOL%2BCRI%2BCZE%2BDNK%2BEST%2BFIN%2BFRA%2BDEU%2BGRC'
                '%2BHUN%2BISL%2BIRL%2BISR%2BITA%2BJPN%2BKOR%2BLVA%2BLTU%2BLUX%2BMEX%2BNLD'
                '%2BNZL%2BNOR%2BPOL%2BPRT%2BSVK%2BSVN%2BESP%2BSWE%2BCHE%2BTUR%2BGBR%2BUSA'
                '%2BOECD_REP%2BAUT%2BAUS..S13.T_5000..PT_B1GQ.A'
                f'&to%5bTIME_PERIOD%5d=false&vw=tb&pd={oecd_pd}'
            ),
            'latin_america_goods': (
                'https://data-explorer.oecd.org/vis?lc=en'
                '&df%5bds%5d=dsDisseminateFinalDMZ&df%5bid%5d=DSD_REV_COMP_LAC%40DF_RSLAC'
                '&df%5bag%5d=OECD.CTP.TPS'
                '&dq=CHL%2BCOL%2BCRI%2BMEX%2BOECD_REP%2BATG%2BARG%2BBHS%2BBRB%2BBLZ%2BBOL'
                '%2BBRA%2BCUB%2BDOM%2BECU%2BSLV%2BGTM%2BGUY%2BHND%2BJAM%2BNIC%2BPAN%2BPRY'
                '%2BPER%2BLCA%2BTTO%2BURY%2BVEN%2BA9..S13.T_5000..PT_B1GQ.A'
                f'&pd={lac_pd}&to%5bTIME_PERIOD%5d=false&vw=tb'
            ),
            'latin_america_exports': (
                'https://data-explorer.oecd.org/vis?lc=en'
                '&df%5bds%5d=dsDisseminateFinalDMZ&df%5bid%5d=DSD_REV_COMP_LAC%40DF_RSLAC'
                '&df%5bag%5d=OECD.CTP.TPS'
                '&dq=CHL%2BCOL%2BCRI%2BMEX%2BOECD_REP%2BATG%2BARG%2BBHS%2BBRB%2BBLZ%2BBOL'
                '%2BBRA%2BCUB%2BDOM%2BECU%2BSLV%2BGTM%2BGUY%2BHND%2BJAM%2BNIC%2BPAN%2BPRY'
                '%2BPER%2BLCA%2BTTO%2BURY%2BVEN%2BA9..S13.T_5124..PT_B1GQ.A'
                f'&pd={lac_pd}&to%5bTIME_PERIOD%5d=false&vw=tb'
            ),
        }
        logger.info(
            f"URLs built: OECD pd=1955, (open end) | LAC pd={self.start_year}, (open end)"
        )

    def setup_driver(self):
        """Setup undetected Chrome driver with download preferences"""
        try:
            options = uc.ChromeOptions()
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "profile.default_content_settings.popups": 0,
                "profile.default_content_setting_values.automatic_downloads": 1,
            }
            options.add_experimental_option("prefs", prefs)
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")

            try:
                self.driver = uc.Chrome(options=options, version_main=None)
            except Exception as e1:
                logger.warning(f"Undetected Chromedriver failed: {e1}. Falling back to standard Selenium.")
                from selenium import webdriver
                from selenium.webdriver.chrome.options import Options
                chrome_options = Options()
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument("--headless=new")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--window-size=1920,1080")
                self.driver = webdriver.Chrome(options=chrome_options)

            # Enable downloads in headless mode via CDP
            self.driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": self.download_dir,
            })
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            self.wait = WebDriverWait(self.driver, 20)
            assert_driver_initialized(self.driver)
            logger.info("Chrome driver initialized successfully")

        except Exception as e:
            logger.error(f"Failed to setup driver: {e}")
            raise

    def _quit_driver(self):
        """Safely quit the driver, suppressing handle-invalid errors on crash."""
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        finally:
            self.driver = None
            self.wait = None

    def wait_for_page_load(self, timeout=30):
        """Wait for document.readyState then for the React Download button to render."""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except TimeoutException:
            logger.warning("document.readyState timeout — continuing")

        # Wait for the Download button: confirms the React app has fully rendered
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//button[@data-testid='downloads-button' or contains(text(),'Download')]")
                )
            )
            logger.info("Page rendered — Download button visible")
        except TimeoutException:
            logger.warning("Download button not found within 20s — page may still be loading")
            time.sleep(5)

    def download_excel_table(self, filename_prefix="OECD_TaxRevenue"):
        """Click Download → Table in Excel and wait for the file to land."""
        try:
            self._clean_incomplete_downloads()
            download_btn = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Download')]"))
            )
            self.driver.execute_script("arguments[0].click();", download_btn)
            logger.info("Clicked Download button")
            time.sleep(3)

            excel_selectors = [
                "//li[@data-testid='excel.selection-button']",
                "//li[contains(@data-testid, 'excel.selection')]",
                "//li[contains(text(), 'Table in Excel')]",
                "//span[contains(text(), 'Table in Excel')]//ancestor::li[@role='menuitem']",
                "//div[contains(text(), 'Table in Excel')]//ancestor::li[@role='menuitem']",
            ]

            excel_option = None
            for selector in excel_selectors:
                try:
                    excel_option = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    logger.info(f"Found Excel option: {selector}")
                    break
                except TimeoutException:
                    logger.warning(f"Selector not found: {selector}")

            if not excel_option:
                self._debug_download_options()
                raise AssertionError("Excel download option not found with any known selector.")

            if not excel_option.is_displayed() or not excel_option.is_enabled():
                logger.warning("Excel option found but not clickable — scrolling into view")
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", excel_option
                )
                time.sleep(1)

            self.driver.execute_script("arguments[0].click();", excel_option)
            logger.info("Selected 'Table in Excel'")
            self._wait_for_download_completion()
            logger.info(f"Excel download completed for {filename_prefix}")

        except Exception as e:
            logger.error(f"Failed to download Excel table: {e}")
            raise

    def _debug_download_options(self):
        """Log available download-related elements for debugging."""
        try:
            logger.info("=== DEBUG: available download elements ===")
            elems = self.driver.find_elements(
                By.XPATH,
                "//li[@role='menuitem'] | //span[contains(text(),'Excel') or contains(text(),'CSV')]"
            )
            for i, el in enumerate(elems[:15]):
                logger.info(
                    f"  {i+1}. <{el.tag_name}> testid='{el.get_attribute('data-testid')}' "
                    f"text='{el.text.strip()[:80]}'"
                )
            logger.info("=== END DEBUG ===")
        except Exception as e:
            logger.warning(f"Debug failed: {e}")

    def _clean_incomplete_downloads(self):
        """Remove leftover .crdownload files from any previous failed attempts."""
        for f in os.listdir(self.download_dir):
            if f.endswith('.crdownload'):
                try:
                    os.remove(os.path.join(self.download_dir, f))
                    logger.info(f"Removed stale incomplete download: {f}")
                except Exception as e:
                    logger.warning(f"Could not remove {f}: {e}")

    def _wait_for_download_completion(self, timeout=120):
        """
        Two-phase wait:
          Phase 1 — wait up to 20s for a .crdownload file to APPEAR  (download started)
          Phase 2 — wait up to timeout for that .crdownload to DISAPPEAR (download finished)
        Falls back to checking for a new .xlsx if Chrome skips the crdownload stage.
        """
        start = time.time()
        xlsx_before = set(
            f for f in os.listdir(self.download_dir) if f.endswith(('.xlsx', '.xls'))
        )

        # Phase 1: wait for crdownload to appear
        crdownload_seen = False
        while time.time() - start < 20:
            if any(f.endswith('.crdownload') for f in os.listdir(self.download_dir)):
                crdownload_seen = True
                logger.info("Download started — crdownload file detected")
                break
            time.sleep(0.5)

        if not crdownload_seen:
            # Chrome may have saved the file directly without a crdownload stage
            time.sleep(5)
            xlsx_after = set(
                f for f in os.listdir(self.download_dir) if f.endswith(('.xlsx', '.xls'))
            )
            if xlsx_after - xlsx_before:
                logger.info(f"Download complete (direct save): {xlsx_after - xlsx_before}")
                return
            logger.warning("No crdownload or new xlsx appeared within 20s — download may have failed")
            return

        # Phase 2: wait for crdownload to disappear
        while time.time() - start < timeout:
            if not any(f.endswith('.crdownload') for f in os.listdir(self.download_dir)):
                xlsx_after = set(
                    f for f in os.listdir(self.download_dir) if f.endswith(('.xlsx', '.xls'))
                )
                new_files = xlsx_after - xlsx_before
                logger.info(f"Download complete — new file(s): {new_files or '(none detected)'}")
                return
            time.sleep(2)
        logger.warning("Download timed out after %ds — file may be incomplete", timeout)

    def inspect_dom_structure(self, label="page", save_html=True):
        """Save page HTML + screenshot and log time-period / button elements."""
        logger.info(f"=== DOM INSPECTION: {label} ===")

        if save_html:
            html_path = os.path.join(
                'logs', f"dom_{label}_{datetime.now().strftime('%H%M%S')}.html"
            )
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Page source saved → {html_path}")

        # --- PANEL_PERIOD: explicit time-range check ---
        logger.info("--- Time period panel ---")
        try:
            panel = self.driver.find_element(
                By.XPATH,
                "//*[@data-testid='PANEL_PERIOD' or @id='PANEL_PERIOD' "
                "or contains(@aria-label,'Time period') or contains(@aria-label,'time period')]"
            )
            aria = panel.get_attribute('aria-label') or ''
            logger.info(f"  PANEL_PERIOD found: aria-label='{aria}' text='{panel.text.strip()[:120]}'")
        except Exception:
            logger.warning("  PANEL_PERIOD not found — time filter panel not yet rendered")

        time_xpaths = [
            "//*[contains(@id,'time') or contains(@id,'period') or contains(@id,'year')]",
            "//*[contains(@class,'time') or contains(@class,'period') or contains(@class,'year')]",
            "//*[@data-testid and (contains(@data-testid,'time') or contains(@data-testid,'period'))]",
        ]
        seen = set()
        logger.info("--- Other time / period elements ---")
        for xpath in time_xpaths:
            for elem in self.driver.find_elements(By.XPATH, xpath)[:5]:
                key = (elem.tag_name, elem.get_attribute("id"), elem.get_attribute("class"))
                if key in seen:
                    continue
                seen.add(key)
                logger.info(
                    f"  <{elem.tag_name}> id='{elem.get_attribute('id')}' "
                    f"testid='{elem.get_attribute('data-testid')}' "
                    f"text='{elem.text.strip()[:60]}'"
                )

        logger.info("--- Buttons (first 20) ---")
        for btn in self.driver.find_elements(
            By.XPATH, "//button | //a[@role='button'] | //div[@role='button']"
        )[:20]:
            logger.info(
                f"  <{btn.tag_name}> testid='{btn.get_attribute('data-testid')}' "
                f"text='{btn.text.strip()[:60]}'"
            )

        shot_path = os.path.join(
            'logs', f"screenshot_{label}_{datetime.now().strftime('%H%M%S')}.png"
        )
        try:
            self.driver.save_screenshot(shot_path)
            logger.info(f"Screenshot → {shot_path}")
        except Exception as e:
            logger.warning(f"Screenshot failed: {e}")

        logger.info(f"=== END DOM INSPECTION: {label} ===")

    def _download_dataset(self, label, url, filename_prefix, max_retries=2):
        """
        Load a single OECD URL and download the Excel file.
        On ChromeDriver crash the driver is restarted and the attempt is retried.
        Returns True on success, False on permanent failure.
        """
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"[{label}] Starting fresh browser (attempt {attempt})")
                self.setup_driver()
                logger.info(f"[{label}] Loading URL (attempt {attempt})...")
                self.driver.get(url)
                self.wait_for_page_load()
                self.inspect_dom_structure(label)
                logger.info(f"[{label}] Downloading Excel file...")
                self.download_excel_table(filename_prefix)
                logger.info(f"[{label}] Download complete.")
                return True
            except Exception as e:
                logger.error(f"[{label}] Attempt {attempt} failed: {str(e)[:300]}")
                save_error_screenshot(self.driver, f"{label}_attempt{attempt}")
                save_page_source(self.driver, f"{label}_attempt{attempt}")
                self._quit_driver()
                if attempt < max_retries:
                    logger.info(f"[{label}] Retrying after browser restart...")
                    time.sleep(3)
        logger.error(f"[{label}] All {max_retries} attempts failed — skipping.")
        return False

    def scrape_oecd_data(self):
        """Download all 3 datasets. Each runs in its own fresh browser instance."""
        logger.info("Starting OECD data scraping — 3 datasets")

        datasets = [
            ('oecd_countries',  self.urls['oecd_countries'],       'OECD_Countries_TaxRevenue'),
            ('lac_goods',       self.urls['latin_america_goods'],   'LatinAmerica_TaxesOnGoods_TaxRevenue'),
            ('lac_exports',     self.urls['latin_america_exports'], 'LatinAmerica_TaxesOnExports_TaxRevenue'),
        ]

        successes, failures = [], []
        for label, url, prefix in datasets:
            # Always start each dataset with a clean browser to avoid state from prior downloads
            self._quit_driver()
            ok = self._download_dataset(label, url, prefix)
            (successes if ok else failures).append(label)

        self._quit_driver()

        logger.info("=" * 60)
        logger.info(f"DOWNLOAD SUMMARY: {len(successes)}/3 succeeded")
        for s in successes:
            logger.info(f"  ✓ {s}")
        for f in failures:
            logger.warning(f"  ✗ {f}")
        logger.info("=" * 60)

        assert_with_log(len(successes) > 0, "At least one dataset must download successfully")

    def list_downloaded_files(self):
        files = os.listdir(self.download_dir)
        excel_files = [f for f in files if f.endswith(('.xlsx', '.xls'))]
        other_files = [f for f in files if not f.endswith(('.xlsx', '.xls'))]
        logger.info(f"Downloaded files in {self.download_dir}:")
        for f in excel_files:
            logger.info(f"  ✓ {f}")
        for f in other_files:
            logger.info(f"  - {f}")
        return files

    def create_metadata_file(self):
        metadata = {
            "last_scrape_date": datetime.now().isoformat(),
            "scrape_timestamp": int(time.time()),
            "urls_scraped": list(self.urls.keys()),
            "download_directory": str(self.download_dir),
            "total_urls": len(self.urls),
            "scraper_version": "2.0.0",
        }
        metadata_file = os.path.join(self.download_dir, "scrape_metadata.json")
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        logger.info(f"Metadata saved: {metadata_file}")
        return metadata_file


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="OECD Tax Revenue Data Scraper")
    parser.add_argument("--start-year", type=int, default=1990,
                        help="Start year for LAC time period (default: 1990)")
    args = parser.parse_args()

    driver_ref = [None]   # mutable ref for error helpers

    try:
        logger.info("=" * 60)
        logger.info(f"STARTING OECD TAX REVENUE SCRAPER — {timestamp}")
        logger.info("=" * 60)

        download_path = os.path.join(os.getcwd(), "downloads")
        scraper = OECDTaxDataScraper(download_dir=download_path, start_year=args.start_year)
        logger.info(f"Time period: {scraper.start_year}– (open end, fetches all available years)")

        scraper.scrape_oecd_data()
        scraper.list_downloaded_files()
        scraper.create_metadata_file()

        logger.info("=" * 60)
        logger.info("✓ SCRAPER COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        return 0

    except AssertionError as e:
        logger.error("=" * 60)
        logger.error("✗ ASSERTION FAILED")
        logger.error(f"✗ {e}")
        logger.error("=" * 60)
        save_error_screenshot(driver_ref[0], "assertion_failure")
        return 1

    except Exception as e:
        logger.error("=" * 60)
        logger.error("✗ UNEXPECTED ERROR")
        logger.error(f"✗ {type(e).__name__}: {e}")
        logger.error("=" * 60)
        logger.error(traceback.format_exc())
        save_error_screenshot(driver_ref[0], "unexpected_error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
