import os

from nypl_py_utils.functions.log_helper import create_log
from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from urllib.parse import quote

_LOGIN_URL = "https://marketplace.overdrive.com/Account/Login"
_BASE_URL = "https://marketplace.overdrive.com/Insights/Reports/{}?data="
_URL_DATA = (
    '{{"ReportChartBy":"Format","Branch":[],"IsLuckyDay":null,"TitleIds":null,'
    '"Format":null,"Language":null,"Audience":null,"Rating":null,'
    '"Subject":null,"CirculationDateParameters":{{'
    '"DateRangePeriodType":"specific","DateUnitsValue":30,'
    '"DateRangeDateUnit":"day","StartDateInputValue":"{start}",'
    '"EndDateInputValue":"{end}"}},"ContentAccessLevel":null,"UserTypes":[],'
    '"LendingModel":null,"Website":null,"Creator":null,"PurchaseOrderId":null,'
    '"DrillDownKey":"{code}","DrillDownLabel":"{name}","Parameters":{{'
    '"page":1,"start":0,"limit":{limit},"sort":[]}}}}'
)


class OverDriveWebScraper:
    """Web scraper that downloads checkouts data from OverDrive Insights"""

    def __init__(self, username, password):
        self.logger = create_log("overdrive_web_scraper")
        self.username = username
        self.password = password
        self.driver = None

        prefs = {
            "download.default_directory": os.getcwd(),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        options = ["--headless=new", "--disable-dev-shm-usage", "--no-sandbox"]
        self.chrome_options = Options()
        self.chrome_options.add_experimental_option("prefs", prefs)
        for option in options:
            self.chrome_options.add_argument(option)

    def get_count(self, date):
        self.logger.info("Opening Chrome web driver")
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.driver.maximize_window()
        self._log_in()

        self.logger.info(f"Getting OverDrive Marketplace record count for {date}")
        url = _BASE_URL.format("Checkouts") + quote(
            _URL_DATA.format(start=date, end=date, code="null", name="null", limit="50")
        )
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, timeout=30).until(
                lambda d: d.find_element(
                    By.XPATH,
                    "//a[@aria-disabled and " ".//span[text()='Create worksheet']]",
                ).get_attribute("aria-disabled")
                == "false"
            )
        except exceptions.TimeoutException:
            self.driver.quit()
            self.logger.error(f"OverDrive Marketplace URL loading timed out: {url}")
            raise OverDriveWebScraperError(
                f"OverDrive Marketplace URL loading timed out: {url}"
            ) from None

        try:
            total_rows_el = self.driver.find_element(
                By.ID, "column_TotalFormatted-textInnerEl"
            )
        except exceptions.NoSuchElementException:
            self.driver.quit()
            self.logger.error("No OverDrive Marketplace total checkouts element found")
            raise OverDriveWebScraperError(
                "No OverDrive Marketplace total checkouts element found"
            ) from None

        # The text should be in the format "Checkouts (1,234)"
        count = total_rows_el.text.replace(",", "").replace(".", "").replace(" ", "")
        self.driver.quit()
        return int(count[count.find("(") + 1 : count.find(")")])

    def _log_in(self):
        self.logger.info("Logging into OverDrive")
        self.driver.get(_LOGIN_URL)
        try:
            self.driver.find_element(By.ID, "UserName").send_keys(self.username)
            self.driver.find_element(By.ID, "Password").send_keys(self.password)
            self.driver.find_element(By.XPATH, "//input[@type='submit']").click()
        except exceptions.NoSuchElementException as e:
            self.driver.quit()
            self.logger.error(f"Login page elements not found: {e}")
            raise OverDriveWebScraperError(
                f"Login page elements not found: {e}"
            ) from None
        if self.driver.current_url == _LOGIN_URL:
            self.driver.quit()
            self.logger.error("Login failed")
            raise OverDriveWebScraperError("Login failed") from None


class OverDriveWebScraperError(Exception):
    def __init__(self, message=None):
        self.message = message
