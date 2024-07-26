from nypl_py_utils.functions.log_helper import create_log
from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from urllib.parse import quote

_LOGIN_URL = "https://marketplace.overdrive.com/Account/Login"
_CHECKOUTS_URL = "https://marketplace.overdrive.com/Insights/Reports/Checkouts?data="
_URL_VARIABLES = (
    '{{"ReportChartBy":"Format","Branch":[],"IsLuckyDay":null,"TitleIds":null,'
    '"Format":null,"Language":null,"Audience":null,"Rating":null,'
    '"Subject":null,"CirculationDateParameters":{{'
    '"DateRangePeriodType":"specific","DateUnitsValue":30,'
    '"DateRangeDateUnit":"day","StartDateInputValue":"{date}",'
    '"EndDateInputValue":"{date}"}},"ContentAccessLevel":null,"UserTypes":[],'
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
        self.chrome_options = Options()
        options = ["--headless=old", "--disable-dev-shm-usage", "--no-sandbox"]
        for option in options:
            self.chrome_options.add_argument(option)

    def get_count(self, date):
        self.logger.info("Opening Chrome web driver")
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.driver.maximize_window()
        self._log_in()

        self.logger.info(f"Getting OverDrive Marketplace record count for {date}")
        url = _CHECKOUTS_URL + quote(
            _URL_VARIABLES.format(date=date, code="null", name="null", limit="50")
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
        self.logger.info("Logging into OverDrive Marketplace")
        self.driver.get(_LOGIN_URL)
        try:
            self.driver.find_element(By.ID, "UserName").send_keys(self.username)
            self.driver.find_element(By.ID, "Password").send_keys(self.password)
            self.driver.find_element(By.XPATH, "//input[@type='submit']").click()
        except exceptions.NoSuchElementException:
            self.driver.quit()
            self.logger.error(f"OverDrive Marketplace login page elements not found")
            raise OverDriveWebScraperError(
                f"OverDrive Marketplace login page elements not found"
            ) from None
        if self.driver.current_url == _LOGIN_URL:
            self.driver.quit()
            self.logger.error("OverDrive Marketplace login failed")
            raise OverDriveWebScraperError(
                "OverDrive Marketplace login failed"
            ) from None


class OverDriveWebScraperError(Exception):
    def __init__(self, message=None):
        self.message = message
