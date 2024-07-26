import pytest

from helpers.overdrive_web_scraper import (
    OverDriveWebScraper,
    OverDriveWebScraperError,
)
from selenium.common import exceptions
from selenium.webdriver.common.by import By

_TEST_URL = (
    "https://marketplace.overdrive.com/Insights/Reports/Checkouts?data=%7B%22"
    "ReportChartBy%22%3A%22Format%22%2C%22Branch%22%3A%5B%5D%2C%22IsLuckyDay%22%3Anull"
    "%2C%22TitleIds%22%3Anull%2C%22Format%22%3Anull%2C%22Language%22%3Anull%2C%22"
    "Audience%22%3Anull%2C%22Rating%22%3Anull%2C%22Subject%22%3Anull%2C%22"
    "CirculationDateParameters%22%3A%7B%22DateRangePeriodType%22%3A%22specific%22%2C%22"
    "DateUnitsValue%22%3A30%2C%22DateRangeDateUnit%22%3A%22day%22%2C%22"
    "StartDateInputValue%22%3A%222023-05-31%22%2C%22EndDateInputValue%22%3A%22"
    "2023-05-31%22%7D%2C%22ContentAccessLevel%22%3Anull%2C%22UserTypes"
    "%22%3A%5B%5D%2C%22LendingModel%22%3Anull%2C%22Website%22%3Anull%2C%22Creator%22%3A"
    "null%2C%22PurchaseOrderId%22%3Anull%2C%22DrillDownKey%22%3A%22null%22%2C%22"
    "DrillDownLabel%22%3A%22null%22%2C%22Parameters%22%3A%7B%22page%22%3A1%2C%22start"
    "%22%3A0%2C%22limit%22%3A50%2C%22sort%22%3A%5B%5D%7D%7D"
)

class TestOverDriveWebScraper:
    def test_get_count(self, mocker):
        mock_log_in = mocker.patch(
            "helpers.overdrive_web_scraper.OverDriveWebScraper._log_in"
        )
        mock_waiter = mocker.patch("helpers.overdrive_web_scraper.WebDriverWait.until")

        mock_driver = mocker.MagicMock()
        mock_driver.find_element.return_value.text = " Checkouts (1, 234.) "
        mocker.patch(
            "helpers.overdrive_web_scraper.webdriver.Chrome", return_value=mock_driver
        )
        test_instance = OverDriveWebScraper(
            "mock_overdrive_username", "mock_overdrive_password"
        )

        assert test_instance.get_count("2023-05-31") == 1234

        mock_driver.maximize_window.assert_called_once()
        mock_log_in.assert_called_once()
        mock_driver.get.assert_called_once_with(_TEST_URL)
        mock_waiter.assert_called_once()
        mock_driver.find_element.assert_called_once_with(
            By.ID, "column_TotalFormatted-textInnerEl")
        mock_driver.quit.assert_called_once()

    def test_get_count_timeout(self, mocker):
        mocker.patch(
            "helpers.overdrive_web_scraper.OverDriveWebScraper._log_in"
        )
        mocker.patch(
            "helpers.overdrive_web_scraper.WebDriverWait.until",
            side_effect=exceptions.TimeoutException,
        )

        mock_driver = mocker.MagicMock()
        mocker.patch(
            "helpers.overdrive_web_scraper.webdriver.Chrome", return_value=mock_driver
        )
        test_instance = OverDriveWebScraper(
            "mock_overdrive_username", "mock_overdrive_password"
        )

        with pytest.raises(OverDriveWebScraperError):
            test_instance.get_count("2023-05-31")

        mock_driver.quit.assert_called_once()
        mock_driver.find_element.assert_not_called()

    def test_get_count_no_element(self, mocker):
        mocker.patch(
            "helpers.overdrive_web_scraper.OverDriveWebScraper._log_in"
        )
        mocker.patch("helpers.overdrive_web_scraper.WebDriverWait.until")

        mock_driver = mocker.MagicMock()
        mock_driver.find_element.side_effect = exceptions.NoSuchElementException
        mocker.patch(
            "helpers.overdrive_web_scraper.webdriver.Chrome", return_value=mock_driver
        )
        test_instance = OverDriveWebScraper(
            "mock_overdrive_username", "mock_overdrive_password"
        )

        with pytest.raises(OverDriveWebScraperError):
            test_instance.get_count("2023-05-31")

        mock_driver.quit.assert_called_once()

    def test_log_in(self, mocker):
        test_instance = OverDriveWebScraper(
            "mock_overdrive_username", "mock_overdrive_password"
        )
        test_instance.driver = mocker.MagicMock()
        test_instance.driver.current_url = "fake url"
        mock_username_el = mocker.MagicMock()
        mock_password_el = mocker.MagicMock()
        mock_submit_el = mocker.MagicMock()
        test_instance.driver.find_element.side_effect = [
            mock_username_el, mock_password_el, mock_submit_el]

        test_instance._log_in()

        test_instance.driver.get.assert_called_once_with(
            "https://marketplace.overdrive.com/Account/Login"
        )
        test_instance.driver.find_element.assert_has_calls(
            [mocker.call(By.ID, "UserName"),
             mocker.call(By.ID, "Password"),
             mocker.call(By.XPATH, "//input[@type='submit']")])
        mock_username_el.send_keys.assert_called_once_with("mock_overdrive_username")
        mock_password_el.send_keys.assert_called_once_with("mock_overdrive_password")
        mock_submit_el.click.assert_called_once()
        test_instance.driver.quit.assert_not_called()

    def test_log_in_no_element(self, mocker):
        test_instance = OverDriveWebScraper(
            "mock_overdrive_username", "mock_overdrive_password"
        )
        test_instance.driver = mocker.MagicMock()
        test_instance.driver.current_url = "fake url"
        test_instance.driver.find_element.side_effect = [
            mocker.MagicMock(),
            exceptions.NoSuchElementException]

        with pytest.raises(OverDriveWebScraperError):
            test_instance._log_in()

        test_instance.driver.quit.assert_called_once()

    def test_log_in_bad_credentials(self, mocker):
        test_instance = OverDriveWebScraper(
            "mock_overdrive_username", "mock_overdrive_password"
        )
        test_instance.driver = mocker.MagicMock()
        test_instance.driver.current_url = (
            "https://marketplace.overdrive.com/Account/Login"
        )

        with pytest.raises(OverDriveWebScraperError):
            test_instance._log_in()

        test_instance.driver.quit.assert_called_once()
