import logging
import pytest

from alarms.models.overdrive_checkouts_alarms import OverDriveCheckoutsAlarms
from helpers.overdrive_web_scraper import OverDriveWebScraperError


class TestOverDriveCheckoutsAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch("alarms.models.overdrive_checkouts_alarms.OverDriveWebScraper")
        mock_redshift_client = mocker.MagicMock()
        return OverDriveCheckoutsAlarms(
            mock_redshift_client, ("mock_overdrive_username", "mock_overdrive_password")
        )

    def test_init(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        overdrive_checkouts_alarms = OverDriveCheckoutsAlarms(
            mock_redshift_client, ("user", "password")
        )
        assert overdrive_checkouts_alarms.redshift_suffix == "_test_redshift_db"
        assert overdrive_checkouts_alarms.run_added_tests
        assert overdrive_checkouts_alarms.overdrive_client.username == "user"
        assert overdrive_checkouts_alarms.overdrive_client.password == "password"

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_redshift_query = mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_overdrive_query",
            return_value="redshift overdrive query",
        )
        test_instance.redshift_client.execute_query.return_value = ([10],)
        test_instance.overdrive_client.get_count.return_value = 10

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert caplog.text == ""
        test_instance.overdrive_client.get_count.assert_called_once_with("2023-05-31")
        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_query.assert_called_once_with(
            "overdrive_checkouts_test_redshift_db", "2023-05-31"
        )
        test_instance.redshift_client.execute_query.assert_called_once_with(
            "redshift overdrive query"
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_unequal_counts_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_overdrive_query"
        )
        test_instance.redshift_client.execute_query.return_value = ([10],)
        test_instance.overdrive_client.get_count.return_value = 20

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert (
            "Number of OverDrive Marketplace records does not match number of Redshift "
            "overdrive_checkouts_test_redshift_db records: 20 OverDrive Marketplace "
            "records and 10 Redshift records"
        ) in caplog.text

    def test_run_checks_no_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_overdrive_query"
        )
        test_instance.redshift_client.execute_query.return_value = ([0],)
        test_instance.overdrive_client.get_count.return_value = 0

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert (
            "No OverDrive Marketplace records found for all of 2023-05-31"
            in caplog.text
        )

    def test_run_checks_failed_web_scrape(self, test_instance, mocker, caplog):
        mock_mismatch_alarm_helper = mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.check_redshift_mismatch_alarm"
        )
        mock_no_recs_alarm_helper = mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.check_no_records_found_alarm"
        )
        mock_redshift_query = mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_overdrive_query"
        )
        test_instance.overdrive_client.get_count.side_effect = OverDriveWebScraperError(
            "mock error"
        )

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert "Failed to scrape OverDrive Marketplace" in caplog.text
        test_instance.redshift_client.connect.assert_not_called()
        mock_redshift_query.assert_not_called()
        test_instance.redshift_client.execute_query.assert_not_called()
        test_instance.redshift_client.close_connection.assert_not_called()
        mock_mismatch_alarm_helper.assert_not_called()
        mock_no_recs_alarm_helper.assert_not_called()
