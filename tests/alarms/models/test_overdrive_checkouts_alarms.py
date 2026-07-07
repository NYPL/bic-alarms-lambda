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
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_ebook_query",
            return_value="redshift od query",
        )
        mock_duplicate_query = mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_overdrive_platform_query",
            return_value="redshift od duplicate query",
        )
        test_instance.redshift_client.execute_query.side_effect = [([10],), ()] * 2
        test_instance.overdrive_client.get_count.return_value = 10

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert caplog.text == ""
        assert test_instance.redshift_client.connect.call_count == 3
        test_instance.overdrive_client.get_count.assert_called_once_with(
            test_instance.daily_date_to_test, test_instance.daily_date_to_test
        )
        mock_redshift_query.assert_has_calls(
            [
                mocker.call(
                    "patron_overdrive_checkouts_test_redshift_db",
                    test_instance.daily_date_to_test,
                ),
                mocker.call(
                    "title_overdrive_checkouts_test_redshift_db",
                    test_instance.daily_date_to_test,
                ),
            ]
        )
        mock_duplicate_query.assert_called_once_with(
            "patron_overdrive_checkouts_test_redshift_db",
            test_instance.daily_date_to_test,
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("redshift od query"),
                mocker.call("redshift od duplicate query"),
            ]
        )
        assert test_instance.redshift_client.close_connection.call_count == 3

    def test_run_checks_unequal_counts_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_ebook_query"
        )
        mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_overdrive_platform_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [([10],), ()] * 2
        test_instance.overdrive_client.get_count.return_value = 20

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert (
            "Number of OverDrive Marketplace records does not match number of Redshift "
            "patron_overdrive_checkouts_test_redshift_db records: 20 OverDrive Marketplace "
            "records and 10 Redshift records"
        ) in caplog.text
        assert (
            "Number of OverDrive Marketplace records does not match number of Redshift "
            "title_overdrive_checkouts_test_redshift_db records: 20 OverDrive Marketplace "
            "records and 10 Redshift records"
        ) in caplog.text

    def test_run_checks_equal_counts_after_adjusting(
        self, test_instance, mocker, caplog
    ):
        mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_ebook_query",
        )
        mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_overdrive_platform_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            ([19],),
            ([10],),
            ([9],),
        ]
        test_instance.overdrive_client.get_count.return_value = 9

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

    def test_run_checks_no_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_ebook_query"
        )
        mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_overdrive_platform_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [([0],), ()] * 2
        test_instance.overdrive_client.get_count.return_value = 0

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert (
            "No OverDrive Marketplace records found for all of 2023-05-27"
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
            "alarms.models.overdrive_checkouts_alarms.build_redshift_daily_ebook_query"
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

    def test_run_checks_weekly_execution(self, test_instance, mocker, caplog):
        # Mock yesterday_date to return 3 (Thursday) for weekday()
        mock_date = mocker.MagicMock(wraps=test_instance.yesterday_date)
        mock_date.weekday.return_value = 3
        test_instance.yesterday_date = mock_date

        mock_monthly_ebook = mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_monthly_ebook_query",
            return_value="monthly count q",
        )

        mock_monthly_overdrive = mocker.patch(
            "alarms.models.overdrive_checkouts_alarms.build_redshift_monthly_overdrive_platform_query",
            return_value="monthly disc q",
        )

        test_instance.overdrive_client.get_count.return_value = 10

        test_instance.redshift_client.execute_query.side_effect = [
            ([10],),
            ([0],),
            ([10],),
            ([10],),
            ([0],),
            ([10],),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert caplog.text == ""
        test_instance.overdrive_client.get_count.assert_has_calls(
            [
                mocker.call(
                    test_instance.daily_date_to_test, test_instance.daily_date_to_test
                ),
                mocker.call(
                    test_instance.monthly_test_start_date,
                    test_instance.monthly_test_end_date,
                ),
            ]
        )
        mock_monthly_ebook.assert_has_calls(
            [
                mocker.call(
                    "patron_overdrive_checkouts_test_redshift_db",
                    test_instance.monthly_test_start_date,
                    test_instance.monthly_test_end_date,
                ),
                mocker.call(
                    "title_overdrive_checkouts_test_redshift_db",
                    test_instance.monthly_test_start_date,
                    test_instance.monthly_test_end_date,
                ),
            ]
        )
        mock_monthly_overdrive.assert_called_once_with(
            "patron_overdrive_checkouts_test_redshift_db",
            test_instance.monthly_test_start_date,
            test_instance.monthly_test_end_date,
        )
