import logging
import pytest

from alarms.models.daily_location_visits_alarms import DailyLocationVisitsAlarms
from datetime import date


class TestDailyLocationVisitsAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch(
            "alarms.models.daily_location_visits_alarms.S3Client.fetch_cache",
            return_value=["aa", "bb", "cc"],
        )
        return DailyLocationVisitsAlarms(mocker.MagicMock())

    def test_init(self, mocker):
        daily_location_visits_alarms = DailyLocationVisitsAlarms(mocker.MagicMock())
        assert daily_location_visits_alarms.redshift_suffix == "_test_redshift_db"
        assert daily_location_visits_alarms.run_added_tests
        assert daily_location_visits_alarms.yesterday_date == date(2023, 5, 31)
        assert daily_location_visits_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarm(self, mocker, caplog):
        daily_location_visits_alarms = DailyLocationVisitsAlarms(mocker.MagicMock())
        mock_s3_client = mocker.MagicMock()
        mock_s3_constructor = mocker.patch(
            "alarms.models.daily_location_visits_alarms.S3Client",
            return_value=mock_s3_client,
        )
        mock_s3_client.fetch_cache.return_value = ["aa", "bb", "cc"]

        mock_redshift_query = mocker.patch(
            "alarms.models.daily_location_visits_alarms.build_redshift_daily_location_visits_query",
            return_value="redshift query",
        )
        daily_location_visits_alarms.redshift_client.execute_query.return_value = (
            ["aa", True],
            ["bb", True],
            ["cc", False],
        )

        with caplog.at_level(logging.ERROR):
            daily_location_visits_alarms.run_checks()
        assert caplog.text == ""

        mock_s3_constructor.assert_called_once_with(
            "test_shoppertrak_s3_bucket", "test_shoppertrak_s3_resource"
        )
        mock_s3_client.fetch_cache.assert_called_once()
        mock_s3_client.close.assert_called_once()
        daily_location_visits_alarms.redshift_client.connect.assert_called_once()
        mock_redshift_query.assert_called_once_with(
            "daily_location_visits_test_redshift_db", "2023-05-02"
        )
        daily_location_visits_alarms.redshift_client.execute_query.assert_called_once_with(
            "redshift query"
        )
        daily_location_visits_alarms.redshift_client.close_connection.assert_called_once()

    def test_run_checks_redshift_duplicate_sites_alarm(
        self, test_instance, mocker, caplog
    ):
        mocker.patch(
            "alarms.models.daily_location_visits_alarms.build_redshift_daily_location_visits_query"
        )
        test_instance.redshift_client.execute_query.return_value = (
            ["aa", True],
            ["bb", True],
            ["bb", True],
            ["cc", False],
        )

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert ("The following ShopperTrak sites are duplicated: ['bb']") in caplog.text

    def test_run_checks_redshift_missing_sites_alarm(
        self, test_instance, mocker, caplog
    ):
        mocker.patch(
            "alarms.models.daily_location_visits_alarms.build_redshift_daily_location_visits_query"
        )
        test_instance.redshift_client.execute_query.return_value = (
            ["aa", True],
            ["cc", True],
        )

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert "The following ShopperTrak sites are missing: ['bb']" in caplog.text

    def test_run_checks_redshift_extra_sites_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.daily_location_visits_alarms.build_redshift_daily_location_visits_query"
        )
        test_instance.redshift_client.execute_query.return_value = (
            ["aa", True],
            ["bb", True],
            ["cc", False],
            ["ee", True],
            ["dd", False],
        )

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following unknown ShopperTrak site ids were found: ['dd', 'ee']"
        ) in caplog.text

    def test_run_checks_redshift_healthy_sites_alarm(
        self, test_instance, mocker, caplog
    ):
        mocker.patch(
            "alarms.models.daily_location_visits_alarms.build_redshift_daily_location_visits_query"
        )
        test_instance.redshift_client.execute_query.return_value = (
            ["aa", True],
            ["bb", False],
            ["cc", False],
        )

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert "Only 33.33% of ShopperTrak sites were healthy" in caplog.text
