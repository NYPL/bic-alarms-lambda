import logging
import pytest

from alarms.models.granular_location_visits_alarms import GranularLocationVisitsAlarms
from datetime import date, datetime


class TestGranularLocationVisitsAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        return GranularLocationVisitsAlarms(mocker.MagicMock())

    def test_init(self, mocker):
        location_visits_alarms = GranularLocationVisitsAlarms(mocker.MagicMock())
        assert location_visits_alarms.redshift_suffix == "_test_redshift_db"
        assert location_visits_alarms.run_added_tests
        assert location_visits_alarms.yesterday_date == date(2023, 5, 31)
        assert location_visits_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_redshift_count_query = mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_count_query",
            return_value="redshift count query",
        )
        mock_redshift_duplicate_query = mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_duplicate_query",
            return_value="redshift duplicate query",
        )
        mock_redshift_stale_query = mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_stale_query",
            return_value="redshift stale query",
        )
        test_instance.redshift_client.execute_query.side_effect = [([11000],), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_count_query.assert_called_once_with(
            "location_visits_test_redshift_db", "2023-05-31"
        )
        mock_redshift_duplicate_query.assert_called_once_with(
            "location_visits_test_redshift_db", "2023-05-31"
        )
        mock_redshift_stale_query.assert_called_once_with(
            "location_visits_test_redshift_db", "2023-05-01"
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("redshift count query"),
                mocker.call("redshift duplicate query"),
                mocker.call("redshift stale query"),
            ]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_no_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_count_query"
        )
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_duplicate_query"
        )
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_stale_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [([10],), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Found only 10 location_visits_test_redshift_db rows for all "
            "of 2023-05-31"
        ) in caplog.text

    def test_run_checks_duplicate_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_count_query"
        )
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_duplicate_query"
        )
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_stale_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            ([11000],),
            (
                ["aa", 1, datetime(2023, 5, 31, 9, 0, 0)],
                ["bb", 2, datetime(2023, 5, 31, 9, 15, 0)],
            ),
            (),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following (shoppertrak_site_id, orbit, increment_start) "
            "combinations contain more than one fresh row: (['aa', 1, "
            "FakeDatetime(2023, 5, 31, 9, 0)], ['bb', 2, "
            "FakeDatetime(2023, 5, 31, 9, 15)])"
        ) in caplog.text

    def test_run_checks_stale_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_count_query"
        )
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_duplicate_query"
        )
        mocker.patch(
            "alarms.models.granular_location_visits_alarms.build_redshift_location_visits_stale_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            ([11000],),
            (),
            (
                ["aa", 1, datetime(2023, 5, 31, 9, 0, 0)],
                ["bb", 2, datetime(2023, 5, 31, 9, 15, 0)],
            ),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following (shoppertrak_site_id, orbit, increment_start) "
            "combinations are marked as stale and have not been replaced "
            "with a fresh row: (['aa', 1, FakeDatetime(2023, 5, 31, 9, "
            "0)], ['bb', 2, FakeDatetime(2023, 5, 31, 9, 15)])"
        ) in caplog.text
