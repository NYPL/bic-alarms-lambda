import logging
import pytest

from alarms.models.location_hours_alarms import LocationHoursAlarms
from datetime import date


class TestLocationHoursAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        return LocationHoursAlarms(mocker.MagicMock())

    def test_init(self, mocker):
        location_hours_alarms = LocationHoursAlarms(mocker.MagicMock())
        assert location_hours_alarms.redshift_suffix == "_test_redshift_db"
        assert location_hours_alarms.run_added_tests
        assert location_hours_alarms.yesterday_date == date(2023, 5, 31)
        assert location_hours_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_redshift_count_query = mocker.patch(
            "alarms.models.location_hours_alarms.build_redshift_hours_current_query",
            return_value="redshift current query",
        )
        mock_redshift_location_id_query = mocker.patch(
            "alarms.models.location_hours_alarms.build_redshift_hours_location_id_query",
            return_value="redshift location id query",
        )
        test_instance.redshift_client.execute_query.side_effect = [(), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_count_query.assert_called_once_with(
            "location_hours_v2_test_redshift_db"
        )
        mock_redshift_location_id_query.assert_called_once_with(
            "location_hours_v2_test_redshift_db",
            "branch_codes_map_test_redshift_db",
            "2023-05-31",
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("redshift current query"),
                mocker.call("redshift location id query"),
            ]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_no_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.location_hours_alarms.build_redshift_hours_current_query"
        )
        mocker.patch(
            "alarms.models.location_hours_alarms.build_redshift_hours_location_id_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            (["aa", "Sunday"],),
            (),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following current (location_id, weekday) combinations did not contain "
            "exactly one current row: (['aa', 'Sunday'],)"
        ) in caplog.text

    def test_run_checks_unknown_location_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.location_hours_alarms.build_redshift_hours_current_query"
        )
        mocker.patch(
            "alarms.models.location_hours_alarms.build_redshift_hours_location_id_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [(), (["fake"],)]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert "The following location_ids are unknown: (['fake'],)" in caplog.text
