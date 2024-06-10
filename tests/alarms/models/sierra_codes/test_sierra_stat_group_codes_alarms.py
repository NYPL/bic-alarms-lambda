import logging
import pytest

from alarms.models.sierra_codes.sierra_stat_group_codes_alarms import (
    SierraStatGroupCodesAlarms,
)
from datetime import date


class TestSierraItypeCodesAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        return SierraStatGroupCodesAlarms(mock_redshift_client, mock_sierra_client)

    def test_init(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        sierra_stat_group_codes_alarms = SierraStatGroupCodesAlarms(
            mock_redshift_client, mock_sierra_client
        )
        assert sierra_stat_group_codes_alarms.redshift_suffix == "_test_redshift_db"
        assert sierra_stat_group_codes_alarms.run_added_tests
        assert sierra_stat_group_codes_alarms.yesterday_date == date(2023, 5, 31)
        assert sierra_stat_group_codes_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarms(self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_sierra_code_count_query",
            return_value="sierra stat group query",
        )
        mock_redshift_counts_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_code_counts_query",
            return_value="redshift code counts query",
        )
        mock_redshift_null_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_null_query",
            return_value="redshift null query",
        )
        mock_redshift_unknown_locations_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_location_query",
            return_value="redshift unknown locations query",
        )

        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([11, 11],), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_query.assert_called_once_with("sierra_view.statistic_group_myuser")
        test_instance.sierra_client.execute_query.assert_has_calls(
            [mocker.call("sierra stat group query")]
        )
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_counts_query.assert_called_once_with(
            "stat_group_code", "sierra_stat_group_codes_test_redshift_db"
        )
        mock_redshift_null_query.assert_called_once_with(
            "sierra_stat_group_codes_test_redshift_db", "2023-05-31"
        )
        mock_redshift_unknown_locations_query.assert_called_once_with(
            "sierra_stat_group_codes_test_redshift_db",
            "sierra_location_codes_test_redshift_db",
            "2023-05-31",
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("redshift code counts query"),
                mocker.call("redshift null query"),
                mocker.call("redshift unknown locations query"),
            ]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_unequal_counts_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_sierra_code_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_null_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_location_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([21, 21],), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Number of Sierra stat group records does not match number of "
            "Redshift stat group records: 10 Sierra stat group records and " 
            "20 Redshift records"
        ) in caplog.text

    def test_run_checks_duplicate_codes_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_sierra_code_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_null_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_location_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([11, 10],), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Duplicate stat group codes found in Redshift: 10 total "
            "active stat group codes but only 9 distinct active stat "
            "group codes"
        ) in caplog.text

    def test_run_checks_null_fields_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_sierra_code_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_null_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_location_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([11, 11],),
            ([1], [2]),
            (),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following stat_group_codes have a null "
            "normalized_branch_code: ([1], [2])"
        ) in caplog.text

    def test_run_checks_unknown_locations_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_sierra_code_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_null_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_stat_group_codes_alarms.build_redshift_stat_group_location_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([11, 11],),
            (),
            ([3], [4]),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following stat_group_codes have a normalized_branch_code "
            "that does not appear in "
            "sierra_location_codes_test_redshift_db: ([3], [4])"
        ) in caplog.text
