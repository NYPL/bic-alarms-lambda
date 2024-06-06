import logging
import pytest

from alarms.models.sierra_codes.sierra_location_codes_alarms import (
    SierraLocationCodesAlarms,
)
from datetime import date


class TestSierraLocationCodesAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        test_logger = logging.getLogger(
            "test"
        )  # Custom logger since Alarm types use AlarmController logger
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        return SierraLocationCodesAlarms(
            test_logger, mock_redshift_client, mock_sierra_client
        )

    def test_init(self, mocker):
        mock_logger = mocker.MagicMock()
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        sierra_location_codes_alarms = SierraLocationCodesAlarms(
            mock_logger, mock_redshift_client, mock_sierra_client
        )
        assert sierra_location_codes_alarms.redshift_suffix == "_test_redshift_db"
        assert sierra_location_codes_alarms.run_added_tests
        assert sierra_location_codes_alarms.yesterday_date == date(2023, 5, 31)
        assert sierra_location_codes_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarms(self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_sierra_code_count_query",
            return_value="sierra locations query",
        )
        mock_redshift_counts_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_redshift_code_counts_query",
            return_value="redshift code counts query",
        )
        mock_redshift_null_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_redshift_location_null_query",
            return_value="redshift null query",
        )

        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([10, 10],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_query.assert_called_once_with("sierra_view.location_myuser")
        test_instance.sierra_client.execute_query.assert_has_calls(
            [mocker.call("sierra locations query")]
        )
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_counts_query.assert_called_once_with(
            "location_code", "sierra_location_codes_test_redshift_db"
        )
        mock_redshift_null_query.assert_called_once_with(
            "sierra_location_codes_test_redshift_db", "2023-05-31"
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("redshift code counts query"),
                mocker.call("redshift null query"),
            ]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_unequal_counts_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_sierra_code_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_redshift_location_null_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([20, 20],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Number of Sierra location codes does not match number of "
            "Redshift location codes: 10 Sierra codes and 20 Redshift "
            "codes"
        ) in caplog.text

    def test_run_checks_duplicate_codes_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_sierra_code_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_redshift_location_null_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([10, 9],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Duplicate location codes found in Redshift: 10 total active "
            "location codes but only 9 distinct active location codes"
        ) in caplog.text

    def test_run_checks_null_fields_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_sierra_code_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_location_codes_alarms.build_redshift_location_null_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([10, 10],),
            (["aa123"], ["bb345"]),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following location_codes have a null value for one or more of "
            "their inferred columns: (['aa123'], ['bb345'])"
        ) in caplog.text
