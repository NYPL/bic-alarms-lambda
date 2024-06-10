import logging
import pytest

from alarms.models.sierra_codes.sierra_itype_codes_alarms import SierraItypeCodesAlarms
from datetime import date


class TestSierraItypeCodesAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        return SierraItypeCodesAlarms(mock_redshift_client, mock_sierra_client)

    def test_init(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        sierra_itype_codes_alarms = SierraItypeCodesAlarms(
            mock_redshift_client, mock_sierra_client
        )
        assert sierra_itype_codes_alarms.redshift_suffix == "_test_redshift_db"
        assert sierra_itype_codes_alarms.run_added_tests
        assert sierra_itype_codes_alarms.yesterday_date == date(2023, 5, 31)
        assert sierra_itype_codes_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarms(self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_sierra_itypes_count_query",
            return_value="sierra itypes query",
        )
        mock_redshift_counts_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_redshift_code_counts_query",
            return_value="redshift code counts query",
        )
        mock_redshift_null_query = mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_redshift_itype_null_query",
            return_value="redshift null query",
        )

        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([10, 10],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_query.assert_called_once()
        test_instance.sierra_client.execute_query.assert_has_calls(
            [mocker.call("sierra itypes query")]
        )
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_counts_query.assert_called_once_with(
            "code", "sierra_itype_codes_test_redshift_db"
        )
        mock_redshift_null_query.assert_called_once_with(
            "sierra_itype_codes_test_redshift_db", "2023-05-31"
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
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_sierra_itypes_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_redshift_itype_null_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([20, 20],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Number of Sierra itype records does not match number of "
            "Redshift itype records: 10 Sierra itype records and 20 Redshift records"
        ) in caplog.text

    def test_run_checks_duplicate_codes_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_sierra_itypes_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_redshift_itype_null_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [([10, 9],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Duplicate itype codes found in Redshift: 10 total active "
            "itype codes but only 9 distinct active itype codes"
        ) in caplog.text

    def test_run_checks_alarms_null_fields_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_sierra_itypes_count_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_redshift_code_counts_query"
        )
        mocker.patch(
            "alarms.models.sierra_codes.sierra_itype_codes_alarms.build_redshift_itype_null_query"
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([10, 10],),
            ([1], [2]),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following itype_codes have a null value for one or more of their "
            "inferred columns: ([1], [2])"
        ) in caplog.text
