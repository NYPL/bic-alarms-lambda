import logging
import pytest

from alarms.models.circ_trans_alarms import CircTransAlarms
from datetime import date


class TestCircTransAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        return CircTransAlarms(mock_redshift_client, mock_sierra_client)

    def test_init(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        circ_trans_alarms = CircTransAlarms(mock_redshift_client, mock_sierra_client)
        assert circ_trans_alarms.redshift_suffix == "_test_redshift_db"
        assert circ_trans_alarms.run_added_tests
        assert circ_trans_alarms.yesterday_date == date(2023, 5, 31)
        assert circ_trans_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            "alarms.models.circ_trans_alarms.build_sierra_circ_trans_query",
            return_value="sierra circ trans query",
        )
        mock_redshift_query = mocker.patch(
            "alarms.models.circ_trans_alarms.build_redshift_circ_trans_query",
            side_effect=[
                "redshift patron circ trans query",
                "redshift item circ trans query",
            ],
        )
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([10],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_query.assert_called_once_with("2023-05-31")
        test_instance.sierra_client.execute_query.assert_called_once_with(
            "sierra circ trans query"
        )
        test_instance.sierra_client.close_connection.assert_called_once()

        assert test_instance.redshift_client.connect.call_count == 2
        mock_redshift_query.assert_has_calls(
            [
                mocker.call(
                    "patron_circ_trans_test_redshift_db", "transaction_et", "2023-05-31"
                ),
                mocker.call(
                    "item_circ_trans_test_redshift_db",
                    "CONVERT_TIMEZONE('America/New_York', transaction_timestamp)::DATE",
                    "2023-05-31",
                ),
            ]
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("redshift patron circ trans query"),
                mocker.call("redshift item circ trans query"),
            ]
        )
        assert test_instance.redshift_client.close_connection.call_count == 2

    def test_run_checks_unequal_counts(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.circ_trans_alarms.build_sierra_circ_trans_query")
        mocker.patch("alarms.models.circ_trans_alarms.build_redshift_circ_trans_query")
        test_instance.sierra_client.execute_query.return_value = [(20,)]
        test_instance.redshift_client.execute_query.side_effect = [([15],), ([10],)]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert (
            "Number of Sierra circ trans records does not match number of "
            "Redshift patron_circ_trans_test_redshift_db records: 20 "
            "Sierra circ trans records and 15 Redshift records"
        ) in caplog.text
        assert (
            "Number of Sierra circ trans records does not match number of "
            "Redshift item_circ_trans_test_redshift_db records: 20 "
            "Sierra circ trans records and 10 Redshift records"
        ) in caplog.text

    def test_run_checks_no_records(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.circ_trans_alarms.build_sierra_circ_trans_query")
        mocker.patch("alarms.models.circ_trans_alarms.build_redshift_circ_trans_query")
        test_instance.sierra_client.execute_query.return_value = [(0,)]
        test_instance.redshift_client.execute_query.return_value = ([0],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert "No Sierra circ trans records found for all of 2023-05-31" in caplog.text
