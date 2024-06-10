import logging
import pytest

from alarms.models import pc_reserve_alarms
from alarms.models.pc_reserve_alarms import PcReserveAlarms
from datetime import date


class TestPcReserveAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_envisionware_client = mocker.MagicMock()
        return PcReserveAlarms(mock_redshift_client, mock_envisionware_client)

    def test_init(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_envisionware_client = mocker.MagicMock()
        pc_reserve_alarms = PcReserveAlarms(
            mock_redshift_client, mock_envisionware_client
        )
        assert pc_reserve_alarms.redshift_suffix == "_test_redshift_db"
        assert pc_reserve_alarms.run_added_tests
        assert pc_reserve_alarms.yesterday_date == date(2023, 5, 31)
        assert pc_reserve_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_envisionware_query = mocker.patch(
            "alarms.models.pc_reserve_alarms.build_envisionware_pc_reserve_query",
            return_value="envisionware pc reserve query",
        )
        mock_redshift_query = mocker.patch(
            "alarms.models.pc_reserve_alarms.build_redshift_pc_reserve_query",
            return_value="redshift pc reserve query",
        )
        test_instance.envisionware_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([10],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.envisionware_client.connect.assert_called_once()
        mock_envisionware_query.assert_called_once_with("2023-05-31")
        test_instance.envisionware_client.execute_query.assert_called_once_with(  # noqa: E501
            "envisionware pc reserve query"
        )
        test_instance.envisionware_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_query.assert_called_once_with(
            "pc_reserve_test_redshift_db", "2023-05-31"
        )
        test_instance.redshift_client.execute_query.assert_called_once_with(
            "redshift pc reserve query"
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_unequal_counts_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.pc_reserve_alarms.build_envisionware_pc_reserve_query"
        )
        mocker.patch("alarms.models.pc_reserve_alarms.build_redshift_pc_reserve_query")
        test_instance.envisionware_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([20],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Number of Envisionware PcReserve records does not match "
            "number of Redshift PcReserve records: 10 Envisionware "
            "records and 20 Redshift records"
        ) in caplog.text

    def test_run_checks_no_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.pc_reserve_alarms.build_envisionware_pc_reserve_query"
        )
        mocker.patch("alarms.models.pc_reserve_alarms.build_redshift_pc_reserve_query")
        test_instance.envisionware_client.execute_query.return_value = [(0,)]
        test_instance.redshift_client.execute_query.return_value = ([0],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert "No PcReserve records found for all of 2023-05-31" in caplog.text
