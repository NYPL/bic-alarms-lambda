import logging
import pytest

from alarms.models.patron_info_alarms import PatronInfoAlarms
from datetime import date


class TestPatronInfoAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        test_logger = logging.getLogger(
            "test"
        )  # Custom logger since Alarm types use AlarmController logger
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        return PatronInfoAlarms(test_logger, mock_redshift_client, mock_sierra_client)

    def test_init(self, mocker):
        mock_logger = mocker.MagicMock()
        mock_redshift_client = mocker.MagicMock()
        mock_sierra_client = mocker.MagicMock()
        patron_info_alarms = PatronInfoAlarms(
            mock_logger, mock_redshift_client, mock_sierra_client
        )
        assert patron_info_alarms.redshift_suffix == "_test_redshift_db"
        assert patron_info_alarms.run_added_tests
        assert patron_info_alarms.yesterday_date == date(2023, 5, 31)
        assert patron_info_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_sierra_new_query = mocker.patch(
            "alarms.models.patron_info_alarms.build_sierra_new_patrons_query",
            return_value="sierra new patrons query",
        )
        mock_sierra_deleted_query = mocker.patch(
            "alarms.models.patron_info_alarms.build_sierra_deleted_patrons_query",
            return_value="sierra deleted patrons query",
        )
        mock_redshift_new_query = mocker.patch(
            "alarms.models.patron_info_alarms.build_redshift_new_patrons_query",
            return_value="redshift new patrons query",
        )
        mock_redshift_deleted_query = mocker.patch(
            "alarms.models.patron_info_alarms.build_redshift_deleted_patrons_query",
            return_value="redshift deleted patrons query",
        )

        test_instance.sierra_client.execute_query.return_value = [
            (date(2023, 5, 24), 10),
            (date(2023, 5, 25), 20),
            (date(2023, 5, 26), 30),
        ]
        test_instance.redshift_client.execute_query.return_value = (
            [date(2023, 5, 24), 10],
            [date(2023, 5, 25), 20],
            [date(2023, 5, 26), 30],
        )

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_new_query.assert_called_once_with("2023-05-24", "2023-05-31")
        mock_sierra_deleted_query.assert_called_once_with("2023-05-24", "2023-05-31")
        test_instance.sierra_client.execute_query.assert_has_calls(
            [
                mocker.call("sierra new patrons query"),
                mocker.call("sierra deleted patrons query"),
            ]
        )
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_new_query.assert_called_once_with(
            "patron_info_test_redshift_db", "2023-05-24", "2023-05-31"
        )
        mock_redshift_deleted_query.assert_called_once_with(
            "patron_info_test_redshift_db", "2023-05-24", "2023-05-31"
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("redshift new patrons query"),
                mocker.call("redshift deleted patrons query"),
            ]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_unequal_counts_alarm(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.patron_info_alarms.build_sierra_new_patrons_query")
        mocker.patch(
            "alarms.models.patron_info_alarms.build_sierra_deleted_patrons_query"
        )
        mocker.patch(
            "alarms.models.patron_info_alarms.build_redshift_new_patrons_query"
        )
        mocker.patch(
            "alarms.models.patron_info_alarms.build_redshift_deleted_patrons_query"
        )

        test_instance.sierra_client.execute_query.return_value = [
            (date(2023, 5, 24), 10),
            (date(2023, 5, 25), 90),
            (date(2023, 5, 26), 80),
        ]
        test_instance.redshift_client.execute_query.return_value = (
            [date(2023, 5, 24), 10],
            [date(2023, 5, 25), 20],
            [date(2023, 5, 26), 30],
        )

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        _ERROR_STRING = (
            "Number of Sierra {type} patron records does not match number of "
            "Redshift {type} patron records on {date}: {sierra_count} Sierra "
            "records and {redshift_count} Redshift records"
        )
        assert (
            _ERROR_STRING.format(
                type="new", sierra_count="90", date="2023-05-25", redshift_count="20"
            )
            in caplog.text
        )
        assert (
            _ERROR_STRING.format(
                type="new", sierra_count="80", date="2023-05-26", redshift_count="30"
            )
            in caplog.text
        )
        assert (
            _ERROR_STRING.format(
                type="deleted",
                sierra_count="90",
                date="2023-05-25",
                redshift_count="20",
            )
            in caplog.text
        )
        assert (
            _ERROR_STRING.format(
                type="deleted",
                sierra_count="80",
                date="2023-05-26",
                redshift_count="30",
            )
            in caplog.text
        )

    def test_run_checks_no_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.patron_info_alarms.build_sierra_new_patrons_query")
        mocker.patch(
            "alarms.models.patron_info_alarms.build_sierra_deleted_patrons_query"
        )
        mocker.patch(
            "alarms.models.patron_info_alarms.build_redshift_new_patrons_query"
        )
        mocker.patch(
            "alarms.models.patron_info_alarms.build_redshift_deleted_patrons_query"
        )

        test_instance.sierra_client.execute_query.return_value = [
            (date(2023, 5, 24), 0),
            (date(2023, 5, 25), 20),
            (date(2023, 5, 26), 0),
        ]
        test_instance.redshift_client.execute_query.return_value = (
            [date(2023, 5, 25), 20],
            [date(2023, 5, 26), 0],
        )

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert "No new patron records found for all of 2023-05-24" in caplog.text
        assert "No new patron records found for all of 2023-05-26" in caplog.text
