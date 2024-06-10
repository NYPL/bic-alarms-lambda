import logging
import pytest

from alarms.models.holds_alarms import HoldsAlarms
from datetime import date


class TestHoldsAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        return HoldsAlarms(mock_redshift_client)

    def test_init(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        holds_alarms = HoldsAlarms(mock_redshift_client)
        assert holds_alarms.redshift_suffix == "_test_redshift_db"
        assert holds_alarms.run_added_tests
        assert holds_alarms.yesterday_date == date(2023, 5, 31)
        assert holds_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_count_query = mocker.patch(
            "alarms.models.holds_alarms.build_redshift_holds_query",
            return_value="count query",
        )
        mock_deleted_query = mocker.patch(
            "alarms.models.holds_alarms.build_redshift_holds_deleted_query",
            return_value="deleted query",
        )
        mock_modified_query = mocker.patch(
            "alarms.models.holds_alarms.build_redshift_holds_modified_query",
            return_value="modified query",
        )
        mock_null_query = mocker.patch(
            "alarms.models.holds_alarms.build_redshift_holds_null_query",
            return_value="null query",
        )
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],),
            ([10],),
            (),
            (),
            (),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.redshift_client.connect.assert_called_once()
        mock_count_query.assert_has_calls(
            [
                mocker.call("hold_info_test_redshift_db", "2023-06-01"),
                mocker.call("queued_holds_test_redshift_db", "2023-06-01"),
            ]
        )
        mock_deleted_query.assert_called_once_with(
            "hold_info_test_redshift_db", "2023-06-01"
        )
        mock_modified_query.assert_called_once_with("hold_info_test_redshift_db")
        mock_null_query.assert_called_once_with(
            "hold_info_test_redshift_db", "2023-06-01"
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("count query"),
                mocker.call("count query"),
                mocker.call("deleted query"),
                mocker.call("modified query"),
                mocker.call("null query"),
            ]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_no_holds_records(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_deleted_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_modified_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_null_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_query")
        test_instance.redshift_client.execute_query.side_effect = [
            ([0],),
            ([10],),
            (),
            (),
            (),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            '"hold_info_test_redshift_db" table not updated for all of '
            "2023-05-31 (ET)"
        ) in caplog.text

    def test_run_checks_no_holds_queue_records_alarm(
        self, test_instance, mocker, caplog
    ):
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_deleted_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_modified_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_null_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_query")
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],),
            ([0],),
            (),
            (),
            (),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            '"queued_holds_test_redshift_db" table not updated for all of '
            "2023-05-31 (ET)"
        ) in caplog.text

    def test_run_checks_deleted_holds_alarm(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_deleted_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_modified_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_null_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_query")
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],),
            ([10],),
            ([1,],[2,],),
            (),
            (),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following hold_ids appear despite having previously been "
            "marked as deleted: ([1], [2])"
        ) in caplog.text

    def test_run_checks_modified_holds_alarm(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_deleted_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_modified_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_null_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_query")
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],),
            ([10],),
            (),
            ([1,],[2,],),
            (),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following hold_ids have an immutable field changing: " "([1], [2])"
        ) in caplog.text

    def test_run_checks_null_holds_alarm(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_deleted_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_modified_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_null_query")
        mocker.patch("alarms.models.holds_alarms.build_redshift_holds_query")
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],),
            ([10],),
            (),
            (),
            ([1,],[2,],),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following hold_ids have an improper null value: " "([1], [2])"
        ) in caplog.text
