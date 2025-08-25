import logging
import pytest

from alarms.models.ezproxy_alarms import EZproxyAlarms
from datetime import date


class TestEZproxyAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        return EZproxyAlarms(mocker.MagicMock())

    def test_init(self, mocker):
        ezproxy_alarms = EZproxyAlarms(mocker.MagicMock())
        assert ezproxy_alarms.redshift_suffix == "_test_redshift_db"
        assert ezproxy_alarms.run_added_tests
        assert ezproxy_alarms.yesterday_date == date(2023, 5, 31)
        assert ezproxy_alarms.yesterday == "2023-05-31"

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_redshift_count_query = mocker.patch(
            "alarms.models.ezproxy_alarms.build_redshift_ezproxy_count_query",
            return_value="redshift count query",
        )
        mock_redshift_duplicate_query = mocker.patch(
            "alarms.models.ezproxy_alarms.build_redshift_ezproxy_duplicate_query",
            return_value="redshift duplicate query",
        )
        test_instance.redshift_client.execute_query.side_effect = [([11000],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_count_query.assert_called_once_with(
            "ezproxy_sessions_test_redshift_db", "2023-05-31"
        )
        mock_redshift_duplicate_query.assert_called_once_with(
            "ezproxy_sessions_test_redshift_db", "2023-05-31"
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("redshift count query"),
                mocker.call("redshift duplicate query"),
            ]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_no_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.ezproxy_alarms.build_redshift_ezproxy_count_query")
        mocker.patch(
            "alarms.models.ezproxy_alarms.build_redshift_ezproxy_duplicate_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [([100],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "Found only 100 ezproxy_sessions_test_redshift_db rows for all of "
            "2023-05-31"
        ) in caplog.text

    def test_run_checks_duplicate_records_alarm(self, test_instance, mocker, caplog):
        mocker.patch("alarms.models.ezproxy_alarms.build_redshift_ezproxy_count_query")
        mocker.patch(
            "alarms.models.ezproxy_alarms.build_redshift_ezproxy_duplicate_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            ([1100],),
            (
                ["sessiona", "patrona", "domaina"],
                ["sessionb", "patronb", "domainb"],
            ),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert (
            "The following (session_id, patron_id, domain) combinations correspond to "
            "more than one row on 2023-05-31: (['sessiona', 'patrona', 'domaina'], "
            "['sessionb', 'patronb', 'domainb'])"
        ) in caplog.text
