import logging
import pytest

from alarms.models.cloudlibrary_alarms import CloudLibraryAlarms
from freezegun import freeze_time


class TestCloudLibraryAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        return CloudLibraryAlarms(mocker.MagicMock())

    @pytest.fixture
    def test_saturday_instance(self, mocker):
        # Instance when ran on a Saturday (this is when the CL tests will run!)
        with freeze_time("2023-06-03 01:23:45+00:00"):
            return CloudLibraryAlarms(mocker.MagicMock())

    def test_init(self, mocker):
        cloudlibrary_alarms = CloudLibraryAlarms(mocker.MagicMock())
        assert cloudlibrary_alarms.redshift_suffix == "_test_redshift_db"
        assert cloudlibrary_alarms.run_added_tests

    def test_run_checks_outside_of_saturday(self, test_instance, caplog):
        test_instance.run_checks()

        # Assert nothing was run
        assert caplog.text == ""
        assert test_instance.redshift_client.connect.call_count == 0
        assert test_instance.redshift_client.execute_query.call_count == 0
        assert test_instance.redshift_client.close_connection.call_count == 0

    def test_run_checks_no_alarm(self, test_saturday_instance, mocker, caplog):
        mock_redshift_query = mocker.patch(
            "alarms.models.cloudlibrary_alarms.build_redshift_ebook_query",
            return_value="redshift cl query",
        )
        test_saturday_instance.redshift_client.execute_query.return_value = ([10],)

        with caplog.at_level(logging.ERROR):
            test_saturday_instance.run_checks()

        assert caplog.text == ""
        # Assert each day in the past week was tested
        assert test_saturday_instance.redshift_client.connect.call_count == 7
        assert mock_redshift_query.call_count == 7
        test_saturday_instance.redshift_client.execute_query.assert_has_calls(
            [mocker.call("redshift cl query")] * 7
        )
        assert test_saturday_instance.redshift_client.close_connection.call_count == 7

    def test_run_checks_alarm(self, test_saturday_instance, mocker, caplog):
        mock_redshift_query = mocker.patch(
            "alarms.models.cloudlibrary_alarms.build_redshift_ebook_query",
            return_value="redshift cl query",
        )
        test_saturday_instance.redshift_client.execute_query.side_effect = [
            ([7],),
            ([6],),
            ([5],),
            ([0],),  # May 31 will have no CL records
            ([4],),
            ([3],),
            ([2],),
            ([1],),
        ]

        with caplog.at_level(logging.ERROR):
            test_saturday_instance.run_checks()

        assert "No cloudLibrary records found for all of 2023-05-31" in caplog.text
        assert len(caplog.records) == 1  # Assert only one error was triggered

        # Assert each day in the past week was tested
        assert test_saturday_instance.redshift_client.connect.call_count == 7
        assert mock_redshift_query.call_count == 7
        test_saturday_instance.redshift_client.execute_query.assert_has_calls(
            [mocker.call("redshift cl query")] * 7
        )
        assert test_saturday_instance.redshift_client.close_connection.call_count == 7
