import logging
import pytest

from alarms.models.cloudlibrary_alarms import CloudLibraryAlarms


class TestCloudLibraryAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        return CloudLibraryAlarms(mocker.MagicMock())

    def test_init(self, mocker):
        cloudlibrary_alarms = CloudLibraryAlarms(mocker.MagicMock())
        assert cloudlibrary_alarms.redshift_suffix == "_test_redshift_db"
        assert cloudlibrary_alarms.run_added_tests

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_redshift_query = mocker.patch(
            "alarms.models.cloudlibrary_alarms.build_redshift_ebook_query",
            return_value="redshift cl query",
        )
        test_instance.redshift_client.execute_query.return_value = ([10],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert caplog.text == ""
        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_query.assert_called_once()
        test_instance.redshift_client.execute_query.assert_has_calls(
            [mocker.call("redshift cl query")]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_alarm(self, test_instance, mocker, caplog):
        mock_redshift_query = mocker.patch(
            "alarms.models.cloudlibrary_alarms.build_redshift_ebook_query",
            return_value="redshift cl query",
        )
        test_instance.redshift_client.execute_query.return_value = ([0],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert "No cloudLibrary records found for all of 2023-05-27" in caplog.text
        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_query.assert_called_once()
        test_instance.redshift_client.execute_query.assert_has_calls(
            [mocker.call("redshift cl query")]
        )
        test_instance.redshift_client.close_connection.assert_called_once()
