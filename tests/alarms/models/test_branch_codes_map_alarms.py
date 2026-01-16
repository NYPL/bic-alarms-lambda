import logging
import pytest

from alarms.models.branch_codes_map_alarms import BranchCodesMapAlarms


class TestBranchCodesMapAlarms:
    @pytest.fixture
    def test_instance(self, mocker):
        return BranchCodesMapAlarms(mocker.MagicMock())

    def test_init(self, mocker):
        location_visits_alarms = BranchCodesMapAlarms(mocker.MagicMock())
        assert location_visits_alarms.redshift_suffix == "_test_redshift_db"
        assert location_visits_alarms.run_added_tests

    def test_run_checks_no_alarm(self, test_instance, mocker, caplog):
        mock_redshift_duplicate_query = mocker.patch(
            "alarms.models.branch_codes_map_alarms.build_redshift_branch_codes_duplicate_query",
            return_value="duplicate query",
        )
        mock_redshift_hours_query = mocker.patch(
            "alarms.models.branch_codes_map_alarms.build_redshift_branch_codes_hours_query",
            return_value="hours query",
        )
        test_instance.redshift_client.execute_query.side_effect = [(), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()
        assert caplog.text == ""

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_duplicate_query.assert_called_once_with(
            "branch_codes_map_test_redshift_db"
        )
        mock_redshift_hours_query.assert_called_once_with(
            "location_hours_test_redshift_db", "branch_codes_map_test_redshift_db"
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [mocker.call("duplicate query"), mocker.call("hours query")]
        )
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_checks_duplicate_alarm(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.branch_codes_map_alarms.build_redshift_branch_codes_duplicate_query"
        )
        mocker.patch(
            "alarms.models.branch_codes_map_alarms.build_redshift_branch_codes_hours_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [(["aa"], ["bb"]), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert (
            "The following Sierra branch codes map to more than one Drupal branch "
            "code: (['aa'], ['bb'])"
        ) in caplog.text

    def test_run_mismatched_hours_alarms(self, test_instance, mocker, caplog):
        mocker.patch(
            "alarms.models.branch_codes_map_alarms.build_redshift_branch_codes_duplicate_query"
        )
        mocker.patch(
            "alarms.models.branch_codes_map_alarms.build_redshift_branch_codes_hours_query"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            (),
            (["aa", None], [None, "BB"], ["cc", None], [None, "DD"]),
        ]

        with caplog.at_level(logging.ERROR):
            test_instance.run_checks()

        assert (
            "The following Sierra branch codes have location hours but do not have a "
            "known Drupal branch mapping: ['aa', 'cc']"
        ) in caplog.text
        assert (
            "The following Drupal branch codes do not have known hours: ['BB', 'DD']"
        ) in caplog.text
