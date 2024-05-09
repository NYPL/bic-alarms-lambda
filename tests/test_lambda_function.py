import json
import lambda_function
import os
import pytest


class TestLambdaFunction:

    @classmethod
    def setup_class(cls):
        os.environ['ENVIRONMENT'] = 'test'

    @classmethod
    def teardown_class(cls):
        del os.environ['ENVIRONMENT']

    @pytest.fixture
    def mock_alarm_controller(self, mocker):
        mocker.patch('lambda_function.create_log')
        mocker.patch('nypl_py_utils.functions.config_helper.load_env_file')

        mock_alarm_controller = mocker.MagicMock()
        mocker.patch('lambda_function.AlarmController',
                     return_value=mock_alarm_controller)
        return mock_alarm_controller

    def test_lambda_handler_success(self, mock_alarm_controller, mocker):
        assert lambda_function.lambda_handler(None, None) == {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Job ran successfully."
            })
        }

        mock_alarm_controller.run_circ_trans_alarms.assert_called_once()
        mock_alarm_controller.run_holds_alarms.assert_called_once()
        mock_alarm_controller.run_pc_reserve_alarms.assert_called_once()
        mock_alarm_controller.run_patron_info_alarms.assert_called_once()
        mock_alarm_controller.run_location_visits_alarms.assert_called_once()
        mock_alarm_controller.run_sierra_itype_codes_alarms.assert_called_once()  # noqa: E501
        mock_alarm_controller.run_sierra_location_codes_alarms.assert_called_once()  # noqa: E501
        mock_alarm_controller.run_sierra_stat_group_codes_alarms.assert_called_once()  # noqa: E501

    def test_lambda_handler_error(self, mock_alarm_controller, mocker):
        mock_alarm_controller.run_pc_reserve_alarms.side_effect = Exception(
            'test exception')

        with pytest.raises(Exception):
            lambda_function.lambda_handler(None, None)

        mock_alarm_controller.run_circ_trans_alarms.assert_called_once()
        mock_alarm_controller.run_holds_alarms.assert_called_once()
        mock_alarm_controller.run_pc_reserve_alarms.assert_called_once()
        mock_alarm_controller.run_patron_info_alarms.assert_not_called()
        mock_alarm_controller.run_run_location_visits_alarms.assert_not_called()  # noqa: E501
        mock_alarm_controller.run_sierra_itype_codes_alarms.assert_not_called()  # noqa: E501
        mock_alarm_controller.run_sierra_location_codes_alarms.assert_not_called()  # noqa: E501
        mock_alarm_controller.run_sierra_stat_group_codes_alarms.assert_not_called()  # noqa: E501
