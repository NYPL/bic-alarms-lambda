import json
import lambda_function
import pytest


class TestLambdaFunction:
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

        mock_alarm_controller.run_alarms.assert_called_once()  # noqa: E501

    def test_lambda_handler_error(self, mock_alarm_controller, mocker):
        mock_alarm_controller.run_alarms.side_effect = Exception(
            'test exception')

        with pytest.raises(Exception):
            lambda_function.lambda_handler(None, None) # noqa: E501
