from alarm_controller import AlarmController


class TestAlarmController:
    def test_init(self, mocker):
        mocker.patch("alarm_controller.RedshiftClient")
        mocker.patch("alarm_controller.PostgreSQLClient")
        mocker.patch("alarm_controller.MySQLClient")

        mock_kms_client = mocker.MagicMock()
        mock_kms_client.decrypt.return_value = "decrypted"
        mocker.patch("alarm_controller.KmsClient", return_value=mock_kms_client)

        alarm_controller = AlarmController()

        mock_kms_client.close.assert_called_once()
        mock_kms_client.decrypt.assert_has_calls(
            [
                mocker.call("test_redshift_host"),
                mocker.call("test_redshift_user"),
                mocker.call("test_redshift_password"),
                mocker.call("test_sierra_host"),
                mocker.call("test_sierra_user"),
                mocker.call("test_sierra_password"),
                mocker.call("test_envisionware_host"),
                mocker.call("test_envisionware_user"),
                mocker.call("test_envisionware_password"),
                mocker.call("test_overdrive_username"),
                mocker.call("test_overdrive_password"),
            ]
        )
