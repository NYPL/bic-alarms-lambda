import logging
import os
import pytest

from alarm_controller import AlarmController
from datetime import date


@pytest.mark.freeze_time('2023-06-01 01:23:45+00:00')
class TestAlarmController:

    @classmethod
    def setup_class(cls):
        os.environ['REDSHIFT_DB_HOST'] = 'test_redshift_host'
        os.environ['REDSHIFT_DB_NAME'] = 'production'
        os.environ['REDSHIFT_DB_USER'] = 'test_redshift_user'
        os.environ['REDSHIFT_DB_PASSWORD'] = 'test_redshift_password'
        os.environ['SIERRA_DB_HOST'] = 'test_sierra_host'
        os.environ['SIERRA_DB_PORT'] = 'test_sierra_port'
        os.environ['SIERRA_DB_NAME'] = 'test_sierra_db'
        os.environ['SIERRA_DB_USER'] = 'test_sierra_user'
        os.environ['SIERRA_DB_PASSWORD'] = 'test_sierra_password'
        os.environ['ENVISIONWARE_DB_HOST'] = 'test_envisionware_host'
        os.environ['ENVISIONWARE_DB_PORT'] = 'test_envisionware_port'
        os.environ['ENVISIONWARE_DB_NAME'] = 'test_envisionware_db'
        os.environ['ENVISIONWARE_DB_USER'] = 'test_envisionware_user'
        os.environ['ENVISIONWARE_DB_PASSWORD'] = 'test_envisionware_password'

    @classmethod
    def teardown_class(cls):
        del os.environ['REDSHIFT_DB_HOST']
        del os.environ['REDSHIFT_DB_NAME']
        del os.environ['REDSHIFT_DB_USER']
        del os.environ['REDSHIFT_DB_PASSWORD']
        del os.environ['SIERRA_DB_HOST']
        del os.environ['SIERRA_DB_PORT']
        del os.environ['SIERRA_DB_NAME']
        del os.environ['SIERRA_DB_USER']
        del os.environ['SIERRA_DB_PASSWORD']
        del os.environ['ENVISIONWARE_DB_HOST']
        del os.environ['ENVISIONWARE_DB_PORT']
        del os.environ['ENVISIONWARE_DB_NAME']
        del os.environ['ENVISIONWARE_DB_USER']
        del os.environ['ENVISIONWARE_DB_PASSWORD']

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('alarm_controller.RedshiftClient')
        mocker.patch('alarm_controller.PostgreSQLClient')
        mocker.patch('alarm_controller.MySQLClient')

        mock_kms_client = mocker.MagicMock()
        mock_kms_client.decrypt.return_value = 'decrypted'
        mocker.patch('alarm_controller.KmsClient',
                     return_value=mock_kms_client)
        return AlarmController()

    def test_init(self, mocker):
        mocker.patch('alarm_controller.RedshiftClient')
        mocker.patch('alarm_controller.PostgreSQLClient')
        mocker.patch('alarm_controller.MySQLClient')

        mock_kms_client = mocker.MagicMock()
        mock_kms_client.decrypt.return_value = 'decrypted'
        mocker.patch('alarm_controller.KmsClient',
                     return_value=mock_kms_client)

        alarm_controller = AlarmController()

        assert alarm_controller.yesterday_date == date(2023, 5, 31)
        assert alarm_controller.yesterday == '2023-05-31'
        mock_kms_client.close.assert_called_once()
        mock_kms_client.decrypt.assert_has_calls([
            mocker.call('test_redshift_host'),
            mocker.call('test_redshift_user'),
            mocker.call('test_redshift_password'),
            mocker.call('test_sierra_host'),
            mocker.call('test_sierra_user'),
            mocker.call('test_sierra_password'),
            mocker.call('test_envisionware_host'),
            mocker.call('test_envisionware_user'),
            mocker.call('test_envisionware_password')])

    def test_run_circ_trans_alarm_no_alarm(
            self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            'alarm_controller.build_sierra_circ_trans_query',
            return_value='sierra circ trans query')
        mock_redshift_query = mocker.patch(
            'alarm_controller.build_redshift_circ_trans_query',
            return_value='redshift circ trans query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([10],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_circ_trans_alarm()
        assert caplog.text == ''

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_query.assert_called_once_with('2023-05-31')
        test_instance.sierra_client.execute_query.assert_called_once_with(
            'sierra circ trans query')
        test_instance.sierra_client.close_connection.assert_called_once()

        mock_redshift_query.assert_called_once_with('circ_trans', '2023-05-31')
        test_instance.redshift_client.execute_query.assert_called_once_with(
            'redshift circ trans query')

    def test_run_circ_trans_alarm_unequal_counts(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_circ_trans_query')
        mocker.patch('alarm_controller.build_redshift_circ_trans_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([20],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_circ_trans_alarm()
        assert ('Number of Sierra circ trans records does not match number of '
                'Redshift circ trans records: 10 Sierra records and 20 '
                'Redshift records') in caplog.text

    def test_run_circ_trans_alarm_no_records(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_circ_trans_query')
        mocker.patch('alarm_controller.build_redshift_circ_trans_query')
        test_instance.sierra_client.execute_query.return_value = [(0,)]
        test_instance.redshift_client.execute_query.return_value = ([0],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_circ_trans_alarm()
        assert 'No circ trans records found for all of 2023-05-31' \
            in caplog.text

    def test_run_pc_reserve_alarm_no_alarm(
            self, test_instance, mocker, caplog):
        mock_envisionware_query = mocker.patch(
            'alarm_controller.build_envisionware_pc_reserve_query',
            return_value='envisionware pc reserve query')
        mock_redshift_query = mocker.patch(
            'alarm_controller.build_redshift_pc_reserve_query',
            return_value='redshift pc reserve query')
        test_instance.envisionware_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([10],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_pc_reserve_alarm()
        assert caplog.text == ''

        test_instance.envisionware_client.connect.assert_called_once()
        mock_envisionware_query.assert_called_once_with('2023-05-31')
        test_instance.envisionware_client.execute_query.assert_called_once_with(  # noqa: E501
            'envisionware pc reserve query')
        test_instance.envisionware_client.close_connection.assert_called_once()

        mock_redshift_query.assert_called_once_with('pc_reserve', '2023-05-31')
        test_instance.redshift_client.execute_query.assert_called_once_with(
            'redshift pc reserve query')

    def test_run_pc_reserve_alarm_unequal_counts(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_envisionware_pc_reserve_query')
        mocker.patch('alarm_controller.build_redshift_pc_reserve_query')
        test_instance.envisionware_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([20],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_pc_reserve_alarm()
        assert ('Number of Envisionware PcReserve records does not match '
                'number of Redshift PcReserve records: 10 Envisionware '
                'records and 20 Redshift records') in caplog.text

    def test_run_pc_reserve_alarm_no_records(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_envisionware_pc_reserve_query')
        mocker.patch('alarm_controller.build_redshift_pc_reserve_query')
        test_instance.envisionware_client.execute_query.return_value = [(0,)]
        test_instance.redshift_client.execute_query.return_value = ([0],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_pc_reserve_alarm()
        assert 'No PcReserve records found for all of 2023-05-31' \
            in caplog.text

    def test_run_patron_info_alarm_no_alarm(
            self, test_instance, mocker, caplog):
        mock_sierra_new_query = mocker.patch(
            'alarm_controller.build_sierra_new_patrons_query',
            return_value='sierra new patrons query')
        mock_sierra_deleted_query = mocker.patch(
            'alarm_controller.build_sierra_deleted_patrons_query',
            return_value='sierra deleted patrons query')
        mock_redshift_new_query = mocker.patch(
            'alarm_controller.build_redshift_new_patrons_query',
            return_value='redshift new patrons query')
        mock_redshift_deleted_query = mocker.patch(
            'alarm_controller.build_redshift_deleted_patrons_query',
            return_value='redshift deleted patrons query')

        test_instance.sierra_client.execute_query.return_value = [
            (date(2023, 5, 24), 10), (date(2023, 5, 25), 20),
            (date(2023, 5, 26), 30)]
        test_instance.redshift_client.execute_query.return_value = (
            [date(2023, 5, 24), 10], [date(2023, 5, 25), 20],
            [date(2023, 5, 26), 30])

        with caplog.at_level(logging.ERROR):
            test_instance.run_patron_info_alarm()
        assert caplog.text == ''

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_new_query.assert_called_once_with(
            '2023-05-24', '2023-05-31')
        mock_sierra_deleted_query.assert_called_once_with(
            '2023-05-24', '2023-05-31')
        test_instance.sierra_client.execute_query.assert_has_calls([
            mocker.call('sierra new patrons query'),
            mocker.call('sierra deleted patrons query')])
        test_instance.sierra_client.close_connection.assert_called_once()

        mock_redshift_new_query.assert_called_once_with(
            'patron_info', '2023-05-24', '2023-05-31')
        mock_redshift_new_query.assert_called_once_with(
            'patron_info', '2023-05-24', '2023-05-31')
        test_instance.redshift_client.execute_query.assert_has_calls([
            mocker.call('redshift new patrons query'),
            mocker.call('redshift deleted patrons query')])

    def test_run_patron_info_alarm_unequal_counts(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_new_patrons_query')
        mocker.patch('alarm_controller.build_sierra_deleted_patrons_query')
        mocker.patch('alarm_controller.build_redshift_new_patrons_query')
        mocker.patch('alarm_controller.build_redshift_deleted_patrons_query')

        test_instance.sierra_client.execute_query.return_value = [
            (date(2023, 5, 24), 10), (date(2023, 5, 25), 90),
            (date(2023, 5, 26), 80)]
        test_instance.redshift_client.execute_query.return_value = (
            [date(2023, 5, 24), 10], [date(2023, 5, 25), 20],
            [date(2023, 5, 26), 30])

        with caplog.at_level(logging.ERROR):
            test_instance.run_patron_info_alarm()

        _ERROR_STRING = (
            'Number of Sierra {type} patron records does not match number of '
            'Redshift {type} patron records on {date}: {sierra_count} Sierra '
            'records and {redshift_count} Redshift records')
        assert _ERROR_STRING.format(
            type='new', sierra_count='90', date='2023-05-25',
            redshift_count='20') in caplog.text
        assert _ERROR_STRING.format(
            type='new', sierra_count='80', date='2023-05-26',
            redshift_count='30') in caplog.text
        assert _ERROR_STRING.format(
            type='deleted', sierra_count='90', date='2023-05-25',
            redshift_count='20') in caplog.text
        assert _ERROR_STRING.format(
            type='deleted', sierra_count='80', date='2023-05-26',
            redshift_count='30') in caplog.text

    def test_run_patron_info_alarm_no_records(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_new_patrons_query')
        mocker.patch('alarm_controller.build_sierra_deleted_patrons_query')
        mocker.patch('alarm_controller.build_redshift_new_patrons_query')
        mocker.patch('alarm_controller.build_redshift_deleted_patrons_query')

        test_instance.sierra_client.execute_query.return_value = [
            (date(2023, 5, 24), 0), (date(2023, 5, 25), 20),
            (date(2023, 5, 26), 0)]
        test_instance.redshift_client.execute_query.return_value = (
            [date(2023, 5, 25), 20], [date(2023, 5, 26), 0])

        with caplog.at_level(logging.ERROR):
            test_instance.run_patron_info_alarm()

        assert 'No new patron records found for all of 2023-05-24' \
            in caplog.text
        assert 'No new patron records found for all of 2023-05-26' \
            in caplog.text
