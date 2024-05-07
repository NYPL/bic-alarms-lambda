import logging
import os
import pytest

from alarm_controller import AlarmController
from datetime import date


@pytest.mark.freeze_time('2023-06-01 01:23:45+00:00')
class TestAlarmController:

    @classmethod
    def setup_class(cls):
        os.environ['ENVIRONMENT'] = 'test'
        os.environ['REDSHIFT_DB_HOST'] = 'test_redshift_host'
        os.environ['REDSHIFT_DB_NAME'] = 'test_redshift_db'
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
        del os.environ['ENVIRONMENT']
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
        assert alarm_controller.redshift_suffix == '_test_redshift_db'
        assert alarm_controller.run_added_tests is True
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

    def test_run_circ_trans_alarms_no_alarm(
            self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            'alarm_controller.build_sierra_circ_trans_query',
            side_effect=['sierra circ trans EST query',
                         'sierra circ trans New York query'])
        mock_redshift_query = mocker.patch(
            'alarm_controller.build_redshift_circ_trans_query',
            side_effect=['redshift circ trans query',
                         'redshift patron circ trans query',
                         'redshift item circ trans query'])
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([10],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_circ_trans_alarms()
        assert caplog.text == ''

        assert test_instance.sierra_client.connect.call_count == 2
        mock_sierra_query.assert_has_calls([
            mocker.call('2023-05-31', 'EST'),
            mocker.call('2023-05-31', 'America/New_York')])
        test_instance.sierra_client.execute_query.assert_has_calls([
            mocker.call('sierra circ trans EST query'),
            mocker.call('sierra circ trans New York query')])
        assert test_instance.sierra_client.close_connection.call_count == 2

        assert test_instance.redshift_client.connect.call_count == 3
        mock_redshift_query.assert_has_calls([
            mocker.call('circ_trans_test_redshift_db', 'transaction_et', '2023-05-31'),  # noqa: E501
            mocker.call('patron_circ_trans_test_redshift_db', 'transaction_et',
                        '2023-05-31'),
            mocker.call('item_circ_trans_test_redshift_db',
                        'CONVERT_TIMEZONE(\'America/New_York\', transaction_timestamp)::DATE',  # noqa: E501
                        '2023-05-31')])
        test_instance.redshift_client.execute_query.assert_has_calls([
            mocker.call('redshift circ trans query'),
            mocker.call('redshift patron circ trans query'),
            mocker.call('redshift item circ trans query')])
        assert test_instance.redshift_client.close_connection.call_count == 3

    def test_run_circ_trans_alarms_unequal_counts(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_circ_trans_query')
        mocker.patch('alarm_controller.build_redshift_circ_trans_query')
        test_instance.sierra_client.execute_query.side_effect = [
            [(10,)], [(20,)]]
        test_instance.redshift_client.execute_query.side_effect = [
            ([20],), ([15],), ([10],)]

        with caplog.at_level(logging.ERROR):
            test_instance.run_circ_trans_alarms()
        assert ('Number of Sierra circ trans records does not match number of '
                'Redshift circ_trans_test_redshift_db records: 10 Sierra '
                'records and 20 Redshift records') in caplog.text
        assert ('Number of Sierra circ trans records does not match number of '
                'Redshift patron_circ_trans_test_redshift_db records: 20 '
                'Sierra records and 15 Redshift records') in caplog.text
        assert ('Number of Sierra circ trans records does not match number of '
                'Redshift item_circ_trans_test_redshift_db records: 20 '
                'Sierra records and 10 Redshift records') in caplog.text

    def test_run_circ_trans_alarms_no_records(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_circ_trans_query')
        mocker.patch('alarm_controller.build_redshift_circ_trans_query')
        test_instance.sierra_client.execute_query.return_value = [(0,)]
        test_instance.redshift_client.execute_query.return_value = ([0],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_circ_trans_alarms()
        assert 'No Sierra circ trans records found for all of 2023-05-31' \
            in caplog.text

    def test_run_holds_alarms_no_alarm(
            self, test_instance, mocker, caplog):
        mock_count_query = mocker.patch(
            'alarm_controller.build_redshift_holds_query',
            return_value='count query')
        mock_deleted_query = mocker.patch(
            'alarm_controller.build_redshift_holds_deleted_query',
            return_value='deleted query')
        mock_modified_query = mocker.patch(
            'alarm_controller.build_redshift_holds_modified_query',
            return_value='modified query')
        mock_null_query = mocker.patch(
            'alarm_controller.build_redshift_holds_null_query',
            return_value='null query')
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],), ([10],), (), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_holds_alarms()
        assert caplog.text == ''

        test_instance.redshift_client.connect.assert_called_once()
        mock_count_query.assert_has_calls([
            mocker.call('hold_info_test_redshift_db', '2023-06-01'),
            mocker.call('queued_holds_test_redshift_db', '2023-06-01')])
        mock_deleted_query.assert_called_once_with(
            'hold_info_test_redshift_db', '2023-06-01')
        mock_modified_query.assert_called_once_with(
            'hold_info_test_redshift_db')
        mock_null_query.assert_called_once_with(
            'hold_info_test_redshift_db', '2023-06-01')
        test_instance.redshift_client.execute_query.assert_has_calls([
            mocker.call('count query'), mocker.call('count query'),
            mocker.call('deleted query'), mocker.call('modified query'),
            mocker.call('null query')])
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_holds_alarms_no_holds_records(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_redshift_holds_deleted_query')
        mocker.patch('alarm_controller.build_redshift_holds_modified_query')
        mocker.patch('alarm_controller.build_redshift_holds_null_query')
        mocker.patch('alarm_controller.build_redshift_holds_query')
        test_instance.redshift_client.execute_query.side_effect = [
            ([0],), ([10],), (), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_holds_alarms()
        assert ('"hold_info_test_redshift_db" table not updated for all of '
                '2023-05-31 (ET)') in caplog.text

    def test_run_holds_alarms_no_holds_queue_records(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_redshift_holds_deleted_query')
        mocker.patch('alarm_controller.build_redshift_holds_modified_query')
        mocker.patch('alarm_controller.build_redshift_holds_null_query')
        mocker.patch('alarm_controller.build_redshift_holds_query')
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],), ([0],), (), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_holds_alarms()
        assert ('"queued_holds_test_redshift_db" table not updated for all of '
                '2023-05-31 (ET)') in caplog.text

    def test_run_holds_alarms_deleted_holds(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_redshift_holds_deleted_query')
        mocker.patch('alarm_controller.build_redshift_holds_modified_query')
        mocker.patch('alarm_controller.build_redshift_holds_null_query')
        mocker.patch('alarm_controller.build_redshift_holds_query')
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],), ([10],), ([1,], [2,]), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_holds_alarms()
        assert ('The following hold_ids appear despite having previously been '
                'marked as deleted: ([1], [2])') in caplog.text

    def test_run_holds_alarms_modified_holds(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_redshift_holds_deleted_query')
        mocker.patch('alarm_controller.build_redshift_holds_modified_query')
        mocker.patch('alarm_controller.build_redshift_holds_null_query')
        mocker.patch('alarm_controller.build_redshift_holds_query')
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],), ([10],), (), ([1,], [2,]), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_holds_alarms()
        assert ('The following hold_ids have an immutable field changing: '
                '([1], [2])') in caplog.text

    def test_run_holds_alarms_null_holds(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_redshift_holds_deleted_query')
        mocker.patch('alarm_controller.build_redshift_holds_modified_query')
        mocker.patch('alarm_controller.build_redshift_holds_null_query')
        mocker.patch('alarm_controller.build_redshift_holds_query')
        test_instance.redshift_client.execute_query.side_effect = [
            ([10],), ([10],), (), (), ([1,], [2,])]

        with caplog.at_level(logging.ERROR):
            test_instance.run_holds_alarms()
        assert ('The following hold_ids have an improper null value: '
                '([1], [2])') in caplog.text

    def test_run_pc_reserve_alarms_no_alarm(
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
            test_instance.run_pc_reserve_alarms()
        assert caplog.text == ''

        test_instance.envisionware_client.connect.assert_called_once()
        mock_envisionware_query.assert_called_once_with('2023-05-31')
        test_instance.envisionware_client.execute_query.assert_called_once_with(  # noqa: E501
            'envisionware pc reserve query')
        test_instance.envisionware_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_query.assert_called_once_with(
            'pc_reserve_test_redshift_db', '2023-05-31')
        test_instance.redshift_client.execute_query.assert_called_once_with(
            'redshift pc reserve query')
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_pc_reserve_alarms_unequal_counts(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_envisionware_pc_reserve_query')
        mocker.patch('alarm_controller.build_redshift_pc_reserve_query')
        test_instance.envisionware_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.return_value = ([20],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_pc_reserve_alarms()
        assert ('Number of Envisionware PcReserve records does not match '
                'number of Redshift PcReserve records: 10 Envisionware '
                'records and 20 Redshift records') in caplog.text

    def test_run_pc_reserve_alarms_no_records(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_envisionware_pc_reserve_query')
        mocker.patch('alarm_controller.build_redshift_pc_reserve_query')
        test_instance.envisionware_client.execute_query.return_value = [(0,)]
        test_instance.redshift_client.execute_query.return_value = ([0],)

        with caplog.at_level(logging.ERROR):
            test_instance.run_pc_reserve_alarms()
        assert 'No PcReserve records found for all of 2023-05-31' \
            in caplog.text

    def test_run_patron_info_alarms_no_alarm(
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
            test_instance.run_patron_info_alarms()
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

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_new_query.assert_called_once_with(
            'patron_info_test_redshift_db', '2023-05-24', '2023-05-31')
        mock_redshift_deleted_query.assert_called_once_with(
            'patron_info_test_redshift_db', '2023-05-24', '2023-05-31')
        test_instance.redshift_client.execute_query.assert_has_calls([
            mocker.call('redshift new patrons query'),
            mocker.call('redshift deleted patrons query')])
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_patron_info_alarms_unequal_counts(
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
            test_instance.run_patron_info_alarms()

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

    def test_run_patron_info_alarms_no_records(
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
            test_instance.run_patron_info_alarms()

        assert 'No new patron records found for all of 2023-05-24' \
            in caplog.text
        assert 'No new patron records found for all of 2023-05-26' \
            in caplog.text

    def test_run_sierra_itype_codes_alarms_no_alarms(
            self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            'alarm_controller.build_sierra_itypes_count_query',
            return_value='sierra itypes query')
        mock_redshift_counts_query = mocker.patch(
            'alarm_controller.build_redshift_code_counts_query',
            return_value='redshift code counts query')
        mock_redshift_null_query = mocker.patch(
            'alarm_controller.build_redshift_itype_null_query',
            return_value='redshift null query')

        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([10, 10],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_itype_codes_alarms()
        assert caplog.text == ''

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_query.assert_called_once()
        test_instance.sierra_client.execute_query.assert_has_calls([
            mocker.call('sierra itypes query')])
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_counts_query.assert_called_once_with(
            'code', 'sierra_itype_codes_test_redshift_db')
        mock_redshift_null_query.assert_called_once_with(
            'sierra_itype_codes_test_redshift_db', '2023-05-31')
        test_instance.redshift_client.execute_query.assert_has_calls([
            mocker.call('redshift code counts query'),
            mocker.call('redshift null query')])
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_sierra_itype_codes_alarms_unequal_counts(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_itypes_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_itype_null_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([20, 20],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_itype_codes_alarms()
        assert ('Number of Sierra itype codes does not match number of '
                'Redshift itype codes: 10 Sierra codes and 20 Redshift codes'
                ) in caplog.text

    def test_run_sierra_itype_codes_alarms_duplicate_codes(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_itypes_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_itype_null_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([10, 9],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_itype_codes_alarms()
        assert ('Duplicate itype codes found in Redshift: 10 total active '
                'itype codes but only 9 distinct active itype codes'
                ) in caplog.text

    def test_run_sierra_itype_codes_alarms_null_fields(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_itypes_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_itype_null_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([10, 10],), ([1], [2])]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_itype_codes_alarms()
        assert ('The following itype_codes have a null value for one of their '
                'inferred columns: ([1], [2])') in caplog.text

    def test_run_sierra_location_codes_alarms_no_alarms(
            self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            'alarm_controller.build_sierra_code_count_query',
            return_value='sierra locations query')
        mock_redshift_counts_query = mocker.patch(
            'alarm_controller.build_redshift_code_counts_query',
            return_value='redshift code counts query')
        mock_redshift_null_query = mocker.patch(
            'alarm_controller.build_redshift_location_null_query',
            return_value='redshift null query')

        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([10, 10],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_location_codes_alarms()
        assert caplog.text == ''

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_query.assert_called_once_with(
            'sierra_view.location_myuser')
        test_instance.sierra_client.execute_query.assert_has_calls([
            mocker.call('sierra locations query')])
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_counts_query.assert_called_once_with(
            'location_code', 'sierra_location_codes_test_redshift_db')
        mock_redshift_null_query.assert_called_once_with(
            'sierra_location_codes_test_redshift_db', '2023-05-31')
        test_instance.redshift_client.execute_query.assert_has_calls([
            mocker.call('redshift code counts query'),
            mocker.call('redshift null query')])
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_sierra_location_codes_alarms_unequal_counts(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_code_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_location_null_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([20, 20],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_location_codes_alarms()
        assert ('Number of Sierra location codes does not match number of '
                'Redshift location codes: 10 Sierra codes and 20 Redshift '
                'codes') in caplog.text

    def test_run_sierra_location_codes_alarms_duplicate_codes(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_code_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_location_null_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([10, 9],), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_location_codes_alarms()
        assert ('Duplicate location codes found in Redshift: 10 total active '
                'location codes but only 9 distinct active location codes'
                ) in caplog.text

    def test_run_sierra_location_codes_alarms_null_fields(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_code_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_location_null_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([10, 10],), (['aa123'], ['bb345'])]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_location_codes_alarms()
        assert ("The following location_codes have a null value for both of "
                "their inferred columns: (['aa123'], ['bb345'])"
                ) in caplog.text

    def test_run_sierra_stat_group_codes_alarms_no_alarms(
            self, test_instance, mocker, caplog):
        mock_sierra_query = mocker.patch(
            'alarm_controller.build_sierra_code_count_query',
            return_value='sierra stat group query')
        mock_redshift_counts_query = mocker.patch(
            'alarm_controller.build_redshift_code_counts_query',
            return_value='redshift code counts query')
        mock_redshift_null_query = mocker.patch(
            'alarm_controller.build_redshift_stat_group_null_query',
            return_value='redshift null query')
        mock_redshift_unknown_locations_query = mocker.patch(
            'alarm_controller.build_redshift_stat_group_location_query',
            return_value='redshift unknown locations query')

        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([11, 11],), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_stat_group_codes_alarms()
        assert caplog.text == ''

        test_instance.sierra_client.connect.assert_called_once()
        mock_sierra_query.assert_called_once_with(
            'sierra_view.statistic_group_myuser')
        test_instance.sierra_client.execute_query.assert_has_calls([
            mocker.call('sierra stat group query')])
        test_instance.sierra_client.close_connection.assert_called_once()

        test_instance.redshift_client.connect.assert_called_once()
        mock_redshift_counts_query.assert_called_once_with(
            'stat_group_code', 'sierra_stat_group_codes_test_redshift_db')
        mock_redshift_null_query.assert_called_once_with(
            'sierra_stat_group_codes_test_redshift_db', '2023-05-31')
        mock_redshift_unknown_locations_query.assert_called_once_with(
            'sierra_stat_group_codes_test_redshift_db',
            'sierra_location_codes_test_redshift_db', '2023-05-31')
        test_instance.redshift_client.execute_query.assert_has_calls([
            mocker.call('redshift code counts query'),
            mocker.call('redshift null query'),
            mocker.call('redshift unknown locations query')])
        test_instance.redshift_client.close_connection.assert_called_once()

    def test_run_sierra_stat_group_codes_alarms_unequal_counts(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_code_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_stat_group_null_query')
        mocker.patch(
            'alarm_controller.build_redshift_stat_group_location_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([21, 21],), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_stat_group_codes_alarms()
        assert ('Number of Sierra stat group codes does not match number of '
                'Redshift stat group codes: 10 Sierra codes and 20 Redshift '
                'codes') in caplog.text

    def test_run_sierra_stat_group_codes_alarms_duplicate_codes(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_code_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_stat_group_null_query')
        mocker.patch(
            'alarm_controller.build_redshift_stat_group_location_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([11, 10],), (), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_stat_group_codes_alarms()
        assert ('Duplicate stat group codes found in Redshift: 10 total '
                'active stat group codes but only 9 distinct active stat '
                'group codes') in caplog.text

    def test_run_sierra_stat_group_codes_alarms_null_fields(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_code_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_stat_group_null_query')
        mocker.patch(
            'alarm_controller.build_redshift_stat_group_location_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([11, 11],), ([1], [2]), ()]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_stat_group_codes_alarms()
        assert ('The following stat_group_codes have a null '
                'normalized_branch_code: ([1], [2])') in caplog.text

    def test_run_sierra_stat_group_codes_alarms_unknown_locations(
            self, test_instance, mocker, caplog):
        mocker.patch('alarm_controller.build_sierra_code_count_query')
        mocker.patch('alarm_controller.build_redshift_code_counts_query')
        mocker.patch('alarm_controller.build_redshift_stat_group_null_query')
        mocker.patch(
            'alarm_controller.build_redshift_stat_group_location_query')
        test_instance.sierra_client.execute_query.return_value = [(10,)]
        test_instance.redshift_client.execute_query.side_effect = [
            ([11, 11],), (), ([3], [4])]

        with caplog.at_level(logging.ERROR):
            test_instance.run_sierra_stat_group_codes_alarms()
        assert ('The following stat_group_codes have a normalized_branch_code '
                'that does not appear in '
                'sierra_location_codes_test_redshift_db: ([3], [4])'
                ) in caplog.text
