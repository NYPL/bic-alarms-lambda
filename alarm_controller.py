import os

from datetime import datetime, timedelta, timezone
from nypl_py_utils.classes.kms_client import KmsClient
from nypl_py_utils.classes.mysql_client import MySQLClient
from nypl_py_utils.classes.postgresql_client import PostgreSQLClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.functions.log_helper import create_log
from helpers.query_helper import (build_envisionware_pc_reserve_query,
                          build_redshift_circ_trans_query,
                          build_redshift_code_counts_query,
                          build_redshift_deleted_patrons_query,
                          build_redshift_holds_deleted_query,
                          build_redshift_holds_modified_query,
                          build_redshift_holds_null_query,
                          build_redshift_holds_query,
                          build_redshift_itype_null_query,
                          build_redshift_location_null_query,
                          build_redshift_location_visits_count_query,
                          build_redshift_location_visits_duplicate_query,
                          build_redshift_location_visits_stale_query,
                          build_redshift_new_patrons_query,
                          build_redshift_pc_reserve_query,
                          build_redshift_stat_group_location_query,
                          build_redshift_stat_group_null_query,
                          build_sierra_circ_trans_query,
                          build_sierra_code_count_query,
                          build_sierra_deleted_patrons_query,
                          build_sierra_itypes_count_query,
                          build_sierra_new_patrons_query)


class AlarmController:
    """Class for orchestrating various alarms"""

    def __init__(self):
        self.logger = create_log('alarm_controller')
        self.yesterday_date = (
            datetime.now(timezone.utc) - timedelta(days=1)).date()
        self.yesterday = self.yesterday_date.isoformat()
        self.redshift_suffix = (
            '' if os.environ['REDSHIFT_DB_NAME'] == 'production' else (
                '_' + os.environ['REDSHIFT_DB_NAME']))
        self.run_added_tests = (os.environ['ENVIRONMENT'] == 'production' or
                                os.environ['ENVIRONMENT'] == 'test')

        kms_client = KmsClient()
        self.redshift_client = RedshiftClient(
            kms_client.decrypt(os.environ['REDSHIFT_DB_HOST']),
            os.environ['REDSHIFT_DB_NAME'],
            kms_client.decrypt(os.environ['REDSHIFT_DB_USER']),
            kms_client.decrypt(os.environ['REDSHIFT_DB_PASSWORD']))
        self.sierra_client = PostgreSQLClient(
            kms_client.decrypt(os.environ['SIERRA_DB_HOST']),
            os.environ['SIERRA_DB_PORT'],
            os.environ['SIERRA_DB_NAME'],
            kms_client.decrypt(os.environ['SIERRA_DB_USER']),
            kms_client.decrypt(os.environ['SIERRA_DB_PASSWORD']))
        self.envisionware_client = MySQLClient(
            kms_client.decrypt(os.environ['ENVISIONWARE_DB_HOST']),
            os.environ['ENVISIONWARE_DB_PORT'],
            os.environ['ENVISIONWARE_DB_NAME'],
            kms_client.decrypt(os.environ['ENVISIONWARE_DB_USER']),
            kms_client.decrypt(os.environ['ENVISIONWARE_DB_PASSWORD']))
        kms_client.close()

    def _get_record_count(self, client, query):
        client.connect()
        result = client.execute_query(query)
        client.close_connection()
        return int(result[0][0])

    def run_circ_trans_alarms(self):
        self.logger.info('\nCIRC TRANS\n')
        sierra_timezones = ('EST', 'America/New_York')
        redshift_tables = (
            ['circ_trans'], ['patron_circ_trans', 'item_circ_trans'])
        redshift_date_fields = (['transaction_et'], [
            'transaction_et',
            'CONVERT_TIMEZONE(\'America/New_York\', transaction_timestamp)::DATE'])  # noqa: E501

        for i in range(len(sierra_timezones)):
            sierra_query = build_sierra_circ_trans_query(
                self.yesterday, sierra_timezones[i])
            sierra_count = self._get_record_count(
                self.sierra_client, sierra_query)
            for j in range(len(redshift_tables[i])):
                redshift_table = redshift_tables[i][j] + self.redshift_suffix
                date_field = redshift_date_fields[i][j]
                redshift_query = build_redshift_circ_trans_query(
                    redshift_table, date_field, self.yesterday)
                redshift_count = self._get_record_count(
                    self.redshift_client, redshift_query)

                if sierra_count != redshift_count:
                    self.logger.error((
                        'Number of Sierra circ trans records does not match '
                        'number of Redshift {redshift_table} records: '
                        '{sierra_count} Sierra records and {redshift_count} '
                        'Redshift records').format(
                            redshift_table=redshift_table,
                            sierra_count=sierra_count,
                            redshift_count=redshift_count))
            if sierra_count == 0 and self.run_added_tests:
                self.logger.error(
                    'No Sierra circ trans records found for all of {}'.format(
                        self.yesterday))

    def run_holds_alarms(self):
        self.logger.info('\nHOLDS\n')
        # The update_timestamp is stored in UTC and the poller is run late at
        # night, so the date for the most recent day of data is today
        date_to_test = (self.yesterday_date + timedelta(days=1)).isoformat()
        self.redshift_client.connect()
        if self.run_added_tests:
            for table_name in ['hold_info', 'queued_holds']:
                redshift_table = table_name + self.redshift_suffix
                redshift_query = build_redshift_holds_query(
                    redshift_table, date_to_test)
                redshift_count = int(
                    self.redshift_client.execute_query(redshift_query)[0][0])

                if redshift_count == 0:
                    self.logger.error((
                        '"{table}" table not updated for all of {date} '
                        '(ET)').format(
                            table=redshift_table, date=self.yesterday))

        redshift_table = 'hold_info' + self.redshift_suffix
        deleted_holds = self.redshift_client.execute_query(
            build_redshift_holds_deleted_query(redshift_table, date_to_test))
        modified_holds = self.redshift_client.execute_query(
            build_redshift_holds_modified_query(redshift_table))
        null_holds = self.redshift_client.execute_query(
            build_redshift_holds_null_query(redshift_table, date_to_test))
        self.redshift_client.close_connection()

        if len(deleted_holds) > 0:
            self.logger.error(
                'The following hold_ids appear despite having previously been '
                'marked as deleted: {}'.format(deleted_holds))
        if len(modified_holds) > 0:
            self.logger.error(
                'The following hold_ids have an immutable field changing: '
                '{}'.format(modified_holds))
        if len(null_holds) > 0:
            self.logger.error(
                'The following hold_ids have an improper null value: '
                '{}'.format(null_holds))

    def run_pc_reserve_alarms(self):
        datetime_to_test = self.yesterday_date
        if not self.run_added_tests:
            datetime_to_test = self.yesterday_date - timedelta(days=1)
        date = datetime_to_test.isoformat()

        self.logger.info('\nPC RESERVE: {}\n'.format(date))
        envisionware_query = build_envisionware_pc_reserve_query(date)
        envisionware_count = self._get_record_count(
            self.envisionware_client, envisionware_query)

        redshift_table = 'pc_reserve' + self.redshift_suffix
        redshift_query = build_redshift_pc_reserve_query(redshift_table, date)
        redshift_count = self._get_record_count(
            self.redshift_client, redshift_query)

        if envisionware_count != redshift_count:
            self.logger.error((
                'Number of Envisionware PcReserve records does not match '
                'number of Redshift PcReserve records: {envisionware_count} '
                'Envisionware records and {redshift_count} Redshift records')
                .format(envisionware_count=envisionware_count,
                        redshift_count=redshift_count))
        # Al libraries are closed on Sunday, so don't fire an alarm then
        elif envisionware_count == 0 and datetime_to_test.weekday() != 6:
            self.logger.error(
                'No PcReserve records found for all of {}'.format(date))

    def run_patron_info_alarms(self):
        # If it's not Thursday, don't run this alarm, as the PatronInfo pollers
        # run weekly on Wednesday night
        if self.yesterday_date.weekday() != 2:
            return

        start_date = (self.yesterday_date - timedelta(days=7)).isoformat()
        self.logger.info('\nPATRON INFO: {start}-{end}\n'.format(
            start=start_date, end=self.yesterday))

        self.sierra_client.connect()
        sierra_new_result = self.sierra_client.execute_query(
            build_sierra_new_patrons_query(start_date, self.yesterday))
        sierra_deleted_result = self.sierra_client.execute_query(
            build_sierra_deleted_patrons_query(start_date, self.yesterday))
        self.sierra_client.close_connection()
        sierra_new_counts = dict(sierra_new_result)
        sierra_deleted_counts = dict(sierra_deleted_result)

        redshift_table = 'patron_info' + self.redshift_suffix
        self.redshift_client.connect()
        redshift_new_result = self.redshift_client.execute_query(
            build_redshift_new_patrons_query(
                redshift_table, start_date, self.yesterday))
        redshift_deleted_result = self.redshift_client.execute_query(
            build_redshift_deleted_patrons_query(
                redshift_table, start_date, self.yesterday))
        self.redshift_client.close_connection()
        redshift_new_counts = dict(redshift_new_result)
        redshift_deleted_counts = dict(redshift_deleted_result)

        for date in (sierra_new_counts.keys() | redshift_new_counts.keys()):
            sierra_count = int(sierra_new_counts.get(date, 0))
            redshift_count = int(redshift_new_counts.get(date, 0))
            if sierra_count != redshift_count:
                self.logger.error((
                    'Number of Sierra new patron records does not match '
                    'number of Redshift new patron records on {date}: '
                    '{sierra_count} Sierra records and {redshift_count} '
                    'Redshift records').format(date=date.isoformat(),
                                               sierra_count=sierra_count,
                                               redshift_count=redshift_count))
            elif sierra_count == 0 and self.run_added_tests:
                self.logger.error(
                    'No new patron records found for all of {}'.format(
                        date.isoformat()))

        # Don't check for whether there are no deleted patron records because
        # there are often days where this legitimately occurs
        for date in (sierra_deleted_counts.keys() |
                     redshift_deleted_counts.keys()):
            sierra_count = int(sierra_deleted_counts.get(date, 0))
            redshift_count = int(redshift_deleted_counts.get(date, 0))
            if sierra_count != redshift_count:
                self.logger.error((
                    'Number of Sierra deleted patron records does not match '
                    'number of Redshift deleted patron records on {date}: '
                    '{sierra_count} Sierra records and {redshift_count} '
                    'Redshift records').format(date=date.isoformat(),
                                               sierra_count=sierra_count,
                                               redshift_count=redshift_count))

    def run_location_visits_alarms(self):
        if not self.run_added_tests:
            return

        self.logger.info('\nLOCATION VISITS\n')
        redshift_table = 'location_visits' + self.redshift_suffix
        stale_start_date = (self.yesterday_date -
                            timedelta(days=30)).isoformat()
        redshift_count_query = build_redshift_location_visits_count_query(
            redshift_table, self.yesterday)
        redshift_duplicate_query = \
            build_redshift_location_visits_duplicate_query(
                redshift_table, self.yesterday)
        redshift_stale_query = build_redshift_location_visits_stale_query(
            redshift_table, stale_start_date)

        self.redshift_client.connect()
        redshift_count = int(self.redshift_client.execute_query(
            redshift_count_query)[0][0])
        redshift_duplicates = self.redshift_client.execute_query(
            redshift_duplicate_query)
        redshift_stale_rows = self.redshift_client.execute_query(
            redshift_stale_query)
        self.redshift_client.close_connection()

        if redshift_count < 10000:
            self.logger.error((
                'Found only {redshift_count} {redshift_table} rows for all of '
                '{date}').format(redshift_count=redshift_count,
                                 redshift_table=redshift_table,
                                 date=self.yesterday))
        if len(redshift_duplicates) > 0:
            self.logger.error(
                'The following (shoppertrak_site_id, orbit, increment_start) '
                'combinations contain more than one fresh row: {}'.format(
                    redshift_duplicates))
        if len(redshift_stale_rows) > 0:
            self.logger.error(
                'The following (shoppertrak_site_id, orbit, increment_start) '
                'combinations are marked as stale and have not been replaced '
                'with a fresh row: {}'.format(redshift_stale_rows))

    def run_sierra_itype_codes_alarms(self):
        self.logger.info('\nITYPE CODES\n')
        sierra_query = build_sierra_itypes_count_query()
        sierra_count = self._get_record_count(self.sierra_client, sierra_query)

        itype_table = 'sierra_itype_codes' + self.redshift_suffix
        self.redshift_client.connect()
        redshift_counts = self.redshift_client.execute_query(
            build_redshift_code_counts_query('code', itype_table))[0]
        total_redshift_count = int(redshift_counts[0])
        distinct_redshift_count = int(redshift_counts[1])
        if self.run_added_tests:
            null_itype_codes = self.redshift_client.execute_query(
                build_redshift_itype_null_query(itype_table, self.yesterday))
        self.redshift_client.close_connection()

        if sierra_count != total_redshift_count:
            self.logger.error((
                'Number of Sierra itype codes does not match number of '
                'Redshift itype codes: {sierra_count} Sierra codes and '
                '{redshift_count} Redshift codes')
                .format(sierra_count=sierra_count,
                        redshift_count=total_redshift_count))
        if total_redshift_count != distinct_redshift_count:
            self.logger.error((
                'Duplicate itype codes found in Redshift: {total_count} total '
                'active itype codes but only {distinct_count} distinct active '
                'itype codes').format(total_count=total_redshift_count,
                                      distinct_count=distinct_redshift_count))
        if self.run_added_tests and len(null_itype_codes) > 0:
            self.logger.error(
                'The following itype_codes have a null value for one of their '
                'inferred columns: {codes}'.format(codes=null_itype_codes))

    def run_sierra_location_codes_alarms(self):
        self.logger.info('\nLOCATION CODES\n')
        sierra_query = build_sierra_code_count_query(
            'sierra_view.location_myuser')
        sierra_count = self._get_record_count(self.sierra_client, sierra_query)

        location_table = 'sierra_location_codes' + self.redshift_suffix
        self.redshift_client.connect()
        redshift_counts = self.redshift_client.execute_query(
            build_redshift_code_counts_query(
                'location_code', location_table))[0]
        total_redshift_count = int(redshift_counts[0])
        distinct_redshift_count = int(redshift_counts[1])
        if self.run_added_tests:
            null_location_codes = self.redshift_client.execute_query(
                build_redshift_location_null_query(location_table,
                                                   self.yesterday))
        self.redshift_client.close_connection()

        if sierra_count != total_redshift_count:
            self.logger.error((
                'Number of Sierra location codes does not match number of '
                'Redshift location codes: {sierra_count} Sierra codes and '
                '{redshift_count} Redshift codes')
                .format(sierra_count=sierra_count,
                        redshift_count=total_redshift_count))
        if total_redshift_count != distinct_redshift_count:
            self.logger.error((
                'Duplicate location codes found in Redshift: {total_count} '
                'total active location codes but only {distinct_count} '
                'distinct active location codes').format(
                    total_count=total_redshift_count,
                distinct_count=distinct_redshift_count))
        if self.run_added_tests and len(null_location_codes) > 0:
            self.logger.error(
                'The following location_codes have a null value for both of '
                'their inferred columns: {codes}'.format(
                    codes=null_location_codes))

    def run_sierra_stat_group_codes_alarms(self):
        self.logger.info('\nSTAT GROUP CODES\n')
        sierra_query = build_sierra_code_count_query(
            'sierra_view.statistic_group_myuser')
        sierra_count = self._get_record_count(self.sierra_client, sierra_query)

        stat_group_table = 'sierra_stat_group_codes' + self.redshift_suffix
        location_table = 'sierra_location_codes' + self.redshift_suffix
        self.redshift_client.connect()
        redshift_counts = self.redshift_client.execute_query(
            build_redshift_code_counts_query(
                'stat_group_code', stat_group_table))[0]
        # Subtract one from Redshift counts because stat group code 0 is
        # manually maintained and not present in Sierra for technical reasons
        total_redshift_count = int(redshift_counts[0]) - 1
        distinct_redshift_count = int(redshift_counts[1]) - 1
        if self.run_added_tests:
            null_stat_group_codes = self.redshift_client.execute_query(
                build_redshift_stat_group_null_query(stat_group_table,
                                                     self.yesterday))
            stat_groups_without_locations = self.redshift_client.execute_query(
                build_redshift_stat_group_location_query(
                    stat_group_table, location_table, self.yesterday))
        self.redshift_client.close_connection()

        if sierra_count != total_redshift_count:
            self.logger.error((
                'Number of Sierra stat group codes does not match number of '
                'Redshift stat group codes: {sierra_count} Sierra codes and '
                '{redshift_count} Redshift codes')
                .format(sierra_count=sierra_count,
                        redshift_count=total_redshift_count))
        if total_redshift_count != distinct_redshift_count:
            self.logger.error((
                'Duplicate stat group codes found in Redshift: {total_count} '
                'total active stat group codes but only {distinct_count} '
                'distinct active stat group codes').format(
                    total_count=total_redshift_count,
                distinct_count=distinct_redshift_count))
        if self.run_added_tests and len(null_stat_group_codes) > 0:
            self.logger.error(
                'The following stat_group_codes have a null '
                'normalized_branch_code: {codes}'.format(
                    codes=null_stat_group_codes))
        if self.run_added_tests and len(stat_groups_without_locations) > 0:
            self.logger.error(
                'The following stat_group_codes have a normalized_branch_code '
                'that does not appear in {location_table}: {codes}'.format(
                    location_table=location_table,
                    codes=stat_groups_without_locations))
