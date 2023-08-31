import os

from datetime import datetime, timedelta, timezone
from nypl_py_utils.classes.kms_client import KmsClient
from nypl_py_utils.classes.mysql_client import MySQLClient
from nypl_py_utils.classes.postgresql_client import PostgreSQLClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.functions.log_helper import create_log
from query_helper import (build_envisionware_pc_reserve_query,
                          build_redshift_circ_trans_query,
                          build_redshift_deleted_patrons_query,
                          build_redshift_new_patrons_query,
                          build_redshift_pc_reserve_query,
                          build_sierra_circ_trans_query,
                          build_sierra_deleted_patrons_query,
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

    def run_circ_trans_alarm(self):
        self.logger.info(
            'Checking that an equal number of circ trans records are in '
            'Sierra and Redshift for {}'.format(self.yesterday))
        sierra_query = build_sierra_circ_trans_query(self.yesterday)
        sierra_count = self._get_record_count(self.sierra_client, sierra_query)

        redshift_table = 'circ_trans' + self.redshift_suffix
        redshift_query = build_redshift_circ_trans_query(
            redshift_table, self.yesterday)
        redshift_count = self._get_record_count(
            self.redshift_client, redshift_query)

        if sierra_count != redshift_count:
            self.logger.error((
                'Number of Sierra circ trans records does not match number of '
                'Redshift circ trans records: {sierra_count} Sierra records '
                'and {redshift_count} Redshift records').format(
                sierra_count=sierra_count, redshift_count=redshift_count))
        elif (sierra_count == 0 and
              os.environ['REDSHIFT_DB_NAME'] == 'production'):
            self.logger.error(
                'No circ trans records found for all of {}'.format(
                    self.yesterday))

    def run_pc_reserve_alarm(self):
        date = self.yesterday
        if os.environ['REDSHIFT_DB_NAME'] != 'production':
            date = (self.yesterday_date - timedelta(days=1)).isoformat()

        self.logger.info(
            'Checking that an equal number of PcReserve records are in '
            'Envisionware and Redshift for {}'.format(date))
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
        elif envisionware_count == 0:
            self.logger.error(
                'No PcReserve records found for all of {}'.format(date))

    def run_patron_info_alarm(self):
        # If it's not Thursday, don't run this alarm, as the PatronInfo pollers
        # run weekly on Wednesday night
        if self.yesterday_date.weekday() != 2:
            return

        start_date = (self.yesterday_date - timedelta(days=7)).isoformat()
        self.logger.info((
            'Checking that an equal number of newly created and newly deleted '
            'patron records are in Sierra and Redshift from {start} until '
            '{end}').format(start=start_date, end=self.yesterday))

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
            elif (sierra_count == 0 and
                  os.environ['REDSHIFT_DB_NAME'] == 'production'):
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
