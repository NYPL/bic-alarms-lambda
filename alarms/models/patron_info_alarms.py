from alarms.alarm import Alarm
from datetime import timedelta
from helpers.query_helper import (build_redshift_deleted_patrons_query,
                          build_redshift_new_patrons_query,
                          build_sierra_deleted_patrons_query,
                          build_sierra_new_patrons_query)

class PatronInfoAlarms(Alarm):
    def __init__(self, logger,
                 redshift_client, sierra_client):
        super().__init__(self, logger, redshift_client)
        self.sierra_client = sierra_client

    def run_checks(self):
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
            self.new_patron_sierra_redshift_discrepancy_alarm(self, sierra_count, redshift_count, date)
            self.new_patron_sierra_no_records_alarm(self, sierra_count, redshift_count, date)

        # Don't check for whether there are no deleted patron records because
        # there are often days where this legitimately occurs
        for date in (sierra_deleted_counts.keys() |
                        redshift_deleted_counts.keys()):
            sierra_count = int(sierra_deleted_counts.get(date, 0))
            redshift_count = int(redshift_deleted_counts.get(date, 0))
            self.deleted_patron_sierra_redshift_discrepancy_alarm(self, sierra_count, redshift_count, date)

    def new_patron_sierra_redshift_discrepancy_alarm(self, sierra_count, 
                                                    redshift_count, date):
        if sierra_count != redshift_count:
            self.logger.error((
                'Number of Sierra new patron records does not match '
                'number of Redshift new patron records on {date}: '
                '{sierra_count} Sierra records and {redshift_count} '
                'Redshift records').format(date=date.isoformat(),
                                            sierra_count=sierra_count,
                                            redshift_count=redshift_count))
            
    def new_patron_sierra_no_records_alarm(self, sierra_count, 
                                        redshift_count, date):
        if ((sierra_count == redshift_count) and 
            sierra_count == 0 and
            self.run_added_tests):
            self.logger.error(
                    'No new patron records found for all of {}'.format(
                        date.isoformat()))
            
    def deleted_patron_sierra_redshift_discrepancy_alarm(self, sierra_count, 
                                                    redshift_count, date):
        if sierra_count != redshift_count:
            self.logger.error((
                'Number of Sierra deleted patron records does not match '
                'number of Redshift deleted patron records on {date}: '
                '{sierra_count} Sierra records and {redshift_count} '
                'Redshift records').format(date=date.isoformat(),
                                            sierra_count=sierra_count,
                                            redshift_count=redshift_count))