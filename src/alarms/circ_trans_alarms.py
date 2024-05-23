from helpers.query_helper import (build_redshift_circ_trans_query,
                          build_sierra_circ_trans_query)

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
            
            circ_trans_sierra_redshift_discrepancy_alarm(
                    self, sierra_count, redshift_count, redshift_table)
            
            circ_trans_sierra_no_records_alarm(
                    self, sierra_count)

# could name this "check" instead of alarm
def circ_trans_sierra_redshift_discrepancy_alarm(self, sierra_count, 
                                                 redshift_count, redshift_table):
    if sierra_count != redshift_count:
         self.logger.error((
            'Number of Sierra circ trans records does not match '
            'number of Redshift {redshift_table} records: '
            '{sierra_count} Sierra records and {redshift_count} '
            'Redshift records').format(
                redshift_table=redshift_table,
                sierra_count=sierra_count,
                redshift_count=redshift_count))

def circ_trans_sierra_no_records_alarm(self, sierra_count):
    if sierra_count == 0 and self.run_added_tests:
        self.logger.error(
            'No Sierra circ trans records found for all of {}'.format(
                self.yesterday))