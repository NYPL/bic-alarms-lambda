from alarm import Alarm
from helpers.query_helper import (build_redshift_code_counts_query,
                          build_redshift_location_null_query,
                          build_sierra_code_count_query)
from helpers.sierra_codes_helper import (sierra_redshift_count_mismatch_alarm,
                                         redshift_duplicate_code_alarm,
                                         null_code_alarm)

class SierraLocationCodesAlarms(Alarm):
    def __init__(self, logger, 
                 redshift_client, sierra_client):
        super().__init__(self, logger, redshift_client)
        self.sierra_client = sierra_client

    def run_checks(self):
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

        sierra_redshift_count_mismatch_alarm("location", sierra_count, total_redshift_count)
        redshift_duplicate_code_alarm("location", total_redshift_count, distinct_redshift_count)
        null_code_alarm("location_codes", null_location_codes)