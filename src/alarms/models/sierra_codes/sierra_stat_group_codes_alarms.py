from alarm import Alarm
from helpers.query_helper import (build_redshift_code_counts_query,
                          build_redshift_stat_group_location_query,
                          build_redshift_stat_group_null_query,
                          build_sierra_code_count_query)
from helpers.sierra_codes_helper import (sierra_redshift_count_mismatch_alarm,
                                         redshift_duplicate_code_alarm,
                                         null_code_alarm)

class SierraStatGroupCodesAlarms(Alarm):
    def __init__(self, logger, redshift_client, 
                 sierra_client):
        super().__init__(self, logger, redshift_client)
        self.sierra_client = sierra_client

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

        sierra_redshift_count_mismatch_alarm('stat group', sierra_count, 
                                             total_redshift_count)
        redshift_duplicate_code_alarm('stat group', total_redshift_count,
                                      distinct_redshift_count)
        null_code_alarm('stat_group_codes', null_stat_group_codes)
        self.missing_location_code_alarm(stat_groups_without_locations, location_table)
    
    def missing_location_code_alarm(self, stat_groups_without_locations, 
                                    location_table):
        if self.run_added_tests and len(stat_groups_without_locations) > 0:
            self.logger.error(
                'The following stat_group_codes have a normalized_branch_code '
                'that does not appear in {location_table}: {codes}'.format(
                    location_table=location_table,
                    codes=stat_groups_without_locations))
