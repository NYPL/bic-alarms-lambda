from alarms.alarm import Alarm
from helpers.query_helper import (
    build_redshift_circ_trans_query,
    build_sierra_circ_trans_query,
)
from helpers.log_helper import (
    build_redshift_mismatch_log,
    build_no_records_found_log
)
from nypl_py_utils.functions.log_helper import create_log


class CircTransAlarms(Alarm):
    def __init__(self, redshift_client, sierra_client):
        super().__init__(redshift_client)
        self.sierra_client = sierra_client
        self.logger = create_log("circ_trans_alarms")

    def run_checks(self):
        self.logger.info("\nCIRC TRANS\n")
        sierra_timezones = ("EST", "America/New_York")
        redshift_tables = (["circ_trans"], ["patron_circ_trans", "item_circ_trans"])
        redshift_date_fields = (
            ["transaction_et"],
            [
                "transaction_et",
                "CONVERT_TIMEZONE('America/New_York', transaction_timestamp)::DATE",
            ],
        )

        for i in range(len(sierra_timezones)):
            sierra_query = build_sierra_circ_trans_query(
                self.yesterday, sierra_timezones[i]
            )
            sierra_count = self.get_record_count(self.sierra_client, sierra_query)
            for j in range(len(redshift_tables[i])):
                redshift_table = redshift_tables[i][j] + self.redshift_suffix
                date_field = redshift_date_fields[i][j]
                redshift_query = build_redshift_circ_trans_query(
                    redshift_table, date_field, self.yesterday
                )
                redshift_count = self.get_record_count(
                    self.redshift_client, redshift_query
                )
                if sierra_count != redshift_count:
                    mismatch_log = build_redshift_mismatch_log(database_type='Sierra circ trans', 
                                                               redshift_table=redshift_table,
                                                               database_count=sierra_count,
                                                               redshift_count=redshift_count) 
                    self.logger.error(mismatch_log)
                    
            if sierra_count == 0 and self.run_added_tests:
                no_records_log = build_no_records_found_log(database_type="Sierra circ trans",
                                                            date=self.yesterday)
                self.logger.error(no_records_log)
