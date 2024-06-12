from alarms.alarm import Alarm
from helpers.alarm_helper import (
    redshift_mismatch_alarm,
    sierra_duplicate_code_alarm,
    sierra_null_codes_alarm
)
from helpers.query_helper import (
    build_redshift_code_counts_query,
    build_redshift_itype_null_query,
    build_sierra_itypes_count_query,
)
from nypl_py_utils.functions.log_helper import create_log


class SierraItypeCodesAlarms(Alarm):
    def __init__(self, redshift_client, sierra_client):
        super().__init__(redshift_client)
        self.sierra_client = sierra_client
        self.logger = create_log("sierra_itype_codes_alarms")

    def run_checks(self):
        self.logger.info("\nITYPE CODES\n")
        sierra_query = build_sierra_itypes_count_query()
        sierra_count = self.get_record_count(self.sierra_client, sierra_query)

        itype_table = "sierra_itype_codes" + self.redshift_suffix
        self.redshift_client.connect()
        redshift_counts = self.redshift_client.execute_query(
            build_redshift_code_counts_query("code", itype_table))[0]
        total_redshift_count = int(redshift_counts[0])
        distinct_redshift_count = int(redshift_counts[1])
        if self.run_added_tests:
            null_itype_codes = self.redshift_client.execute_query(
                build_redshift_itype_null_query(itype_table, self.yesterday)
            )
            sierra_null_codes_alarm(logger=self.logger, 
                                    null_codes=null_itype_codes,
                                    code_type="itype_codes")
        self.redshift_client.close_connection()

        redshift_mismatch_alarm(logger=self.logger, 
                                database_type="Sierra itype", 
                                redshift_table="itype",
                                database_count=sierra_count,
                                redshift_count=total_redshift_count)

        sierra_duplicate_code_alarm(logger=self.logger, 
                                    code_type="itype",
                                    total_count=total_redshift_count,
                                    distinct_count=distinct_redshift_count)
