from alarms.alarm import Alarm
from helpers.query_helper import (
    build_redshift_code_counts_query,
    build_redshift_itype_null_query,
    build_sierra_itypes_count_query,
)
from helpers.sierra_codes_helper import (
    sierra_redshift_count_mismatch_alarm,
    redshift_duplicate_code_alarm,
    null_code_alarm,
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
            build_redshift_code_counts_query("code", itype_table)
        )[0]
        total_redshift_count = int(redshift_counts[0])
        distinct_redshift_count = int(redshift_counts[1])
        if self.run_added_tests:
            null_itype_codes = self.redshift_client.execute_query(
                build_redshift_itype_null_query(itype_table, self.yesterday)
            )
            null_code_alarm(
                self.run_added_tests, self.logger, "itype_codes", null_itype_codes
            )
        self.redshift_client.close_connection()

        sierra_redshift_count_mismatch_alarm(
            self.logger, "itype", sierra_count, total_redshift_count
        )
        redshift_duplicate_code_alarm(
            self.logger, "itype", total_redshift_count, distinct_redshift_count
        )
