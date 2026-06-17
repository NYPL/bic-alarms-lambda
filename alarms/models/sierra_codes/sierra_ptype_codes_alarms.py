from alarms.alarm import Alarm
from helpers.alarm_helper import (
    check_redshift_mismatch_alarm,
    check_sierra_duplicate_code_alarm,
    check_sierra_null_codes_alarm,
)
from helpers.query_helper import (
    build_redshift_code_counts_query,
    build_redshift_ptype_null_query,
    build_sierra_ptypes_count_query,
)
from nypl_py_utils.functions.log_helper import create_log


class SierraPtypeCodesAlarms(Alarm):
    def __init__(self, redshift_client, sierra_client):
        super().__init__(redshift_client)
        self.sierra_client = sierra_client
        self.logger = create_log("sierra_ptype_codes_alarms")

    def run_checks(self):
        self.logger.info("Ptype Codes")
        sierra_query = build_sierra_ptypes_count_query()
        sierra_count = self.get_record_count(self.sierra_client, sierra_query)

        ptype_table = "sierra_ptype_codes" + self.redshift_suffix
        self.redshift_client.connect()
        redshift_counts = self.redshift_client.execute_query(
            build_redshift_code_counts_query("code", ptype_table)
        )[0]
        total_redshift_count = int(redshift_counts[0])
        distinct_redshift_count = int(redshift_counts[1])
        if self.run_added_tests:
            null_ptype_codes = self.redshift_client.execute_query(
                build_redshift_ptype_null_query(ptype_table, self.yesterday)
            )
            check_sierra_null_codes_alarm(
                logger=self.logger, null_codes=null_ptype_codes, code_type="ptype_codes"
            )
        self.redshift_client.close_connection()

        check_redshift_mismatch_alarm(
            logger=self.logger,
            database_type="Sierra ptype",
            redshift_table="ptype",
            database_count=sierra_count,
            redshift_count=total_redshift_count,
        )

        check_sierra_duplicate_code_alarm(
            logger=self.logger,
            code_type="ptype",
            total_count=total_redshift_count,
            distinct_count=distinct_redshift_count,
        )
