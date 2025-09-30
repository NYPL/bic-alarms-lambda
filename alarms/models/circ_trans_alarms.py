from alarms.alarm import Alarm
from helpers.alarm_helper import (
    check_redshift_mismatch_alarm,
    check_no_records_found_alarm,
)
from helpers.query_helper import (
    build_redshift_circ_trans_query,
    build_sierra_circ_trans_query,
)
from nypl_py_utils.functions.log_helper import create_log


class CircTransAlarms(Alarm):
    def __init__(self, redshift_client, sierra_client):
        super().__init__(redshift_client)
        self.sierra_client = sierra_client
        self.logger = create_log("circ_trans_alarms")

    def run_checks(self):
        self.logger.info("Circ Trans")
        redshift_parameters = [
            ("patron_circ_trans", "transaction_et"),
            (
                "item_circ_trans",
                "CONVERT_TIMEZONE('America/New_York', transaction_timestamp)::DATE",
            ),
        ]

        sierra_query = build_sierra_circ_trans_query(self.yesterday)
        sierra_count = self.get_record_count(self.sierra_client, sierra_query)
        for parameter_tuple in redshift_parameters:
            redshift_table = parameter_tuple[0] + self.redshift_suffix
            redshift_query = build_redshift_circ_trans_query(
                redshift_table, parameter_tuple[1], self.yesterday
            )
            redshift_count = self.get_record_count(self.redshift_client, redshift_query)
            check_redshift_mismatch_alarm(
                logger=self.logger,
                database_type="Sierra circ trans",
                redshift_table=redshift_table,
                database_count=sierra_count,
                redshift_count=redshift_count,
            )

        check_no_records_found_alarm(
            logger=self.logger,
            database_count=sierra_count,
            conditional=self.run_added_tests,
            database_type="Sierra circ trans",
            date=self.yesterday,
        )
