from alarms.alarm import Alarm
from datetime import timedelta
from helpers.query_helper import (
    build_envisionware_pc_reserve_query,
    build_redshift_pc_reserve_query,
)
from helpers.log_helper import (
    build_redshift_mismatch_log,
    build_no_records_found_log
)
from nypl_py_utils.functions.log_helper import create_log


class PcReserveAlarms(Alarm):
    def __init__(self, redshift_client, envisionware_client):
        super().__init__(redshift_client)
        self.envisionware_client = envisionware_client
        self.logger = create_log("pc_reserve_alarms")

    def run_checks(self):
        datetime_to_test = self.yesterday_date
        if not self.run_added_tests:
            datetime_to_test = self.yesterday_date - timedelta(days=1)
        date = datetime_to_test.isoformat()

        self.logger.info("\nPC RESERVE: {}\n".format(date))
        envisionware_query = build_envisionware_pc_reserve_query(date)
        envisionware_count = self.get_record_count(
            self.envisionware_client, envisionware_query
        )

        redshift_table = "pc_reserve" + self.redshift_suffix
        redshift_query = build_redshift_pc_reserve_query(redshift_table, date)
        redshift_count = self.get_record_count(self.redshift_client, redshift_query)
        if envisionware_count != redshift_count:
            mismatch_log = build_redshift_mismatch_log(database_type="Envisionware PcReserve",
                                                       redshift_table="PcReserve",
                                                       database_count=envisionware_count,
                                                       redshift_count=redshift_count)
            self.logger.error(mismatch_log)
        elif envisionware_count == 0 and datetime_to_test.weekday() != 6:
            no_records_log = build_no_records_found_log(database_type="PcReserve",
                                                        date=date)
            self.logger.error(no_records_log)