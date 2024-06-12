from alarms.alarm import Alarm
from datetime import timedelta
from helpers.alarm_helper import (
    redshift_mismatch_alarm,
    no_records_found_alarm
)
from helpers.query_helper import (
    build_envisionware_pc_reserve_query,
    build_redshift_pc_reserve_query,
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
        
        redshift_mismatch_alarm(logger=self.logger, 
                                database_type="Envisionware",
                                redshift_table="PcReserve",
                                database_count=envisionware_count,
                                redshift_count=redshift_count)
        
        # Al libraries are closed on Sunday, so don't fire an alarm then
        is_sunday = datetime_to_test.weekday() != 6
        no_records_found_alarm(logger=self.logger,
                               database_count=envisionware_count,
                               conditional=is_sunday,
                               database_type="PcReserve",
                               date=date)