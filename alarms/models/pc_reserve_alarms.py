from alarms.alarm import Alarm
from datetime import timedelta
from helpers.query_helper import (
    build_envisionware_pc_reserve_query,
    build_redshift_pc_reserve_query,
)


class PcReserveAlarms(Alarm):
    def __init__(self, logger, redshift_client, envisionware_client):
        super().__init__(logger, redshift_client)
        self.envisionware_client = envisionware_client

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

        self.pc_reserve_envisionware_redshift_discrepancy_alarm(
            envisionware_count, redshift_count
        )

        self.pc_reserve_no_records_alarm(
            envisionware_count, redshift_count, datetime_to_test, date
        )

    def pc_reserve_envisionware_redshift_discrepancy_alarm(
        self, envisionware_count, redshift_count
    ):
        if envisionware_count != redshift_count:
            self.logger.error(
                (
                    "Number of Envisionware PcReserve records does not match "
                    "number of Redshift PcReserve records: {envisionware_count} "
                    "Envisionware records and {redshift_count} Redshift records"
                ).format(
                    envisionware_count=envisionware_count, redshift_count=redshift_count
                )
            )

    def pc_reserve_no_records_alarm(
        self, envisionware_count, redshift_count, datetime_to_test, date
    ):
        # All libraries are closed on Sunday, so don't fire an alarm then
        if (
            (envisionware_count == redshift_count)
            and envisionware_count == 0
            and datetime_to_test.weekday() != 6
        ):
            self.logger.error("No PcReserve records found for all of {}".format(date))
