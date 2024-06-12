from alarms.alarm import Alarm
from datetime import timedelta
from helpers.alarm_helper import (
    check_redshift_mismatch_alarm,
    check_no_records_found_alarm,
)
from helpers.query_helper import (
    build_redshift_deleted_patrons_query,
    build_redshift_new_patrons_query,
    build_sierra_deleted_patrons_query,
    build_sierra_new_patrons_query,
)
from nypl_py_utils.functions.log_helper import create_log


class PatronInfoAlarms(Alarm):
    def __init__(self, redshift_client, sierra_client):
        super().__init__(redshift_client)
        self.sierra_client = sierra_client
        self.logger = create_log("patron_info_alarms")

    def run_checks(self):
        # If it's not Thursday, don't run this alarm, as the PatronInfo pollers
        # run weekly on Wednesday night
        if self.yesterday_date.weekday() != 2:
            return

        start_date = (self.yesterday_date - timedelta(days=7)).isoformat()
        self.logger.info(
            "\nPATRON INFO: {start}-{end}\n".format(
                start=start_date, end=self.yesterday
            )
        )

        self.sierra_client.connect()
        sierra_new_result = self.sierra_client.execute_query(
            build_sierra_new_patrons_query(start_date, self.yesterday)
        )
        sierra_deleted_result = self.sierra_client.execute_query(
            build_sierra_deleted_patrons_query(start_date, self.yesterday)
        )
        self.sierra_client.close_connection()
        sierra_new_counts = dict(sierra_new_result)
        sierra_deleted_counts = dict(sierra_deleted_result)

        redshift_table = "patron_info" + self.redshift_suffix
        self.redshift_client.connect()
        redshift_new_result = self.redshift_client.execute_query(
            build_redshift_new_patrons_query(redshift_table, start_date, self.yesterday)
        )
        redshift_deleted_result = self.redshift_client.execute_query(
            build_redshift_deleted_patrons_query(
                redshift_table, start_date, self.yesterday
            )
        )
        self.redshift_client.close_connection()
        redshift_new_counts = dict(redshift_new_result)
        redshift_deleted_counts = dict(redshift_deleted_result)

        for date in sierra_new_counts.keys() | redshift_new_counts.keys():
            sierra_count = int(sierra_new_counts.get(date, 0))
            redshift_count = int(redshift_new_counts.get(date, 0))
            check_redshift_mismatch_alarm(
                logger=self.logger,
                database_type="Sierra new patron",
                redshift_table=redshift_table,
                database_count=sierra_count,
                redshift_count=redshift_count,
            )
            check_no_records_found_alarm(
                logger=self.logger,
                database_count=sierra_count,
                conditional=self.run_added_tests,
                database_type="new patron",
                date=date,
            )

        # Don't check for whether there are no deleted patron records because
        # there are often days where this legitimately occurs
        for date in sierra_deleted_counts.keys() | redshift_deleted_counts.keys():
            sierra_count = int(sierra_deleted_counts.get(date, 0))
            redshift_count = int(redshift_deleted_counts.get(date, 0))
            check_redshift_mismatch_alarm(
                logger=self.logger,
                database_type="Sierra deleted patron",
                redshift_table="deleted patron",
                database_count=sierra_count,
                redshift_count=redshift_count,
            )
