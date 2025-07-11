from alarms.alarm import Alarm
from datetime import timedelta
from helpers.query_helper import (
    build_redshift_holds_deleted_query,
    build_redshift_holds_modified_query,
    build_redshift_holds_null_query,
    build_redshift_holds_query,
)
from nypl_py_utils.functions.log_helper import create_log


class HoldsAlarms(Alarm):
    def __init__(self, redshift_client):
        super().__init__(redshift_client)
        self.logger = create_log("holds_alarms")

    def run_checks(self):
        self.logger.info("HOLDS")
        # The update_timestamp is stored in UTC and the poller is run late at
        # night, so the date for the most recent day of data is today
        date_to_test = (self.yesterday_date + timedelta(days=1)).isoformat()
        self.redshift_client.connect()
        if self.run_added_tests:
            for table_name in ["hold_info", "queued_holds"]:
                redshift_table = table_name + self.redshift_suffix
                redshift_query = build_redshift_holds_query(
                    redshift_table, date_to_test
                )
                redshift_count = int(
                    self.redshift_client.execute_query(redshift_query)[0][0]
                )
                self.check_holds_not_updated_alarm(redshift_count, redshift_table)

        redshift_table = "hold_info" + self.redshift_suffix
        deleted_holds = self.redshift_client.execute_query(
            build_redshift_holds_deleted_query(redshift_table, date_to_test)
        )
        modified_holds = self.redshift_client.execute_query(
            build_redshift_holds_modified_query(redshift_table)
        )
        null_holds = self.redshift_client.execute_query(
            build_redshift_holds_null_query(redshift_table, date_to_test)
        )
        self.redshift_client.close_connection()

        self.check_olds_not_deleted_alarm(deleted_holds)
        self.check_immutable_hold_field_updated_alarm(modified_holds)
        self.check_null_hold_id_alarm(null_holds)

    def check_holds_not_updated_alarm(self, redshift_count, redshift_table):
        if redshift_count == 0:
            self.logger.error(
                ('"{table}" table not updated for all of {date} ' "(ET)").format(
                    table=redshift_table, date=self.yesterday
                )
            )

    def check_olds_not_deleted_alarm(self, deleted_holds):
        if len(deleted_holds) > 0:
            self.logger.error(
                "The following hold_ids appear despite having previously been "
                "marked as deleted: {}".format(deleted_holds)
            )

    def check_immutable_hold_field_updated_alarm(self, modified_holds):
        if len(modified_holds) > 0:
            self.logger.error(
                "The following hold_ids have an immutable field changing: "
                "{}".format(modified_holds)
            )

    def check_null_hold_id_alarm(self, null_holds):
        if len(null_holds) > 0:
            self.logger.error(
                "The following hold_ids have an improper null value: "
                "{}".format(null_holds)
            )
