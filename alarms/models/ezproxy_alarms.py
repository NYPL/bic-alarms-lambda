from alarms.alarm import Alarm
from helpers.query_helper import (
    build_redshift_ezproxy_count_query,
    build_redshift_ezproxy_duplicate_query,
)
from nypl_py_utils.functions.log_helper import create_log


class EZproxyAlarms(Alarm):
    def __init__(self, redshift_client):
        super().__init__(redshift_client)
        self.logger = create_log("ezproxy_alarms")

    def run_checks(self):
        self.logger.info("EZProxy")

        redshift_table = "ezproxy_sessions" + self.redshift_suffix
        count_query = build_redshift_ezproxy_count_query(redshift_table, self.yesterday)
        duplicate_query = build_redshift_ezproxy_duplicate_query(
            redshift_table, self.yesterday
        )

        self.redshift_client.connect()
        count = int(self.redshift_client.execute_query(count_query)[0][0])
        duplicates = self.redshift_client.execute_query(duplicate_query)
        self.redshift_client.close_connection()

        self.check_less_than_one_thousand_alarm(count, redshift_table)
        self.check_duplicates_alarm(duplicates)

    def check_less_than_one_thousand_alarm(self, count, redshift_table):
        if count < 1000:
            self.logger.error(
                "Found only {count} {redshift_table} rows for all of {date}".format(
                    count=count, redshift_table=redshift_table, date=self.yesterday
                )
            )

    def check_duplicates_alarm(self, duplicates):
        if len(duplicates) > 0:
            self.logger.error(
                "The following (session_id, patron_id, domain) combinations correspond "
                "to more than one row on {date}: {rows}".format(
                    date=self.yesterday, rows=duplicates
                )
            )
