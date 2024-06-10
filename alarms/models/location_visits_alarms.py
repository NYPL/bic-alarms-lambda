from alarms.alarm import Alarm
from datetime import timedelta
from helpers.query_helper import (
    build_redshift_location_visits_count_query,
    build_redshift_location_visits_duplicate_query,
    build_redshift_location_visits_stale_query,
)
from nypl_py_utils.functions.log_helper import create_log


class LocationVisitsAlarms(Alarm):
    def __init__(self, redshift_client):
        super().__init__(redshift_client)
        self.logger = create_log("location_visits_alarms")

    def run_checks(self):
        if not self.run_added_tests:
            return

        self.logger.info("\nLOCATION VISITS\n")
        redshift_table = "location_visits" + self.redshift_suffix
        stale_start_date = (self.yesterday_date - timedelta(days=30)).isoformat()
        redshift_count_query = build_redshift_location_visits_count_query(
            redshift_table, self.yesterday
        )
        redshift_duplicate_query = build_redshift_location_visits_duplicate_query(
            redshift_table, self.yesterday
        )
        redshift_stale_query = build_redshift_location_visits_stale_query(
            redshift_table, stale_start_date
        )

        self.redshift_client.connect()
        redshift_count = int(
            self.redshift_client.execute_query(redshift_count_query)[0][0]
        )
        redshift_duplicates = self.redshift_client.execute_query(
            redshift_duplicate_query
        )
        redshift_stale_rows = self.redshift_client.execute_query(redshift_stale_query)
        self.redshift_client.close_connection()

        self.new_location_visits_less_than_ten_thousand_alarm(
            redshift_count, redshift_table
        )
        self.redshift_duplicates_alarm(redshift_duplicates)
        self.redshift_stale_rows_alarm(redshift_stale_rows)

    def new_location_visits_less_than_ten_thousand_alarm(
        self, redshift_count, redshift_table
    ):
        if redshift_count < 10000:
            self.logger.error(
                (
                    "Found only {redshift_count} {redshift_table} rows for all of "
                    "{date}"
                ).format(
                    redshift_count=redshift_count,
                    redshift_table=redshift_table,
                    date=self.yesterday,
                )
            )

    def redshift_duplicates_alarm(self, redshift_duplicates):
        if len(redshift_duplicates) > 0:
            self.logger.error(
                "The following (shoppertrak_site_id, orbit, increment_start) "
                "combinations contain more than one fresh row: {}".format(
                    redshift_duplicates
                )
            )

    def redshift_stale_rows_alarm(self, redshift_stale_rows):
        if len(redshift_stale_rows) > 0:
            self.logger.error(
                "The following (shoppertrak_site_id, orbit, increment_start) "
                "combinations are marked as stale and have not been replaced "
                "with a fresh row: {}".format(redshift_stale_rows)
            )
