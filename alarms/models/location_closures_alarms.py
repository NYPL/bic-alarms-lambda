from alarms.alarm import Alarm
from helpers.alarm_helper import check_no_records_found_alarm
from helpers.query_helper import (
    build_redshift_closures_count_query,
    build_redshift_closures_location_id_query,
)
from nypl_py_utils.functions.log_helper import create_log


class LocationClosuresAlarms(Alarm):
    def __init__(self, redshift_client):
        super().__init__(redshift_client)
        self.logger = create_log("location_closures_alarms")

    def run_checks(self):
        self.logger.info("LOCATION CLOSURES")

        redshift_table = "location_closures_v2" + self.redshift_suffix
        redshift_branch_codes_table = "branch_codes_map" + self.redshift_suffix
        count_query = build_redshift_closures_count_query(
            redshift_table, self.yesterday
        )
        location_id_query = build_redshift_closures_location_id_query(
            redshift_table, redshift_branch_codes_table, self.yesterday
        )

        self.redshift_client.connect()
        count = int(self.redshift_client.execute_query(count_query)[0][0])
        unknown_location_ids = self.redshift_client.execute_query(location_id_query)
        self.redshift_client.close_connection()

        check_no_records_found_alarm(
            logger=self.logger,
            database_count=count,
            conditional=True,
            database_type=redshift_table,
            date=self.yesterday,
        )
        self.check_unknown_location_ids(unknown_location_ids)

    def check_unknown_location_ids(self, unknown_location_ids):
        if unknown_location_ids:
            self.logger.error(
                f"The following location_ids are unknown: {unknown_location_ids}"
            )
