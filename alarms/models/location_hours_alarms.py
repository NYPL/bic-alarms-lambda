from alarms.alarm import Alarm
from helpers.query_helper import (
    build_redshift_hours_current_query,
    build_redshift_hours_location_id_query,
)
from nypl_py_utils.functions.log_helper import create_log


class LocationHoursAlarms(Alarm):
    def __init__(self, redshift_client):
        super().__init__(redshift_client)
        self.logger = create_log("location_hours_alarms")

    def run_checks(self):
        self.logger.info("Location Hours")

        redshift_table = "location_hours_v2" + self.redshift_suffix
        redshift_branch_codes_table = "branch_codes_map" + self.redshift_suffix
        current_query = build_redshift_hours_current_query(redshift_table)
        location_id_query = build_redshift_hours_location_id_query(
            redshift_table, redshift_branch_codes_table, self.yesterday
        )

        self.redshift_client.connect()
        non_current_loc_days = self.redshift_client.execute_query(current_query)
        unknown_location_ids = self.redshift_client.execute_query(location_id_query)
        self.redshift_client.close_connection()

        self.check_non_current(non_current_loc_days)
        self.check_unknown_location_ids(unknown_location_ids)

    def check_non_current(self, non_current_loc_days):
        if non_current_loc_days:
            self.logger.error(
                "The following (location_id, weekday) combinations did not contain "
                f"exactly one current row: {non_current_loc_days}"
            )

    def check_unknown_location_ids(self, unknown_location_ids):
        if unknown_location_ids:
            self.logger.error(
                f"The following location_ids are unknown: {unknown_location_ids}"
            )
