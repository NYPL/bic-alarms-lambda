from alarms.alarm import Alarm
from helpers.query_helper import (
    build_redshift_branch_codes_duplicate_query,
    build_redshift_branch_codes_hours_query,
)
from nypl_py_utils.functions.log_helper import create_log


class BranchCodesMapAlarms(Alarm):
    def __init__(self, redshift_client):
        super().__init__(redshift_client)
        self.logger = create_log("branch_codes_map_alarms")

    def run_checks(self):
        self.logger.info("\nBRANCH CODES MAP\n")
        branch_codes_table = "branch_codes_map" + self.redshift_suffix
        location_hours_table = "location_hours" + self.redshift_suffix

        self.redshift_client.connect()
        duplicates = self.redshift_client.execute_query(
            build_redshift_branch_codes_duplicate_query(branch_codes_table)
        )
        if self.run_added_tests:
            mismatched_hours = self.redshift_client.execute_query(
                build_redshift_branch_codes_hours_query(
                    location_hours_table, branch_codes_table
                )
            )
        else:
            mismatched_hours = []
        self.redshift_client.close_connection()

        ids_not_in_map = [el[0] for el in mismatched_hours if el[1] is None]
        ids_not_in_hours = [el[1] for el in mismatched_hours if el[0] is None]
        self.check_duplicate_sierra_codes(duplicates)
        self.check_hours_ids_without_mapping(ids_not_in_map)
        self.check_map_ids_without_hours(ids_not_in_hours)

    def check_duplicate_sierra_codes(self, duplicates):
        if len(duplicates) > 0:
            self.logger.error(
                "The following Sierra branch codes map to more than one Drupal branch "
                "code: {}".format(duplicates)
            )

    def check_hours_ids_without_mapping(self, ids_not_in_map):
        if len(ids_not_in_map) > 0:
            self.logger.error(
                "The following Drupal branch codes have location hours but do not have "
                "a known Sierra branch mapping: {}".format(ids_not_in_map)
            )

    def check_map_ids_without_hours(self, ids_not_in_hours):
        if len(ids_not_in_hours) > 0:
            self.logger.error(
                "The following Sierra branch codes do not have known hours: {}".format(
                    ids_not_in_hours
                )
            )
