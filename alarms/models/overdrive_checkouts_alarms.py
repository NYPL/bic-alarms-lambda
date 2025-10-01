from alarms.alarm import Alarm
from helpers.alarm_helper import (
    check_redshift_mismatch_alarm,
    check_no_records_found_alarm,
)
from helpers.overdrive_web_scraper import OverDriveWebScraper
from helpers.query_helper import (
    build_redshift_ebook_query,
    build_redshift_overdrive_duplicate_platform_query,
    build_redshift_overdrive_duplicate_query,
)
from nypl_py_utils.functions.log_helper import create_log


class OverDriveCheckoutsAlarms(Alarm):
    def __init__(self, redshift_client, overdrive_credentials):
        super().__init__(redshift_client)
        self.overdrive_client = OverDriveWebScraper(
            overdrive_credentials[0], overdrive_credentials[1]
        )
        self.logger = create_log("overdrive_checkouts_alarms")

    def run_checks(self):
        self.logger.info("OverDrive Checkouts")
        try:
            overdrive_count = self.overdrive_client.get_count(self.yesterday)
        except Exception as e:
            self.logger.error(f"Failed to scrape OverDrive Marketplace: {e}")
            return

        redshift_table = "overdrive_checkouts" + self.redshift_suffix
        redshift_query = build_redshift_ebook_query(redshift_table, self.yesterday)
        redshift_count = self.get_record_count(self.redshift_client, redshift_query)
        check_no_records_found_alarm(
            logger=self.logger,
            database_count=overdrive_count,
            conditional=True,
            database_type="OverDrive Marketplace",
            date=self.yesterday,
        )
        # If there are records in the table, perform further checks
        adjusted_redshift_count = self._adjust_redshift_count(
            redshift_table, redshift_count
        )
        check_redshift_mismatch_alarm(
            logger=self.logger,
            database_type="OverDrive Marketplace",
            redshift_table=redshift_table,
            database_count=overdrive_count,
            redshift_count=adjusted_redshift_count,
        )

    def _adjust_redshift_count(self, redshift_table, initial_redshift_count):
        """
        In OverDrive, when users download titles through different platforms, these
        transactions are all counted as one row. This is not the case in Redshift --
        these are separate rows that are otherwise identical aside from the
        `platform` column. This method accounts for this discrepancy.
        """
        self.redshift_client.connect()

        duplicate_checksums = self.redshift_client.execute_query(
            build_redshift_overdrive_duplicate_query(redshift_table, self.yesterday)
        )

        if not duplicate_checksums:
            # if the resulting tuple is empty, return original count
            return initial_redshift_count

        adjusted_redshift_count = initial_redshift_count
        duplicate_checksums = [checksum for checksum in duplicate_checksums[0]]
        for checksum in duplicate_checksums:
            platform_types = self.redshift_client.execute_query(
                build_redshift_overdrive_duplicate_platform_query(
                    redshift_table, self.yesterday, checksum
                )
            )
            adjusted_redshift_count -= len(platform_types) - 1

        self.redshift_client.close_connection()
        return adjusted_redshift_count
