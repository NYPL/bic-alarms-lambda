from alarms.alarm import Alarm
from helpers.alarm_helper import (
    check_redshift_mismatch_alarm,
    check_no_records_found_alarm,
)
from helpers.overdrive_web_scraper import OverDriveWebScraper, OverDriveWebScraperError
from helpers.query_helper import build_redshift_overdrive_query
from nypl_py_utils.functions.log_helper import create_log


class OverDriveCheckoutsAlarms(Alarm):
    def __init__(self, redshift_client, overdrive_credentials):
        super().__init__(redshift_client)
        self.overdrive_client = OverDriveWebScraper(
            overdrive_credentials[0], overdrive_credentials[1]
        )
        self.logger = create_log("overdrive_checkouts_alarms")

    def run_checks(self):
        self.logger.info("\nOVERDRIVE CHECKOUTS\n")
        try:
            overdrive_count = self.overdrive_client.get_count(self.yesterday)
        except OverDriveWebScraperError:
            self.logger.error("Failed to scrape OverDrive Marketplace")
            return

        redshift_table = "overdrive_checkouts" + self.redshift_suffix
        redshift_query = build_redshift_overdrive_query(redshift_table, self.yesterday)
        redshift_count = self.get_record_count(self.redshift_client, redshift_query)
        check_redshift_mismatch_alarm(
            logger=self.logger,
            database_type="OverDrive Marketplace",
            redshift_table=redshift_table,
            database_count=overdrive_count,
            redshift_count=redshift_count,
        )

        check_no_records_found_alarm(
            logger=self.logger,
            database_count=overdrive_count,
            conditional=True,
            database_type="OverDrive Marketplace",
            date=self.yesterday,
        )
