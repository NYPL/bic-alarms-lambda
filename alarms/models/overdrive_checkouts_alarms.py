from alarms.alarm import Alarm
from datetime import timedelta, timezone
from helpers.alarm_helper import (
    check_redshift_mismatch_alarm,
    check_no_records_found_alarm,
)
from helpers.overdrive_web_scraper import OverDriveWebScraper
from helpers.query_helper import (
    build_redshift_daily_ebook_query,
    build_redshift_daily_overdrive_platform_query,
    build_redshift_monthly_ebook_query,
    build_redshift_monthly_overdrive_platform_query,
)
from nypl_py_utils.functions.log_helper import create_log


class OverDriveCheckoutsAlarms(Alarm):
    def __init__(self, redshift_client, overdrive_credentials):
        super().__init__(redshift_client)
        self.overdrive_client = OverDriveWebScraper(
            overdrive_credentials[0], overdrive_credentials[1]
        )
        self.logger = create_log("overdrive_checkouts_alarms")
        self.daily_test_date = self.yesterday_date - timedelta(days=4)
        # This is to make monthly checks start date 36 days before the current day
        self.monthly_test_start_date = self.yesterday_date - timedelta(days=35)

    def run_checks(self):
        self.logger.info("OverDrive Checkouts")

        try:
            self.overdrive_client.overdrive_login()
            overdrive_count = self.overdrive_client.get_count(
                self.daily_test_date, self.daily_test_date
            )
        except Exception as e:
            self.logger.error(f"Failed to scrape OverDrive Marketplace: {e}")
            self.overdrive_client.quit_driver()
            return

        self.logger.info(f"Checking OD record count from ({self.daily_test_date})...")
        self._run_redshift_checks(overdrive_count, monthly_check=False)

        if self.run_added_tests:
            # The 'weekday' method returns Thursday as 3 so this check will run every Friday
            if self.yesterday_date.weekday() == 3:
                try:
                    self.logger.info(
                        f"Running weekly check for inconsistencies between overdrive and redshift for the dates between {self.monthly_test_start_date} - {self.daily_test_date}"
                    )
                    monthly_overdrive_count = self.overdrive_client.get_count(
                        self.monthly_test_start_date, self.daily_test_date
                    )
                    self._run_redshift_checks(
                        monthly_overdrive_count, monthly_check=True
                    )
                except Exception as e:
                    self.logger.error(f"Failed to scrape OverDrive Marketplace: {e}")
                    self.overdrive_client.quit_driver()
                    return

        self.overdrive_client.quit_driver()
        check_no_records_found_alarm(
            logger=self.logger,
            database_count=overdrive_count,
            conditional=True,
            database_type="OverDrive Marketplace",
            date=self.daily_test_date,
        )

    def _run_redshift_checks(self, overdrive_count, monthly_check=False):
        redshift_tables = ["patron_overdrive_checkouts", "title_overdrive_checkouts"]
        for redshift_table in redshift_tables:
            table = redshift_table + self.redshift_suffix
            if monthly_check:
                redshift_query = build_redshift_monthly_ebook_query(
                    table, self.monthly_test_start_date, self.daily_test_date
                )
            else:
                redshift_query = build_redshift_daily_ebook_query(
                    table, self.daily_test_date
                )
            redshift_count = self.get_record_count(self.redshift_client, redshift_query)

            if "patron_overdrive_checkouts" in table:
                redshift_count = self._adjust_redshift_count(
                    table, redshift_count, monthly_check
                )

            check_redshift_mismatch_alarm(
                logger=self.logger,
                database_type="OverDrive Marketplace",
                redshift_table=table,
                database_count=overdrive_count,
                redshift_count=redshift_count,
            )

    def _adjust_redshift_count(
        self, redshift_table, initial_redshift_count, monthly_check=False
    ):
        """
        In OverDrive, when users download titles through different platforms, these
        transactions are all counted as one row. This is not the case for the
        Redshift OverDrive patron table -- these are separate rows that are otherwise
        identical aside from the `platform` column. This method accounts for this discrepancy.
        """
        self.redshift_client.connect()

        if monthly_check:
            overdrive_duplicate_query = build_redshift_monthly_overdrive_platform_query(
                redshift_table,
                self.monthly_test_start_date,
                self.daily_test_date,
            )
        else:
            overdrive_duplicate_query = build_redshift_daily_overdrive_platform_query(
                redshift_table, self.daily_test_date
            )

        duplicate_result = self.redshift_client.execute_query(overdrive_duplicate_query)
        duplicate_count = (
            int(duplicate_result[0][0])
            if duplicate_result and duplicate_result[0][0]
            else 0
        )

        self.redshift_client.close_connection()
        return initial_redshift_count - duplicate_count
