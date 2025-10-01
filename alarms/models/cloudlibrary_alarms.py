from alarms.alarm import Alarm
from datetime import timedelta
from helpers.alarm_helper import check_no_records_found_alarm
from helpers.query_helper import build_redshift_ebook_query
from nypl_py_utils.functions.log_helper import create_log


class CloudLibraryAlarms(Alarm):
    def __init__(self, redshift_client):
        super().__init__(redshift_client)
        self.logger = create_log("cloudlibrary_alarms")

    def run_checks(self):
        # Checks for data from 5 days ago (roughly
        # how long it should take to enter the BIC)
        date_to_test = self.yesterday_date - timedelta(days=4)
        self.logger.info("cloudLibrary")
        redshift_table = "cloudlibrary_transactions" + self.redshift_suffix

        self.logger.info(
            f"Checking CL record count from 4 days prior ({date_to_test})..."
        )
        redshift_query = build_redshift_ebook_query(redshift_table, date_to_test)
        redshift_count = self.get_record_count(self.redshift_client, redshift_query)
        check_no_records_found_alarm(
            logger=self.logger,
            database_count=redshift_count,
            conditional=True,
            database_type="cloudLibrary",
            date=date_to_test,
        )
