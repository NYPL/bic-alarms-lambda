import os

from alarms.alarm import Alarm
from datetime import timedelta
from helpers.query_helper import build_redshift_daily_location_visits_query
from nypl_py_utils.classes.s3_client import S3Client
from nypl_py_utils.functions.log_helper import create_log


class DailyLocationVisitsAlarms(Alarm):
    def __init__(self, redshift_client):
        super().__init__(redshift_client)
        self.logger = create_log("daily_location_visits_alarms")

    def run_checks(self):
        date_to_test = (self.yesterday_date - timedelta(days=29)).isoformat()
        self.logger.info(f"\nDAILY LOCATION VISITS: {date_to_test}\n")
        s3_client = S3Client(
            os.environ["SHOPPERTRAK_S3_BUCKET"], os.environ["SHOPPERTRAK_S3_RESOURCE"]
        )
        all_shoppertrak_sites = set(s3_client.fetch_cache())
        s3_client.close()

        redshift_table = "daily_location_visits" + self.redshift_suffix
        redshift_query = build_redshift_daily_location_visits_query(
            redshift_table, date_to_test
        )

        self.redshift_client.connect()
        redshift_results = self.redshift_client.execute_query(redshift_query)
        self.redshift_client.close_connection()

        redshift_sites = []
        redshift_healthy = []
        for shoppertrak_site, is_all_healthy in redshift_results:
            redshift_sites.append(shoppertrak_site)
            redshift_healthy.append(int(is_all_healthy))

        self.check_redshift_duplicate_sites_alarm(redshift_sites)
        self.check_redshift_missing_sites_alarm(redshift_sites, all_shoppertrak_sites)
        self.check_redshift_extra_sites_alarm(redshift_sites, all_shoppertrak_sites)
        self.check_redshift_healthy_sites_alarm(redshift_healthy)

    def check_redshift_duplicate_sites_alarm(self, redshift_sites):
        seen_sites = set()
        duplicate_sites = set()
        for site in redshift_sites:
            if site in seen_sites:
                duplicate_sites.add(site)
            seen_sites.add(site)

        if duplicate_sites:
            self.logger.error(
                "The following ShopperTrak sites are duplicated: {}".format(
                    sorted(list(duplicate_sites))
                )
            )

    def check_redshift_missing_sites_alarm(self, redshift_sites, all_sites):
        missing_sites = all_sites.difference(set(redshift_sites))
        if missing_sites:
            self.logger.error(
                "The following ShopperTrak sites are missing: {}".format(
                    sorted(list(missing_sites))
                )
            )

    def check_redshift_extra_sites_alarm(self, redshift_sites, all_sites):
        extra_sites = set(redshift_sites).difference(all_sites)
        if extra_sites:
            self.logger.error(
                "The following unknown ShopperTrak site ids were found: {}".format(
                    sorted(list(extra_sites))
                )
            )

    def check_redshift_healthy_sites_alarm(self, redshift_healthy):
        percent_healthy = sum(redshift_healthy) / len(redshift_healthy)
        if percent_healthy < 0.5:
            self.logger.error(
                "Only {0:.2f}% of ShopperTrak sites were healthy".format(
                    percent_healthy * 100
                )
            )
