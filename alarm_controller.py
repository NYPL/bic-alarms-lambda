import os

from alarms.models.circ_trans_alarms import CircTransAlarms
from alarms.models.granular_location_visits_alarms import GranularLocationVisitsAlarms
from alarms.models.holds_alarms import HoldsAlarms
from alarms.models.overdrive_checkouts_alarms import OverDriveCheckoutsAlarms
from alarms.models.patron_info_alarms import PatronInfoAlarms
from alarms.models.pc_reserve_alarms import PcReserveAlarms
from alarms.models.sierra_codes.sierra_itype_codes_alarms import SierraItypeCodesAlarms
from alarms.models.sierra_codes.sierra_location_codes_alarms import (
    SierraLocationCodesAlarms,
)
from alarms.models.sierra_codes.sierra_stat_group_codes_alarms import (
    SierraStatGroupCodesAlarms,
)

from nypl_py_utils.classes.kms_client import KmsClient
from nypl_py_utils.classes.mysql_client import MySQLClient
from nypl_py_utils.classes.postgresql_client import PostgreSQLClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.functions.log_helper import create_log


class AlarmController:
    """Class for orchestrating various alarms"""

    def __init__(self):
        self.logger = create_log("alarm_controller")
        kms_client = KmsClient()
        self._setup_database_clients(kms_client)
        self.alarms = self._setup_alarms()
        kms_client.close()

    def _setup_database_clients(self, kms_client):
        self.redshift_client = RedshiftClient(
            kms_client.decrypt(os.environ["REDSHIFT_DB_HOST"]),
            os.environ["REDSHIFT_DB_NAME"],
            kms_client.decrypt(os.environ["REDSHIFT_DB_USER"]),
            kms_client.decrypt(os.environ["REDSHIFT_DB_PASSWORD"]),
        )
        self.sierra_client = PostgreSQLClient(
            kms_client.decrypt(os.environ["SIERRA_DB_HOST"]),
            os.environ["SIERRA_DB_PORT"],
            os.environ["SIERRA_DB_NAME"],
            kms_client.decrypt(os.environ["SIERRA_DB_USER"]),
            kms_client.decrypt(os.environ["SIERRA_DB_PASSWORD"]),
        )
        self.envisionware_client = MySQLClient(
            kms_client.decrypt(os.environ["ENVISIONWARE_DB_HOST"]),
            os.environ["ENVISIONWARE_DB_PORT"],
            os.environ["ENVISIONWARE_DB_NAME"],
            kms_client.decrypt(os.environ["ENVISIONWARE_DB_USER"]),
            kms_client.decrypt(os.environ["ENVISIONWARE_DB_PASSWORD"]),
        )
        self.overdrive_credentials = (
            kms_client.decrypt(os.environ["OVERDRIVE_USERNAME"]),
            kms_client.decrypt(os.environ["OVERDRIVE_PASSWORD"]),
        )

    def _setup_alarms(self):
        self.logger.info("Setting up alarms...")
        return [
            CircTransAlarms(self.redshift_client, self.sierra_client),
            GranularLocationVisitsAlarms(self.redshift_client),
            HoldsAlarms(self.redshift_client),
            OverDriveCheckoutsAlarms(self.redshift_client, self.overdrive_credentials),
            PatronInfoAlarms(self.redshift_client, self.sierra_client),
            PcReserveAlarms(self.redshift_client, self.envisionware_client),
            SierraItypeCodesAlarms(self.redshift_client, self.sierra_client),
            SierraLocationCodesAlarms(self.redshift_client, self.sierra_client),
            SierraStatGroupCodesAlarms(self.redshift_client, self.sierra_client),
        ]

    def run_alarms(self):
        for e in self.alarms:
            e.run_checks()
