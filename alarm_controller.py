import os

from alarms.models.circ_trans_alarms import CircTransAlarms
from alarms.models.holds_alarms import HoldsAlarms
from alarms.models.location_visits_alarms import LocationVisitsAlarms
from alarms.models.patron_info_alarms import PatronInfoAlarms
from alarms.models.pc_reserve_alarms import PcReserveAlarms
from alarms.models.sierra_codes.sierra_itype_codes_alarms import SierraItypeCodesAlarms
from alarms.models.sierra_codes.sierra_location_codes_alarms import SierraLocationCodesAlarms
from alarms.models.sierra_codes.sierra_stat_group_codes_alarms import SierraStatGroupCodesAlarms

from nypl_py_utils.classes.kms_client import KmsClient
from nypl_py_utils.classes.mysql_client import MySQLClient
from nypl_py_utils.classes.postgresql_client import PostgreSQLClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.functions.log_helper import create_log

class AlarmController:
    """Class for orchestrating various alarms"""

    def __init__(self):
        self.logger = create_log('alarm_controller')
        self.alarms= self._setup_alarms()
        kms_client = KmsClient()
        self._setup_database_clients(kms_client)
        kms_client.close()
    
    def _setup_alarms(self):
        self.logger.info('Setting up alarms...')
        alarms = []
        alarms.append(CircTransAlarms(self.logger, self.redshift_client, self.sierra_client))
        alarms.append(HoldsAlarms(self.logger, self.redshift_client))
        alarms.append(LocationVisitsAlarms(self.logger, self.redshift_client))
        alarms.append(PatronInfoAlarms(self.logger, self.redshift_client))
        alarms.append(PcReserveAlarms(self.logger, self.redshift_client, self.envisionware_client))
        alarms.append(SierraItypeCodesAlarms(self.logger, self.redshift_client, self.sierra_client))
        alarms.append(SierraLocationCodesAlarms(self.logger, self.redshift_client, self.sierra_client))
        alarms.append(SierraStatGroupCodesAlarms(self.logger, self.redshift_client, self.sierra_client))
        return alarms

    def _setup_database_clients(self, kms_client):
        self.redshift_client = RedshiftClient(
            kms_client.decrypt(os.environ['REDSHIFT_DB_HOST']),
            os.environ['REDSHIFT_DB_NAME'],
            kms_client.decrypt(os.environ['REDSHIFT_DB_USER']),
            kms_client.decrypt(os.environ['REDSHIFT_DB_PASSWORD']))
        self.sierra_client = PostgreSQLClient(
            kms_client.decrypt(os.environ['SIERRA_DB_HOST']),
            os.environ['SIERRA_DB_PORT'],
            os.environ['SIERRA_DB_NAME'],
            kms_client.decrypt(os.environ['SIERRA_DB_USER']),
            kms_client.decrypt(os.environ['SIERRA_DB_PASSWORD']))
        self.envisionware_client = MySQLClient(
            kms_client.decrypt(os.environ['ENVISIONWARE_DB_HOST']),
            os.environ['ENVISIONWARE_DB_PORT'],
            os.environ['ENVISIONWARE_DB_NAME'],
            kms_client.decrypt(os.environ['ENVISIONWARE_DB_USER']),
            kms_client.decrypt(os.environ['ENVISIONWARE_DB_PASSWORD']))
    
    def run_alarms(self):
        for e in self.alarms:
            e.run_checks()