from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

class Alarm(ABC):
    def __init__(self, logger, run_added_tests,
                 redshift_client, redshift_suffix):
        self.logger = logger
        self.run_added_tests = run_added_tests
        self.redshift_client = redshift_client
        self.redshift_suffix = redshift_suffix
        self.yesterday_date = (
                datetime.now(timezone.utc) - timedelta(days=1)).date()
        self.yesterday = self.yesterday_date.isoformat()

    def get_record_count(self, client, query):
        client.connect()
        result = client.execute_query(query)
        client.close_connection()
        return int(result[0][0])
    
    @abstractmethod
    def run_checks(self):
        return False