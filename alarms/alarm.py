import os

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone


class Alarm(ABC):
    def __init__(self, redshift_client):
        self.redshift_client = redshift_client
        self.redshift_suffix = (
            ""
            if os.environ["REDSHIFT_DB_NAME"] == "production"
            else ("_" + os.environ["REDSHIFT_DB_NAME"])
        )
        self.run_added_tests = (
            os.environ["ENVIRONMENT"] == "production"
            or os.environ["ENVIRONMENT"] == "test"
        )
        self.yesterday_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        self.yesterday = self.yesterday_date.isoformat()

    def get_record_count(self, client, query):
        client.connect()
        result = client.execute_query(query)
        client.close_connection()
        return int(result[0][0])

    @abstractmethod
    def run_checks(self):
        return None
