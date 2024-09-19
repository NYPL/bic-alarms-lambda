import os

from alarm_controller import AlarmController
from datetime import datetime, timedelta, timezone
from nypl_py_utils.functions.log_helper import create_log
from nypl_py_utils.functions.config_helper import load_env_file


def main():
    load_env_file(os.environ["ENVIRONMENT"], "config/{}.yaml")
    logger = create_log("main")
    logger.info("Starting alarms")

    alarm_controller = AlarmController()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    logger.info("Running alarms for {}".format(yesterday))
    try:
        alarm_controller.run_alarms()
    except Exception as e:
        alarm_controller.redshift_client.close_connection()
        logger.error("Error running alarms: {}".format(e))
        raise e

    logger.info("Finished alarms")


if __name__ == "__main__":
    main()
