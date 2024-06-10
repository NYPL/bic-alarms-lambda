import json
import os

from alarm_controller import AlarmController
from datetime import datetime, timedelta, timezone
from nypl_py_utils.functions.log_helper import create_log


def lambda_handler(event, context):
    if os.environ["ENVIRONMENT"] == "devel":
        from nypl_py_utils.functions.config_helper import load_env_file

        load_env_file("devel", "config/{}.yaml")
    logger = create_log("lambda_function")
    logger.info("Starting lambda processing")

    alarm_controller = AlarmController()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    logger.info("Running alarms for {}".format(yesterday))
    try:
        alarm_controller.run_alarms()
    except Exception as e:
        alarm_controller.redshift_client.close_connection()
        logger.error("Error running alarms: {}".format(e))
        raise e

    logger.info("Finished lambda processing")
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Job ran successfully."}),
    }
