import json

from alarm_controller import AlarmController
from nypl_py_utils.functions.log_helper import create_log


def lambda_handler(event, context):
    logger = create_log('lambda_function')
    logger.info('Starting lambda processing')

    alarm_controller = AlarmController()
    try:
        alarm_controller.redshift_client.connect()
        alarm_controller.run_circ_trans_alarm()
        alarm_controller.run_pc_reserve_alarm()
        alarm_controller.run_patron_info_alarm()
        alarm_controller.redshift_client.close_connection()
    except Exception as e:
        alarm_controller.redshift_client.close_connection()
        logger.error('Error running alarms: {}'.format(e))
        raise e

    logger.info('Finished lambda processing')
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Job ran successfully."
        }),
    }
