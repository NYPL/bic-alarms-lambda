# BICAlarmsLambda

This repository contains the code used by the [BICAlarms-qa](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/BICAlarms-qa?newFunction=true&tab=code) and [BICAlarms-production](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/BICAlarms-production?newFunction=true&tab=code) AWS lambda functions. It is responsible for ensuring the data in the BIC is fresh and high quality.

Currently, the code will log an error (triggering an alarm to fire) under the following circumstances:
* When the number of circ trans records in Sierra and Redshift differs for the previous day
* When there are no circ trans records in Sierra for the previous day
* When there are no holds updated in Redshift for the previous day
* When an invalid hold appears in Redshift for the previous day
* When the number of PC reserve records in Envisionware and Redshift differs for the previous day
* When there are no PC reserve records in Sierra for the previous day
* When the number of newly created/deleted patron records in Sierra and Redshift differs for any day in the previous week
* When there are no newly created patron records in Sierra for the previous any day in the previous week
* When there are fewer than 10000 new location visits records for the previous day
* When a given location visits (site id, orbit, increment start) combination does not map to exactly one fresh row
* When the number of active itype/location/stat group codes in Sierra and Redshift differs
* When there are duplicate active itype/location/stat group codes in Redshift
* When there are active itype/location/stat group codes in Redshift without the necessary additional fields populated

## Git workflow
This repo uses the [Main-QA-Production](https://github.com/NYPL/engineering-general/blob/main/standards/git-workflow.md#main-qa-production) git workflow.

`main` has the latest and greatest commits, `qa` has what's in our QA environment, and `production` has what's in our production environment.

## How do I make a new alarm?
To create a new alarm, set up a model for said alarm in the [alarms/models](alarms/models) folder. Here are some examples:
* [CircTransAlarms](alarms/models/circ_trans_alarms.py) = General class containing all alarms related to circ trans records. *Each alarm is its own function, as is the norm*
* [PatronInfoAlarms](alarms/models/patron_info_alarms.py) = General class with patron record related alerts

Make sure to import whatever database client is needed as well (ex. Sierra, Envisionware). After creating your new alarm, add the alarm object to the [AlarmController](alarm_controller.py).

## Local development
Before running the code, make sure the following environment vars are set up in either your bash or zsh profile:
```
export AWS_ACCESS_KEY_ID=<AWS access key ID>
export AWS_SECRET_ACCESS_KEY=<AWS secret key associated with key ID>
```

After setting up said vars, run the following on your command line:
```
make run
```
The application logs should output to your terminal.

## Deployment
CI/CD is not enabled. To deploy a new version of this function, first modify the code in the git repo and open a pull request to the appropriate environment branch. Then run `source deployment_script.sh` and upload the resulting zip. Note that if any files are added or deleted, this script must be modified. For more information, see the directions [here](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html).

## Environment variables
The following environment variables are required for the code to run. The variables marked as encrypted should have been encrypted via KMS.

| Name        | Notes           |
| ------------- | ------------- |
| `ENVIRONMENT` | The environment in which to run the alarms. Certain alarms are only run when the environment is `production`. |
| `REDSHIFT_DB_HOST` | Encrypted Redshift cluster endpoint |
| `REDSHIFT_DB_NAME` | Which Redshift database to query (either `dev`, `qa`, or `production`) |
| `REDSHIFT_DB_USER` | Encrypted Redshift user |
| `REDSHIFT_DB_PASSWORD` | Encrypted Redshift password for the user |
| `SIERRA_DB_HOST` | Encrypted Sierra host |
| `SIERRA_DB_PORT` | Always `1032` |
| `SIERRA_DB_NAME` | Always `iii` |
| `SIERRA_DB_USER` | Encrypted Sierra user |
| `SIERRA_DB_PASSWORD` | Encrypted Sierra password for the user |
| `ENVISIONWARE_DB_HOST` | Encrypted Envisionware host |
| `ENVISIONWARE_DB_PORT` | Always `1032` |
| `ENVISIONWARE_DB_NAME` | Always `iii` |
| `ENVISIONWARE_DB_USER` | Encrypted Envisionware user |
| `ENVISIONWARE_DB_PASSWORD` | Encrypted Envisionware password for the user |
