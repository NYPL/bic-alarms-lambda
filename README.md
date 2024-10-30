# BICAlarmsLambda

This repository contains the code used by the [BICAlarms-qa](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/BICAlarms-qa?newFunction=true&tab=code) and [BICAlarms-production](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/BICAlarms-production?newFunction=true&tab=code) AWS lambda functions. It is responsible for ensuring the data in the BIC is fresh and high quality.

Currently, the code will log an error (triggering an alarm to fire) under the following circumstances:
* When the number of circ trans records in Sierra and Redshift differs for the previous day
* When there are no circ trans records in Sierra for the previous day
* When there are no holds updated in Redshift for the previous day
* When an invalid hold appears in Redshift for the previous day
* When the number of PC reserve records in Envisionware and Redshift differs for the previous day
* When there are no PC reserve records in Sierra for the previous day
* When the number of OverDrive checkout records online (via OverDrive Marketplace) and in Redshift differs for the previous day
* When there are no OverDrive checkout records online (via OverDrive Marketplace) for the previous day
* When the number of newly created/deleted patron records in Sierra and Redshift differs for any day in the previous week
* When there are no newly created patron records in Sierra for the previous any day in the previous week
* When a single Sierra branch code maps to multiple Drupal branch codes
* When a Drupal branch code in location_hours does not contain a mapping to a Sierra branch code
* When a Sierra branch code with a mapping to a Drupal branch code does not appear in location_hours
* When there are fewer than 10000 new location visits records for the previous day
* When a given location visits (site id, orbit, increment start) combination from the previous day contains multiple fresh rows
* When a given location visits (site id, orbit, increment start) combination from the previous thirty days contains only stale rows
* When the number of active itype/location/stat group codes in Sierra and Redshift differs
* When there are duplicate active itype/location/stat group codes in Redshift
* When there are active itype/location/stat group codes in Redshift without the necessary additional fields populated

## How do I make a new alarm?
To create a new alarm, set up a model for said alarm in the [alarms/models](alarms/models) directory. After creating your new alarm, add the alarm object to the [AlarmController](alarm_controller.py). Here are some examples:
* [PcReserveAlarms](alarms/models/pc_reserve_alarms.py): all generic alarms
* [LocationVisitsAlarms](alarms/models/location_visits_alarms.py): custom alarms -- *each custom alarm is its own function, as is the norm*

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

* Export your `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and run `make run`
* Alternatively, to build and run a Docker container, copy the `configs` directory in the `Dockerfile` and run:
```
docker build --platform linux/amd64 -t bic-alarms:local .

docker run --platform linux/amd64 -p 9000:8080 -e ENVIRONMENT=devel -e AWS_ACCESS_KEY_ID=<> -e AWS_SECRET_ACCESS_KEY=<> bic-alarms:local

# From a new terminal tab
curl "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

## Git workflow
This repo uses the [Main-QA-Production](https://github.com/NYPL/engineering-general/blob/main/standards/git-workflow.md#main-qa-production) git workflow.

`main` has the latest and greatest commits, `qa` has what's in our QA environment, and `production` has what's in our production environment.

### Ideal Workflow
- Cut a feature branch off of `main`
- Commit changes to your feature branch
- File a pull request against `main` and assign a reviewer
  - In order for the PR to be accepted, it must pass all unit tests, have no lint issues, and update the CHANGELOG (or contain the Skip-Changelog label in GitHub)
- After the PR is accepted, merge into `main`
- Merge `main` > `qa`
- Deploy app to QA and confirm it works
- Merge `qa` > `production`
- Deploy app to production and confirm it works

## Deployment
The poller is deployed as a Docker image to the `bic-alarms` repository in [ECR](https://us-east-1.console.aws.amazon.com/ecr/private-registry/repositories). From there, the appropriate Lambda's code is updated based on the image's tag (either `qa` or `production`). To upload a new QA version of the Lambda, create a new release in GitHub off of the `qa` branch and tag it `qa-vX.X.X`. The GitHub Actions deploy-qa workflow will then deploy the code to ECR and update the Lambda appropriately. To deploy to production, create the release from the `production` branch and tag it `production-vX.X.X`.

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
| `OVERDRIVE_USERNAME` | Encrypted OverDrive Marketplace username |
| `OVERDRIVE_PASSWORD` | Encrypted OverDrive Marketplace password for the user |