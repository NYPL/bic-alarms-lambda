## 2025-10-01
### Added
- Add weekly cloudLibrary alarm

## 2025-09-30
### Added
- Add hours and closures v2 alarms

## 2025-09-15
### Added
- Add mismatched holds alarm

## 2025-08-25
### Added
- Add EZproxy alarms

### Fixed
- Update hold alarms to check for previously deleted holds using timestamps rather than only dates

## 2025-06-27
### Added
- Update OverDrive alerting to account for different download types

## 2025-02-19
### Added
- Update OverDrive web scraper to avoid session creation errors (use newest "headless" webdriver property)

## 2024-12-17
### Added
- Add DailyLocationVisits alarms checking that the Redshift daily_location_visits table has the right sites, has no duplicates, and contains mostly healthy data

## 2024-11-13
### Added
- Add BranchCodesMap alarms checking that it's in sync with all branches with location hours

### Fixed
- Rename LocationVisits alarms to GranularLocationVisits alarms and remove unnecessary alarms
- Delete old chrome_installation file

## 2024-11-12
### Fixed
- Gracefully handle any exception thrown by the OverDrive web scraper

## 2024-09-19
### Added
- Refactor code to run in an ECS cluster rather than as a Lambda

### Fixed
- Update Dockerfile to run Chrome correctly
- Update GitHub workflows to update ECS service instead of Lambda

## 2024-07-29
### Added
- Use Docker to install Google Chrome and run tests
- Include GitHub workflows for deploying Docker image to ECR and updating the Lambda
- Remove old deployment script now that GitHub Actions is in use

## 2024-07-25
### Added
- Added OverDriveCheckouts alarms checking that the number of records online at OverDrive Marketplace is greater than 0 and matches the number of records in Redshift

## 2024-07-01
### Fixed
- Removed defunct circ_trans Redshift tests now that the table has been replaced by item_circ_trans and patron_circ_trans

## 2024-06-27
### Fixed
- Changed PatronInfo alarm timezone from EST to ET, matching what the poller now uses

## 2024-06-12
### Added
- Refactored codebase