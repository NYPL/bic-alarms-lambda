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