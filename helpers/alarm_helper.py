_REDSHIFT_MISMATCH_LOG = (
    "Number of {database_type} records does not match number "
    "of Redshift {redshift_table} records: {database_count} "
    "{database_type} records and {redshift_count} Redshift records"
)

_NO_RECORDS_FOUND_LOG = "No {database_type} records found for all of {date}"

_SIERRA_NULL_CODES_LOG = (
    "The following {code_type} have a null value for one or more of "
    "their inferred columns: {codes}"
)

_SIERRA_DUPLICATE_CODE_LOG = (
    "Duplicate {code_type} codes found in Redshift: {total_count} "
    "total active {code_type} codes but only {distinct_count} "
    "distinct active {code_type} codes"
)


def check_redshift_mismatch_alarm(
    logger, database_type, redshift_table, database_count, redshift_count
):
    if database_count != redshift_count:
        mismatch_log = _REDSHIFT_MISMATCH_LOG.format(
            database_type=database_type,
            redshift_table=redshift_table,
            database_count=database_count,
            redshift_count=redshift_count,
        )
        logger.error(mismatch_log)


def check_no_records_found_alarm(
    logger, database_count, conditional, database_type, date
):
    if database_count == 0 and conditional:
        no_records_log = _NO_RECORDS_FOUND_LOG.format(
            database_type=database_type, date=date
        )
        logger.error(no_records_log)


def check_sierra_null_codes_alarm(logger, null_codes, code_type):
    if len(null_codes) > 0:
        null_codes_log = _SIERRA_NULL_CODES_LOG.format(
            code_type=code_type, codes=null_codes
        )
        logger.error(null_codes_log)


def check_sierra_duplicate_code_alarm(logger, code_type, total_count, distinct_count):
    if total_count != distinct_count:
        duplicate_code_log = _SIERRA_DUPLICATE_CODE_LOG.format(
            code_type=code_type, total_count=total_count, distinct_count=distinct_count
        )
        logger.error(duplicate_code_log)
