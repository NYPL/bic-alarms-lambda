_REDSHIFT_MISMATCH_LOG = (
    'Number of {database_type} records does not match number '
    'of Redshift {redshift_table} records: {database_count} '
    '{database_type} records and {redshift_count} Redshift records'
)

_NO_RECORDS_FOUND_LOG = (
    'No {database_type} records found for all of {date}'
)

_SIERRA_NULL_CODES_LOG = (
    'The following {code_type} have a null value for one or more of '
    'their inferred columns: {codes}'
)

_SIERRA_DUPLICATE_CODE_LOG = (
    'Duplicate {code_type} codes found in Redshift: {total_count} '
    'total active {code_type} codes but only {distinct_count} '
    'distinct active {code_type} codes'
)

def build_redshift_mismatch_log(database_type, redshift_table,
                                database_count, redshift_count):
    return _REDSHIFT_MISMATCH_LOG.format(
        database_type=database_type, redshift_table=redshift_table,
        database_count=database_count, redshift_count=redshift_count)

def build_no_records_found_log(database_type, date):
    return _NO_RECORDS_FOUND_LOG.format(
        database_type=database_type, date=date)

def build_sierra_null_codes_log(code_type, codes):
    return _SIERRA_NULL_CODES_LOG.format(code_type=code_type, 
                                        codes=codes)

def build_sierra_duplicate_code_log(code_type, total_count, 
                                    distinct_count):
    return _SIERRA_DUPLICATE_CODE_LOG.format(code_type=code_type, 
                                             total_count=total_count,
                                             distinct_count=distinct_count)