_REDSHIFT_MISMATCH_LOG = (
    'Number of {database_type} records does not match number '
    'of Redshift {redshift_table} records: {database_count} '
    '{database_type} records and {redshift_count} Redshift records'
)

_NO_RECORDS_FOUND_LOG = (
    'No {database_type} records found for all of {date}'
)

def build_redshift_mismatch_log(database_type, redshift_table,
                                database_count, redshift_count):
    return _REDSHIFT_MISMATCH_LOG.format(
        database_type=database_type, redshift_table=redshift_table,
        database_count=database_count, redshift_count=redshift_count)

def build_no_records_found_log(database_type, date):
    return _NO_RECORDS_FOUND_LOG.format(
        database_type=database_type, date=date)