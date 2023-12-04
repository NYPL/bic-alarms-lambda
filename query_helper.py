_SIERRA_CIRC_TRANS_QUERY = (
    "SELECT COUNT(id) FROM sierra_view.circ_trans "
    "WHERE (transaction_gmt AT TIME ZONE 'EST')::DATE = '{date}';")

_SIERRA_NEW_PATRONS_QUERY = '''
    SELECT (creation_date_gmt AT TIME ZONE 'EST')::DATE, COUNT(id)
    FROM sierra_view.record_metadata
    WHERE record_type_code = 'p'
        AND (creation_date_gmt AT TIME ZONE 'EST')::DATE >= '{start_date}'
        AND (creation_date_gmt AT TIME ZONE 'EST')::DATE < '{end_date}'
    GROUP BY (creation_date_gmt AT TIME ZONE 'EST')::DATE;'''

_SIERRA_DELETED_PATRONS_QUERY = '''
    SELECT deletion_date_gmt, COUNT(id)
    FROM sierra_view.record_metadata
    WHERE record_type_code = 'p'
        AND deletion_date_gmt >= '{start_date}'
        AND deletion_date_gmt < '{end_date}'
    GROUP BY deletion_date_gmt;'''

_SIERRA_CODE_COUNT_QUERY = 'SELECT COUNT(code) FROM {table};'

_SIERRA_ITYPES_COUNT_QUERY = (
    "SELECT COUNT(code) FROM sierra_view.itype_property_myuser "
    "WHERE TRIM(name) != '';")

_ENVISIONWARE_PC_RESERVE_QUERY = (
    "SELECT COUNT(pcrKey) FROM strad_bci "
    "WHERE DATE(pcrDateTime) = '{date}';")

_REDSHIFT_CIRC_TRANS_QUERY = (
    "SELECT COUNT(transaction_checksum) FROM {table} "
    "WHERE transaction_et = '{date}';")

_REDSHIFT_NEW_PATRONS_QUERY = '''
    SELECT creation_date_et, COUNT(patron_id)
    FROM {table}
    WHERE creation_date_et >= '{start_date}'
        AND creation_date_et < '{end_date}'
    GROUP BY creation_date_et;'''

_REDSHIFT_DELETED_PATRONS_QUERY = '''
    SELECT deletion_date_et, COUNT(patron_id)
    FROM {table}
    WHERE deletion_date_et >= '{start_date}'
        AND deletion_date_et < '{end_date}'
    GROUP BY deletion_date_et;'''

_REDSHIFT_PC_RESERVE_QUERY = (
    "SELECT COUNT(key) FROM {table} "
    "WHERE transaction_et = '{date}';")

_REDSHIFT_CODE_COUNTS_QUERY = (
    'SELECT COUNT({code}), COUNT(DISTINCT {code}) FROM {table} '
    'WHERE deletion_date IS NULL;')

_REDSHIFT_ITYPE_NULL_QUERY = '''
    SELECT code FROM {table}
    WHERE code != 0
        AND creation_date = '{date}'
        AND (is_research IS NULL
            OR age_category IS NULL
            OR is_print IS NULL);'''

_REDSHIFT_LOCATION_NULL_QUERY = '''
    SELECT location_code FROM {table}
    WHERE creation_date = '{date}'
        AND shelving_category IS NULL
        AND (research_branch IS NULL OR is_mixed_use = TRUE);'''

_REDSHIFT_STAT_GROUP_NULL_QUERY = '''
    SELECT stat_group_code FROM {table}
    WHERE creation_date = '{date}'
        AND normalized_branch_code IS NULL;'''

_REDSHIFT_STAT_GROUP_LOCATION_QUERY = '''
    SELECT stat_group_code FROM {stat_group_table}
    WHERE creation_date = '{date}'
        AND normalized_branch_code NOT IN
            (SELECT location_code FROM {location_table}
            WHERE deletion_date IS NULL);'''


def build_sierra_circ_trans_query(date):
    return _SIERRA_CIRC_TRANS_QUERY.format(date=date)


def build_sierra_new_patrons_query(start_date, end_date):
    return _SIERRA_NEW_PATRONS_QUERY.format(
        start_date=start_date, end_date=end_date)


def build_sierra_deleted_patrons_query(start_date, end_date):
    return _SIERRA_DELETED_PATRONS_QUERY.format(
        start_date=start_date, end_date=end_date)


def build_sierra_code_count_query(table):
    return _SIERRA_CODE_COUNT_QUERY.format(table=table)


def build_sierra_itypes_count_query():
    return _SIERRA_ITYPES_COUNT_QUERY


def build_envisionware_pc_reserve_query(date):
    return _ENVISIONWARE_PC_RESERVE_QUERY.format(date=date)


def build_redshift_circ_trans_query(table, date):
    return _REDSHIFT_CIRC_TRANS_QUERY.format(table=table, date=date)


def build_redshift_new_patrons_query(table, start_date, end_date):
    return _REDSHIFT_NEW_PATRONS_QUERY.format(
        table=table, start_date=start_date, end_date=end_date)


def build_redshift_deleted_patrons_query(table, start_date, end_date):
    return _REDSHIFT_DELETED_PATRONS_QUERY.format(
        table=table, start_date=start_date, end_date=end_date)


def build_redshift_pc_reserve_query(table, date):
    return _REDSHIFT_PC_RESERVE_QUERY.format(table=table, date=date)


def build_redshift_code_counts_query(code, table):
    return _REDSHIFT_CODE_COUNTS_QUERY.format(code=code, table=table)


def build_redshift_itype_null_query(itype_table, date):
    return _REDSHIFT_ITYPE_NULL_QUERY.format(table=itype_table, date=date)


def build_redshift_location_null_query(location_table, date):
    return _REDSHIFT_LOCATION_NULL_QUERY.format(
        table=location_table, date=date)


def build_redshift_stat_group_null_query(stat_group_table, date):
    return _REDSHIFT_STAT_GROUP_NULL_QUERY.format(
        table=stat_group_table, date=date)


def build_redshift_stat_group_location_query(stat_group_table, location_table,
                                             date):
    return _REDSHIFT_STAT_GROUP_LOCATION_QUERY.format(
        stat_group_table=stat_group_table, location_table=location_table,
        date=date)
