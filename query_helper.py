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


def build_sierra_circ_trans_query(date):
    return _SIERRA_CIRC_TRANS_QUERY.format(date=date)


def build_sierra_new_patrons_query(start_date, end_date):
    return _SIERRA_NEW_PATRONS_QUERY.format(
        start_date=start_date, end_date=end_date)


def build_sierra_deleted_patrons_query(start_date, end_date):
    return _SIERRA_DELETED_PATRONS_QUERY.format(
        start_date=start_date, end_date=end_date)


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
