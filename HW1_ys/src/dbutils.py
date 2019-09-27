from src.BaseDataTable import BaseDataTable
import pymysql

import logging
logger = logging.getLogger()


def get_connection(connect_info):
    """

    :param connect_info: A dictionary containing the information necessary to make a PyMySQL connection.
    :return: The connection. May raise an Exception/Error.
    """

    cnx = pymysql.connect(**connect_info)
    return cnx


def run_q(sql, args=None, fields=None, fetch=True, cur=None, conn=None, commit=True):
    '''
    Helper function to run an SQL statement.

    This is a modification that better supports HW1. An RDBDataTable MUST have a connection specified by
    the connection information. This means that this implementation of run_q MUST NOT try to obtain
    a defailt connection.

    :param sql: SQL template with placeholders for parameters. Canno be NULL.
    :param args: Values to pass with statement. May be null.
    :param fetch: Execute a fetch and return data if TRUE.
    :param conn: The database connection to use. This cannot be NULL, unless a cursor is passed.
        DO NOT PASS CURSORS for HW1.
    :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
        DO NOT PASS CURSORS for HW1.
    :param commit: This is wizard stuff. Do not worry about it.

    :return: A pair of the form (execute response, fetched data). There will only be fetched data if
        the fetch parameter is True. 'execute response' is the return from the connection.execute, which
        is typically the number of rows effected.
    '''

    cursor_created = False
    connection_created = False

    try:

        if conn is None:
            raise ValueError("In this implementation, conn cannot be None.")

        if fields:
            sql = sql.format(",".join(fields))

        if cur is None:
            cursor_created = True
            cur = conn.cursor()

        if args is not None:
            log_message = cur.mogrify(sql, args)
        else:
            log_message = sql

        logger.debug("Executing SQL = " + log_message)

        res = cur.execute(sql, args)

        if fetch:
            data = cur.fetchall()
        else:
            data = None

        # Do not ask.
        if commit == True:
            conn.commit()

    except Exception as e:
        raise(e)

    return (res, data)