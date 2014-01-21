# -*- coding: utf-8 -*-
from decimal import Decimal
from datetime import datetime, date
from uuid import UUID
from MySQLdb.converters import conversions
import umysql
from pony.orm import core
from pony.orm import dbapiprovider
from pony.orm.dbproviders.mysql import MySQLBuilder, MySQLSchema, \
    MySQLTranslator, MySQLUnicodeConverter, MySQLLongConverter, \
    MySQLRealConverter, MySQLBlobConverter,MySQLStrConverter, \
    MySQLUuidConverter


class UMySQLConnection(object):
    _result_query = None

    def __init__(self, *args, **kwargs):
        self.conn = umysql.Connection()

        if 'conv' in kwargs:
            conv = kwargs['conv']
        else:
            conv = conversions

        self.encoders = dict([(k, v) for k, v in conv.items()
                              if type(k) is not int])

    def literal(self, o):
        return self.escape(o, self.encoders)

    def escape(self, v, encoders):
        for t, f in encoders.iteritems():
            if isinstance(v, t):
                v = f(v, t)
                break

        for t, f in encoders.iteritems():
            if isinstance(v, t):
                return f(v, t)

        return v

    def connect(self, *args, **kwargs):
        self.conn.connect(
            kwargs.get('host', '127.0.0.1'),
            kwargs.get('post', 3306),
            kwargs.get('user', ''),
            kwargs.get('passwd', ''),
            kwargs.get('db', ''),
        )
        return self

    def close(self):
        return self.conn.close()

    def cursor(self):
        return self

    def query(self, query):
        return self.conn.query(query)

    def execute(self, query, args=None):
        if args is not None:
            if isinstance(args, dict):
                query = query % dict((key, self.literal(item))
                                     for key, item in args.iteritems())
            else:
                query = query % tuple([self.literal(item) for item in args])

        self._result_query = self.query(query)
        return self._result_query

    def commit(self):
        if core.debug:
            core.log_orm('COMMIT')
        self.conn.query('commit')

    def rollback(self):
        if core.debug:
            core.log_orm('ROLLBACK')
        self.conn.query('rollback')

    def fetchone(self):
        return self._result_query.rows[0] if self._result_query.rows else None

    def fetchall(self):
        return self._result_query.rows

    def fetchmany(self, size=None):
        if size is not None:
            return self._result_query.rows[:size - 1]
        else:
            return self._result_query.rows


class UMySQLProvider(dbapiprovider.DBAPIProvider):
    dialect = 'MySQL'
    paramstyle = 'format'
    quote_char = "`"
    max_name_len = 64
    table_if_not_exists_syntax = True
    index_if_not_exists_syntax = False
    select_for_update_nowait_syntax = False
    max_time_precision = default_time_precision = 0

    dbapi_module = UMySQLConnection()
    dbschema_cls = MySQLSchema
    translator_cls = MySQLTranslator
    sqlbuilder_cls = MySQLBuilder

    converter_classes = [
        (bool, dbapiprovider.BoolConverter),
        (unicode, MySQLUnicodeConverter),
        (str, MySQLStrConverter),
        (int, dbapiprovider.IntConverter),
        (long, MySQLLongConverter),
        (float, MySQLRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (buffer, MySQLBlobConverter),
        (datetime, dbapiprovider.DatetimeConverter),
        (date, dbapiprovider.DateConverter),
        (UUID, MySQLUuidConverter),
    ]

    def inspect_connection(provider, connection):
        cursor = connection.cursor()
        cursor.execute('select version()')

        row = cursor.fetchone()
        assert row is not None

        provider.server_version = dbapiprovider.get_version_tuple(row[0])
        if provider.server_version >= (5, 6, 4):
            provider.max_time_precision = 6

        cursor.execute('select database()')

        provider.default_schema_name = cursor.fetchone()[0]

    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction

        db_session = cache.db_session
        if db_session is not None and db_session.ddl:
            cursor = connection.cursor()
            cursor.execute("SHOW VARIABLES LIKE 'foreign_key_checks'")
            fk = cursor.fetchone()
            if fk is not None: fk = (fk[1] == 'ON')
            if fk:
                sql = 'SET foreign_key_checks = 0'
                if core.debug:
                    core.log_orm(sql)
                cursor.execute(sql)
            cache.saved_fk_state = bool(fk)
            cache.in_transaction = True

        cache.immediate = True

        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
            if core.debug:
                core.log_orm(sql)
            cursor.execute(sql)
            cache.in_transaction = True

    def release(provider, connection, cache=None):
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and \
                    cache.saved_fk_state:
                try:
                    cursor = connection.cursor()
                    sql = 'SET foreign_key_checks = 1'

                    if core.debug:
                        core.log_orm(sql)

                    cursor.execute(sql)
                except:
                    provider.pool.drop(connection)
                    raise

        dbapiprovider.DBAPIProvider.release(provider, connection, cache)


    def table_exists(provider, connection, table_name):
        db_name, table_name = provider.split_table_name(table_name)

        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM information_schema.tables '
                       'WHERE table_schema=%s and table_name=%s',
                       [ db_name, table_name ])

        return cursor.fetchone() is not None

    def index_exists(provider, connection, table_name, index_name):
        db_name, table_name = provider.split_table_name(table_name)

        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM information_schema.statistics '
                       'WHERE table_schema=%s and table_name=%s and '
                       'index_name=%s',
                       [ db_name, table_name, index_name ])

        return cursor.fetchone() is not None

    def fk_exists(provider, connection, table_name, fk_name):
        db_name, table_name = provider.split_table_name(table_name)

        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM information_schema.table_constraints '
                       'WHERE table_schema=%s and table_name=%s '
                       "and constraint_type='FOREIGN KEY' and "
                       "constraint_name=%s",
                       [ db_name, table_name, fk_name ])

        return cursor.fetchone() is not None
