import sqlite3
import requests
import os
from multiprocessing import Process
from indexd import get_app as get_indexd_app
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver


class IndexdTestHelper:
    """
    Used in unit tests to initialize dummy indexd database and
    run indexd service
    """
    OLD_SQLITE = sqlite3.sqlite_version_info < (3, 7, 16)
    INDEX_HOST = 'index.sq3'
    ALIAS_HOST = 'alias.sq3'
    AUTH_HOST = 'auth.sq3'

    INDEX_TABLES = {
        'index_record': [
            (0, u'did', u'VARCHAR', 1, None, 1),
            (1, u'rev', u'VARCHAR', 0, None, 0),
            (2, u'form', u'VARCHAR', 0, None, 0),
            (3, u'size', u'INTEGER', 0, None, 0),
        ],
        'index_record_hash': [
            (0, u'did', u'VARCHAR', 1, None, 1),
            (1, u'hash_type', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
            (2, u'hash_value', u'VARCHAR', 0, None, 0),
        ],
        'index_record_url': [
            (0, u'did', u'VARCHAR', 1, None, 1),
            (1, u'url', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
        ],
    }

    # pulled from indexd/tests/test_setup.py
    ALIAS_TABLES = {
        'alias_record': [
            (0, u'name', u'VARCHAR', 1, None, 1),
            (1, u'rev', u'VARCHAR', 0, None, 0),
            (2, u'size', u'INTEGER', 0, None, 0),
            (3, u'release', u'VARCHAR', 0, None, 0),
            (4, u'metastring', u'VARCHAR', 0, None, 0),
            (5, u'keeper_authority', u'VARCHAR', 0, None, 0),
        ],
        'alias_record_hash': [
            (0, u'name', u'VARCHAR', 1, None, 1),
            (1, u'hash_type', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
            (2, u'hash_value', u'VARCHAR', 0, None, 0)
        ],
        'alias_record_host_authority': [
            (0, u'name', u'VARCHAR', 1, None, 1),
            (1, u'host', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
        ],
    }

    INDEX_CONFIG = {
        'driver': SQLAlchemyIndexDriver('sqlite:///' + INDEX_HOST),
    }

    ALIAS_CONFIG = {
        'driver': SQLAlchemyAliasDriver('sqlite:///' + ALIAS_HOST),
    }

    @classmethod
    def run_indexd(cls, port=8000, user='fake_username', pwd='fake_password'):
        """
        Seamlessly runs dummy indexd service for unit tests
        """
        # make sure there are no old indexd .sq3 files left
        cls.remove_sqlite_files()

        # init indexd client and database
        indexd_app = cls.get_indexd_app()
        cls.init_indexd_database(user, pwd)

        # start indexd process and wait till it is up
        indexd = Process(target=indexd_app.run, args=['localhost', port])
        indexd.start()
        cls.wait_for_indexd_alive(port)

        return indexd

    @classmethod
    def setup_sqlite3_index_tables(cls):
        """Setup the SQLite3 index database."""

        SQLAlchemyIndexDriver('sqlite:///' + cls.INDEX_HOST)

        with sqlite3.connect(cls.INDEX_HOST) as conn:
            connection = conn.execute('''
                SELECT name FROM sqlite_master WHERE type = 'table'
            ''')

            tables = [i[0] for i in connection]

            for table in cls.INDEX_TABLES:
                assert table in tables, '{table} not created'.format(table=table)

            for table, _ in cls.INDEX_TABLES.items():
                # NOTE PRAGMA's don't work with parameters...
                connection = conn.execute('''
                    PRAGMA table_info ('{table}')
                '''.format(table=table))

    @classmethod
    def setup_sqlite3_alias_tables(cls):
        """Setup the SQLite3 alias database."""

        SQLAlchemyAliasDriver('sqlite:///' + cls.ALIAS_HOST)

        with sqlite3.connect(cls.ALIAS_HOST) as conn:
            connection = conn.execute('''
                SELECT name FROM sqlite_master WHERE type = 'table'
            ''')

            tables = [i[0] for i in connection]

            for table in cls.ALIAS_TABLES:
                assert table in tables, '{} not created'.format(table)

            for table, _ in cls.ALIAS_TABLES.items():
                # NOTE PRAGMA's don't work with parameters...
                connection = conn.execute('''
                    PRAGMA table_info ('{table}')
                '''.format(table=table))

    @classmethod
    def setup_sqlite3_auth_tables(cls, username, password):
        """Setup the SQLite3 auth database."""
        auth_driver = SQLAlchemyAuthDriver('sqlite:///' + cls.AUTH_HOST)
        try:
            auth_driver.add(username, password)
        except Exception as error:
            print('Unable to create auth tables')
            print(error)

    @classmethod
    def init_indexd_database(cls, username, password):
        cls.setup_sqlite3_index_tables()
        cls.setup_sqlite3_alias_tables()
        cls.setup_sqlite3_auth_tables(username, password)

    @staticmethod
    def get_indexd_app(host='localhost', version='v0', auth=('fake_username', 'fake_password')):
        return get_indexd_app()

    @staticmethod
    def remove_sqlite_files():
        for filename in ['auth.sq3', 'index.sq3', 'alias.sq3']:
            if os.path.exists(filename):
                os.remove(filename)

    @classmethod
    def wait_for_indexd_not_alive(cls, port):
        url = 'http://localhost:{}/_status'.format(port)
        try:
            requests.get(url)
        except requests.ConnectionError:
            return
        else:
            return cls.wait_for_indexd_not_alive(port)

    @classmethod
    def wait_for_indexd_alive(cls, port):
        url = 'http://localhost:{}/_status'.format(port)
        try:
            requests.get(url)
        except requests.ConnectionError:
            return cls.wait_for_indexd_alive(port)
        else:
            return
