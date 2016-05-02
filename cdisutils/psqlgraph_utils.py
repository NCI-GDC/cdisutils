# -*- coding: utf-8 -*-
"""
cdisutils.psqlgraph_utils
----------------------------------

Holds commonly used PsqlGraph utilities
"""

import os

def add_db_conn_args(parser):
    """Adds args to :param:`parser` for db connection

    :param parser: :class:`argparse.ArgumentParser`

    """

    parser.add_argument(
        '-PD',
        '--pg-database',
        type=str,
        default=os.environ.get('PG_DATABASE','test'),
        help='The name of the PostgreSQL database to connect to')
    parser.add_argument(
        '-PH',
        '--pg-host',
        type=str,
        default=os.environ.get('PG_HOST','localhost'),
        help='The host of the PostgreSQL server')
    parser.add_argument(
        '-PP',
        '--pg-password',
        type=str,
        default=os.environ.get('PG_PASS',None),
        help='The password for given user (-u). Prompt if not provided')
    parser.add_argument(
        '-PU',
        '--pg-user',
        type=str,
        default=os.environ.get('PG_USER','test'),
        help='The user with which to connect to PostgreSQL')

    return parser


def extract_db_conn_args(args):
    """Returns subset of ``args`` of args used for db connection

    :param args: either a `dict` or a namespace (e.g. argparse.Namespace)

    """

    args = vars(args) if not isinstance(args, dict) else args
    return dict(
        database=args.get('pg_database'),
        host=args.get('pg_host'),
        password=args.get('pg_password'),
        user=args.get('pg_user'),
    )
