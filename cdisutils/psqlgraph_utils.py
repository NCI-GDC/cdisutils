# -*- coding: utf-8 -*-
"""
cdisutils.psqlgraph_utils
----------------------------------

Holds commonly used PsqlGraph utilities
"""


def add_db_conn_args(parser):
    """Adds args to :param:`parser` for db connection

    :param parser: :class:`argparse.ArgumentParser`

    """

    parser.add_argument(
        '-D',
        '--database',
        type=str,
        default='test',
        help='The name of the PostgreSQL database to connect to')
    parser.add_argument(
        '-H',
        '--host',
        type=str,
        default='localhost',
        help='The host of the PostgreSQL server')
    parser.add_argument(
        '-P',
        '--password',
        type=str,
        default=None,
        help='The password for given user (-u). Prompt if not provided')
    parser.add_argument(
        '-U',
        '--user',
        type=str,
        default='test',
        help='The user with which to connect to PostgreSQL')

    return parser


def extract_db_conn_args(args):
    """Returns subset of ``args`` of args used for db connection

    :param args: either a `dict` or a namespace (e.g. argparse.Namespace)

    """

    args = vars(args)
    return dict(
        database=args.get('database'),
        host=args.get('host'),
        password=args.get('password'),
        user=args.get('user'),
    )
