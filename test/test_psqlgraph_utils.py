#!/usr/bin/env python

import argparse


from cdisutils.psqlgraph_utils import (
    extract_db_conn_args,
    add_db_conn_args,
)


def test_add_db_conn_args_1():
    parser = add_db_conn_args(argparse.ArgumentParser())

    assert vars(parser.parse_args([
        '--database', 'database',
        '--host', 'host',
        '--password', 'password',
        '--user', 'user',
    ])) == dict(
        database='database',
        host='host',
        password='password',
        user='user',
    )


def test_add_db_conn_args_2():
    parser = add_db_conn_args(argparse.ArgumentParser())

    assert vars(parser.parse_args([
        '-D', 'database',
        '-H', 'host',
        '-P', 'password',
        '-U', 'user',
    ])) == dict(
        database='database',
        host='host',
        password='password',
        user='user',
    )


def test_extract_db_conn_args_1():
    parser = add_db_conn_args(argparse.ArgumentParser())

    assert extract_db_conn_args(parser.parse_args([
        '-D', 'database',
        '-H', 'host',
        '-P', 'password',
        '-U', 'user',
    ])) == dict(
        database='database',
        host='host',
        password='password',
        user='user',
    )
