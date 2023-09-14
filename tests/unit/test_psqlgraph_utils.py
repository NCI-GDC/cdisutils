#!/usr/bin/env python

import argparse

from cdisutils.psqlgraph_utils import add_db_conn_args, extract_db_conn_args


def test_add_db_conn_args_1():
    parser = add_db_conn_args(argparse.ArgumentParser())

    assert vars(
        parser.parse_args(
            [
                "--pg-database",
                "database",
                "--pg-host",
                "host",
                "--pg-password",
                "password",
                "--pg-user",
                "user",
            ]
        )
    ) == dict(
        pg_database="database",
        pg_host="host",
        pg_password="password",
        pg_user="user",
    )


def test_add_db_conn_args_2():
    parser = add_db_conn_args(argparse.ArgumentParser())

    assert vars(
        parser.parse_args(
            [
                "-PD",
                "database",
                "-PH",
                "host",
                "-PP",
                "password",
                "-PU",
                "user",
            ]
        )
    ) == dict(
        pg_database="database",
        pg_host="host",
        pg_password="password",
        pg_user="user",
    )


def test_extract_db_conn_args_1():
    parser = add_db_conn_args(argparse.ArgumentParser())

    assert extract_db_conn_args(
        parser.parse_args(
            [
                "-PD",
                "database",
                "-PH",
                "host",
                "-PP",
                "password",
                "-PU",
                "user",
            ]
        )
    ) == dict(
        database="database",
        host="host",
        password="password",
        user="user",
    )
