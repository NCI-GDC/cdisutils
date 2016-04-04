#!/usr/bin/env python -*- coding: utf-8 -*-
"""
server_tail
----------------------------------

Tail the same file from multiple servers at once
"""

from sshtail import SSHMultiTailer
from termcolor import colored

import argparse
import json
import os
import paramiko
import getpass
import textwrap


def prepend_home_dir(filename):
    """
    Prepends the home directory to the given filename if it doesn't
    already contain some kind of directory path.

    Pulled and modified from python-sshtail - jsm
    """
    return os.path.expanduser(
        os.path.join(os.environ['HOME'], '.ssh', filename)
        if not filename.startswith('/')
        and not filename.startswith('./')
        and not filename.startswith('..')
        and not filename.startswith('~/')
        else filename
    )


def add_parser_args(parser):
    parser.add_argument("servers", nargs='+')
    parser.add_argument('-f', "--file",
                        action='append', default=[], dest='files')
    parser.add_argument("-u", "--user",
                        default='ubuntu')
    parser.add_argument("-i", "--identity", required=True)
    parser.add_argument('-w', '--width', type=int, default=128)
    parser.add_argument('-d', '--delay', type=float, default=0.5)
    return parser


def create_tailer(args):
    # Fill out identity path
    key = prepend_home_dir(args.identity)

    # Create product of {servers} x {files}
    setup = {
        '{}@{}'.format(args.user, server): args.files
        for server in args.servers
    }

    # Tell the user the setup
    print('Using key: {}'.format(key))
    print(json.dumps(setup, indent=2))

    # Read the key and decrypt if necessary
    try:
        key = paramiko.RSAKey.from_private_key_file(key)
    except paramiko.ssh_exception.PasswordRequiredException as e:
        print('Doh! ' + str(e))
        key_pass = getpass.getpass('password for {}: '.format(args.identity))
        key = paramiko.RSAKey.from_private_key_file(key, password=key_pass)

    # Create tailer
    tailer = SSHMultiTailer(
        setup,
        poll_interval=args.delay,
        private_key=key,
        verbose=True,
    )

    return tailer


def main(parser=None):
    parser = parser or add_parser_args(argparse.ArgumentParser())
    args = parser.parse_args()
    tailer = create_tailer(args)

    # Poll ssh connections for logs!
    for host, filename, line in tailer.tail():
        line = textwrap.fill(line, width=args.width, subsequent_indent='\t')
        host = colored(host, 'green')
        filename = colored(filename, 'blue', attrs=['bold'])

        header = '{:30s}  {}'.format(host, filename)

        print('\n' + header + '\n\t' + line)


if __name__ == '__main__':
    main()
