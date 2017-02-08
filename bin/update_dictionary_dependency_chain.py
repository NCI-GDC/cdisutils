#!/usr/bin/env python

"""
Description: This script is for updating the dictionary and
datamodel dependency hashes throughout the GDC codebase.

Usage: There are two steps

1. Update the datamodel with the new dictionary commit

2. Update the rest of the repos with both the datamodel and dictionary
   commits

First step

```bash
python update_dictionary_dependency_chain.py \ #
    datamodel                                \ # only update datamodel
    chore/bump-deps                          \ # push on this branch
    dictionary_commit                        \ # change to this dictionary commit
```

Second step

```bash
python update_dictionary_dependency_chain.py \ #
    downstream                               \ # don't update datamodel
    chore/bump-deps                          \ # push on this branch
    dictionary_commit                        \ # change to this dictionary commit
    --datemodel datamodel_commit               # change to this datamodel commit
```

Note: you can set the OPEN_CMD environment variable to a browser to
open remote urls in.
"""


from subprocess import check_call, call, PIPE, Popen
from contextlib import contextmanager

import argparse
import re
import os
import shutil
import tempfile


OPEN_CMD = os.environ.get("OPEN_CMD", 'open')
DEP_PIN_PATTERN = ("git\+(https|ssh)://(git@)?github\.com/NCI-GDC/"
                   "{repo}\.git@([0-9a-f]{{40}})#egg={repo}")

DEPENDENCY_MAP = {
    'gdcdatamodel': ['setup.py'],
    'gdcapi': ['requirements.txt'],
    'zugs': ['setup.py'],
    'esbuild': ['requirements.txt'],
}


REPO_MAP = {
    'gdcdatamodel': 'git@github.com:NCI-GDC/gdcdatamodel.git',
    'gdcapi': 'git@github.com:NCI-GDC/gdcapi.git',
    'zugs': 'git@github.com:NCI-GDC/zugs.git',
    'esbuild': 'git@github.com:NCI-GDC/esbuild.git',
}


@contextmanager
def within_dir(path):
    original_path = os.getcwd()
    try:
        print "Entering directory %s" % path
        os.chdir(path)
        yield
    finally:
        os.chdir(original_path)
        print "Exiting directory %s" % path


@contextmanager
def within_tempdir():
    original_path = os.getcwd()
    try:
        dirpath = tempfile.mkdtemp()
        print "Working in %s" % dirpath
        os.chdir(dirpath)
        yield dirpath
    finally:
        print "Cleaning up temp files in %s" % dirpath
        shutil.rmtree(dirpath)
        os.chdir(original_path)


def replace_dep_in_file(path, pattern, repl):
    with open(path, 'r') as original:
        data = original.read()

    matches = re.findall(pattern, data)

    for match in matches:
        _, _, commit = match
        print '\n\n\tREPLACING: %s: %s -> %s\n\n' % (path, commit, repl)
        data = re.sub(commit, repl, data)

    with open(path, 'w') as updated:
        updated.write(data)


def checkout_fresh_branch(repo, name):
    cwd = os.getcwd()
    try:
        print "Checking out new branch %s in %s" % (name, repo)
        os.chdir(repo)

        check_call(['git', 'fetch', 'origin'])
        check_call(['git', 'checkout', 'origin/develop'])
        check_call(['git', 'checkout', '-B', name])
    finally:
        os.chdir(cwd)


def open_repo_url():
    proc = Popen(['git', 'config', '--get', 'remote.origin.url'] ,stdout=PIPE)
    url = proc.stdout.read().replace('git@github.com:', 'https://github.com/')
    print "Opening remote url %s" % url
    call([OPEN_CMD, url])


def bump_datamodel(branch, to_dictionary_hash):
    pattern = DEP_PIN_PATTERN.format(repo='gdcdictionary')
    repo = 'gdcdatamodel'
    url = REPO_MAP[repo]
    check_call(['git', 'clone', url])
    checkout_fresh_branch(repo, branch)

    with within_dir(repo):

        for path in DEPENDENCY_MAP[repo]:
            replace_dep_in_file(path, pattern, to_dictionary_hash)

        message = 'updating dictionary commit to %s' % to_dictionary_hash
        check_call(['git', 'commit', '-am', message])

        print "Pushing datamodel origin/%s" % branch
        check_call(['git', 'push', 'origin', branch])

        open_repo_url()


def bump_downstream(branch, to_dictionary_hash, to_datamodel_hash):
    dictionary_pattern = DEP_PIN_PATTERN.format(repo='gdcdictionary')
    datamodel_pattern = DEP_PIN_PATTERN.format(repo='gdcdatamodel')

    for repo, url in REPO_MAP.iteritems():
        if repo == 'gdcdatamodel':
            continue  # should be done via bump_datamodel

        check_call(['git', 'clone', url])
        checkout_fresh_branch(repo, branch)

        with within_dir(repo):

            for path in DEPENDENCY_MAP[repo]:
                replace_dep_in_file(
                    path,
                    dictionary_pattern,
                    to_dictionary_hash)

                replace_dep_in_file(
                    path,
                    datamodel_pattern,
                    to_datamodel_hash)

            message = 'updating dictionary commit to %s' % to_dictionary_hash
            check_call(['git', 'commit', '-am', message])

            print "Pushing datamodel origin/%s" % branch
            check_call(['git', 'push', 'origin', branch])

            open_repo_url()

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('target', help='just the datamodel or all downstream',
                        choices=['datamodel', 'downstream'])
    parser.add_argument('branch', help='branch to push bump as')
    parser.add_argument('dictionary_commit', help='commit of dictionary')
    parser.add_argument('--datamodel', required=False,
                        help='commit of dictionary')

    args = parser.parse_args()

    with within_tempdir():
        if args.target == 'datamodel':
            bump_datamodel(args.branch, args.dictionary_commit)

        else:
            assert args.datamodel, (
                "When run with target=%s, argument `datamodel_commit` "
                "is required") % args.target

            bump_downstream(
                args.branch, args.dictionary_commit, args.datamodel)

if __name__ == '__main__':
    main()
