#!/usr/bin/env python

"""
Description: This script is for updating the dictionary and
datamodel dependency hashes throughout the GDC codebase.

Usage: There are 4.5 steps

0. Update the gdcdictionary, or rather, find the commit in which you want all
   the dependent services to be pulling from

1. Update the datamodel with the new dictionary commit (flag datamodel) and
   commit SHA from step 0

1.5 Should you want to base the dictionary/datamodel updates of a branch that is
   not origin/develop (or orgin/master for authorization). Go ahead and modify the
   BASE_BRANCH_MAP within this file. This will put the pin upates onto the tip of
   whatever branch you require.

2. Update the rest of the repos with both the datamodel and dictionary
   commits (flag downstream). Also the commit created from Step 1, gets used here.

3. Start opening PRs for the newly pushed branches against the branches you wanted
   in step 1.5. Go fight travis.

First(1) step

```bash
python update_dictionary_dependency_chain.py \ #
    --target datamodel                             \ # only update datamodel
    --branch chore/bump-deps                       \ # push on this branch
    --dictionary_commit SHA1                         # change to this dictionary commit
```

Second(2) step

```bash
python update_dictionary_dependency_chain.py \ #
    --target downstream                            \ # don't update datamodel
    --branch chore/bump-deps                       \ # push on this branch
    --dictionary_commit SHA1                       \ # change to this dictionary commit
    --datamodel_commit  SHA1                         # change to this datamodel commit
```

Note: you can set the OPEN_CMD environment variable to a browser to
open remote urls in. On a mac this just works, don't mess with setting the OPEN_CMD

Here's some oneliners an example:
python update_dictionary_dependency_chain.py --target datamodel --branch jbarno/test_pin_updater --dictionary_commit TESTING_A_SCRIPT
python update_dictionary_dependency_chain.py --target downstream --branch jbarno/test_pin_updater --dictionary_commit TESTING_A_SCRIPT --datamodel_commit TESTING_SECOND_SCRIPT
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
    'runners': ['setup.py'],
    'auto-qa': ['requirements.txt'],
    'authorization': ['auth_server/requirements.txt'],
    'legacy-import': ['setup.py']
}


REPO_MAP = {
    'gdcdatamodel': 'git@github.com:NCI-GDC/gdcdatamodel.git',
    'gdcapi': 'git@github.com:NCI-GDC/gdcapi.git',
    'zugs': 'git@github.com:NCI-GDC/zugs.git',
    'esbuild': 'git@github.com:NCI-GDC/esbuild.git',
    'runners': 'git@github.com:NCI-GDC/runners.git',
    'auto-qa': 'git@github.com:NCI-GDC/auto-qa.git',
    'authorization': 'git@github.com:NCI-GDC/authorization.git',
    'legacy-import': 'git@github.com:NCI-GDC/legacy-import.git',
}

# edit this if you want to change each branch manually, otherwise
# just use the command line arg default_base_branch
BASE_BRANCH_MAP = {
    'gdcdatamodel': 'origin/master',
    'gdcapi': 'origin/master',
    'zugs': 'origin/develop',
    'esbuild': 'origin/develop',
    'runners': 'origin/develop',
    'auto-qa': 'origin/develop',
    'authorization': 'origin/develop',
    'legacy-import': 'origin/master',
}

DEFAULT_BASE_BRANCH = 'origin/develop'

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
def within_tempdir(dry_run=False):
    original_path = os.getcwd()
    try:
        dirpath = tempfile.mkdtemp()
        print "Working in %s" % dirpath
        os.chdir(dirpath)
        yield dirpath
    finally:
        if not dry_run:
            print "Cleaning up temp files in %s" % dirpath
            shutil.rmtree(dirpath)
        else:
            print "dry_run, saving temp files in {}".format(dirpath)
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

def get_base_branch(repo):
    if repo in BASE_BRANCH_MAP:
        return BASE_BRANCH_MAP[repo]
    else:
        return DEFAULT_BASE_BRANCH

def checkout_fresh_branch(repo, name, default_base=DEFAULT_BASE_BRANCH):
    cwd = os.getcwd()
    try:
        #base_branch = get_base_branch(repo)
        if default_base != DEFAULT_BASE_BRANCH:
            base_branch = default_base
        else:
            base_branch = BASE_BRANCH_MAP.get(repo, default_base)
        print "Checking out new branch %s based off %s in %s" % (name, base_branch, repo)
        os.chdir(repo)

        check_call(['git', 'fetch', 'origin'])
        check_call(['git', 'checkout', base_branch])
        check_call(['git', 'checkout', '-B', name])
    finally:
        os.chdir(cwd)

def commit_and_push(hash, branch, dry_run=False):
    message = 'updating dictionary commit to %s' % hash
    check_call(['git', 'commit', '-am', message])

    if not dry_run:
        print "Pushing datamodel origin/%s" % branch
        check_call(['git', 'push', 'origin', branch])
    else:
        print "dry_run requested, skipping push"

def open_repo_url(open_repo=False):
    if open_repo:
        proc = Popen(['git', 'config', '--get', 'remote.origin.url'] ,stdout=PIPE)
        url = proc.stdout.read().replace('git@github.com:', 'https://github.com/')
        print "Opening remote url %s" % url
        call([OPEN_CMD, url])


def bump_datamodel(branch,
                   to_dictionary_hash,
                   open_repo=False,
                   dry_run=False,
                   default_base=DEFAULT_BASE_BRANCH):
    pattern = DEP_PIN_PATTERN.format(repo='gdcdictionary')
    repo = 'gdcdatamodel'
    url = REPO_MAP[repo]
    check_call(['git', 'clone', url])
    checkout_fresh_branch(repo, branch, default_base=default_base)

    with within_dir(repo):

        for path in DEPENDENCY_MAP[repo]:
            replace_dep_in_file(path, pattern, to_dictionary_hash)

        commit_and_push(hash=to_dictionary_hash, branch=branch, dry_run=dry_run)
        open_repo_url(open_repo=open_repo)


def bump_downstream(branch,
                    to_dictionary_hash,
                    to_datamodel_hash,
                    open_repo=False,
                    dry_run=False,
                    default_base=DEFAULT_BASE_BRANCH):
    dictionary_pattern = DEP_PIN_PATTERN.format(repo='gdcdictionary')
    datamodel_pattern = DEP_PIN_PATTERN.format(repo='gdcdatamodel')

    for repo, url in REPO_MAP.iteritems():
        if repo == 'gdcdatamodel':
            continue  # should be done via bump_datamodel

        check_call(['git', 'clone', url])
        checkout_fresh_branch(repo, branch, default_base=default_base)

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

            commit_and_push(hash=to_dictionary_hash, branch=branch, dry_run=dry_run)
            open_repo_url(open_repo=open_repo)

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--target', help='just the datamodel or all downstream',
                        choices=['datamodel', 'downstream'])
    parser.add_argument('--branch', help='branch to push bump as')
    parser.add_argument('--dictionary_commit', required=True, help='commit of dictionary')
    parser.add_argument('--datamodel_commit', required=False, help='commit of datamodel')
    parser.add_argument('--open_browser', action='store_true',
        help='open repo in browser after commit')
    parser.add_argument('--default_base_branch',
        help='default base branch to use for target repo, default = {}'.format(DEFAULT_BASE_BRANCH),
        default=DEFAULT_BASE_BRANCH)
    parser.add_argument('--dry_run', action='store_true',
        help='dry run, simply create branches, do not push them')

    args = parser.parse_args()

    with within_tempdir(dry_run=args.dry_run):
        if args.target == 'datamodel':
            bump_datamodel(args.branch,
                           args.dictionary_commit,
                           open_repo=args.open_browser,
                           dry_run=args.dry_run,
                           default_base=args.default_base_branch)

        else:
            assert args.datamodel_commit, (
                "When run with target=%s, argument `datamodel_commit` "
                "is required") % args.target

            bump_downstream(args.branch,
                            args.dictionary_commit,
                            args.datamodel_commit,
                            open_repo=args.open_browser,
                            dry_run=args.dry_run,
                            default_base=args.default_base_branch)

if __name__ == '__main__':
    main()
