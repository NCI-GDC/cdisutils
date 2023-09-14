[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commitlogoColor=white)](https://github.com/pre-commit/pre-commit)

---
# cdisutils

Various utilities useful for working on cdis systems.


- [cdisutils](#cdisutils)
  - [`cdisutils.net`](#cdisutilsnet)
    - [`no_proxy`](#no_proxy)
  - [`cdisutils.storage3`](#cdisutilsstorage3)
    - [`is_probably_swift_segments(obj)`](#is_probably_swift_segmentsobj)
    - [`swift_stream(obj)`](#swift_streamobj)
  - [`cdisutils.log`](#cdisutilslog)
    - [`get_logger(name)`](#get_loggername)
  - [`cdisutils.tungsten`](#cdisutilstungsten)
- [Setup pre-commit hook to check for secrets](#setup-pre-commit-hook-to-check-for-secrets)
- [contributing](#contributing)

---

A few modules:

## `cdisutils.net`

Networking utilities.

### `no_proxy`

function that can be used as a decorator or a context manager to
temporarily disable the pdc http_proxy

## `cdisutils.storage3`

For working with storage via apache libcloud

### `is_probably_swift_segments(obj)`

Takes a libcloud storage Object, tells you if it's probably one of
OpenStack Swift's goofy segment indicator JSON blobs

### `swift_stream(obj)`

Given a libcloud storage object containing one of the aforementioned
JSON blobs, gives you a stream to the data you actually wanted.

## `cdisutils.log`

Simple logging setup.

### `get_logger(name)`

Returns a basic stdlib `Logger` object that logs to stdout with a
reasonable format string, set to level INFO.

## `cdisutils.tungsten`

Utilities for working with tungsten provisioned machines

# Setup pre-commit hook to check for secrets

We use [pre-commit](https://pre-commit.com/) to setup pre-commit hooks for this repo.
We use [detect-secrets](https://github.com/Yelp/detect-secrets) to search for secrets being committed into the repo.

To install the pre-commit hook, run
```
pre-commit install
```

To update the .secrets.baseline file run
```
detect-secrets scan --update .secrets.baseline
```

`.secrets.baseline` contains all the string that were caught by detect-secrets but are not stored in plain text. Audit the baseline to view the secrets .

```
detect-secrets audit .secrets.baseline
```
# contributing

Read how to contribute [here](https://github.com/nci-gdc/gdcapi/blob/master/contributing.md)
