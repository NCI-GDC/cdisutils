# cdisutils

Various utilities useful for working on cdis systems.

A few modules:

## `cdisutils.net`

Networking utilities.

### `no_proxy`

function that can be used as a decorator or a context manager to
temporarily disable the pdc http_proxy

## `cdisutils.storage`

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

Returns an basic stdlib `Logger` object that logs to stdout with a
reasonable format string, set to level INFO.

## `cdisutils.tungsten`

Utilites for working with tungsten provisioned machines

## `cdisutils.settings`
