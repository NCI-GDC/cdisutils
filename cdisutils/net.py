"""General utility belt."""

import functools
import os


class ContextDecorator:
    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwds):
            with self:
                return f(*args, **kwds)

        return decorated


class no_proxy(ContextDecorator):
    def __enter__(self):
        PROXY_ENV_VARS = ["http_proxy", "https_proxy"]
        self.temp_env = {}
        for key in PROXY_ENV_VARS:
            if os.environ.get(key):
                self.temp_env[key] = os.environ.pop(key)

    def __exit__(self, exc_type, exc_value, traceback):
        for key, val in self.temp_env.items():
            os.environ[key] = val
