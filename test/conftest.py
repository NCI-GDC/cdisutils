# -*- coding: utf-8 -*-
"""
py.test test configuration
"""

from tempfile import NamedTemporaryFile

import pytest

TEST_SETTINGS = r"""
namespace1:
  var1: 3
  var2: test

namespace2:
  var1: false
  var4: maybe
"""


@pytest.yield_fixture
def temp_settings(settings=TEST_SETTINGS):
    with NamedTemporaryFile() as f:
        f.write(settings)
        f.flush()
        f.seek(0)
        yield f.name
