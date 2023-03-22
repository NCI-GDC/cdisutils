import os

from cdisutils import net


def test_no_proxy_cm():
    os.environ["http_proxy"] = "http://foobar:1234"
    with net.no_proxy():
        assert not os.environ.get("http_proxy")
    assert os.environ["http_proxy"] == "http://foobar:1234"
    del os.environ["http_proxy"]


def test_no_proxy_dec():
    os.environ["http_proxy"] = "http://foobar:1234"

    @net.no_proxy()
    def inner():
        assert not os.environ.get("http_proxy")
    inner()
    assert os.environ["http_proxy"] == "http://foobar:1234"
    del os.environ["http_proxy"]
