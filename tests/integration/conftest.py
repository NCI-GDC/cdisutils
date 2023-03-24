import logging
import signal
import subprocess
import uuid
from threading import Thread

import pytest

logger = logging.getLogger(__name__)


class MotoServer(Thread):
    """
    An AWS mock server. Since we are not going to use boto directly
    having a mock server seems more appropriate for testing. BotoManager
    would then make a connection to this service instead of using moto for
    intercepting requests to AWS. The server is threaded to allow it run
    without interfering with other aspects of the code
    """

    def __init__(self, port=7000, is_secure=True):
        Thread.__init__(self)
        self._proc = None
        self.port = port
        self.hostname = "localhost"
        self.server_id = str(uuid.uuid4())
        self.is_secure = is_secure

    @property
    def url(self) -> str:
        return f"{self.hostname}:{self.port}"

    @property
    def https_url(self) -> str:
        return f"https://{self.url}"

    @property
    def s3_url(self) -> str:
        return f"s3://{self.url}"

    @property
    def client_config(self):
        return dict(
            aws_secret_access_key="testing",  # pragma: allowlist secret
            aws_access_key_id="testing",
            verify=False,
            endpoint_url=self.https_url,
            use_ssl=True,
            service_name="s3",
        )

    def run(self):
        """
        Called automatically by Thread.start().
        Could not find a way to programmatically start the mock_server
        so resorted to using command line
        :return:
        """
        logger.info("Starting a Moto Service id: " + self.server_id)
        if self.is_secure:
            cmd = ["moto_server", "s3", "-s", "-p", str(self.port)]
        else:
            cmd = ["moto_server", "s3", "-p", str(self.port)]
        self._proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )

    def stop(self):

        if not self._proc:
            return
        try:
            sig = signal.SIGKILL
        except Exception as e:
            logger.error(msg=f"Error: {e}", exc_info=True)
            sig = signal.SIGTERM

        try:
            self._proc.send_signal(sig)
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            self._proc.terminate()
        logger.info("Shutdown successful for mock Boto Service id: " + self.server_id)


@pytest.fixture(scope="module")
def moto_server():

    mock = MotoServer()
    mock.start()

    yield mock

    mock.stop()


@pytest.fixture(scope="module")
def moto_server_no_ssl():

    mock = MotoServer(is_secure=False)
    mock.start()

    yield mock

    mock.stop()
