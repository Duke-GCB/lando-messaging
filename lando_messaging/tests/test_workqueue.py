from __future__ import absolute_import
from unittest import TestCase, skip
import pickle
from lando_messaging.dockerutil import DockerRabbitmq
from lando_messaging.workqueue import WorkQueueConnection, WorkQueueProcessor, WorkQueueClient, WorkProgressQueue, \
    WorkRequest, get_version_str
from mock import MagicMock, patch


class FakeConfig(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.work_queue_config = self


@skip('skipping until we can rework into circleci compatible tests')
class TestWorkProgressQueue(TestCase):
    @classmethod
    @skip('skipping until we can rework into circleci compatible tests')
    def setUpClass(cls):
        cls.rabbit_vm = DockerRabbitmq()
        cls.config = FakeConfig(DockerRabbitmq.HOST, DockerRabbitmq.USER, DockerRabbitmq.PASSWORD)

    @classmethod
    @skip('skipping until we can rework into circleci compatible tests')
    def tearDownClass(cls):
        cls.rabbit_vm.destroy()

    def test_work_progress_queue_can_send_json_message(self):
        wpq = WorkProgressQueue(self.config, "job_status")
        result = wpq.send('{"job":16, "status":"GOOD"}')
        self.assertEqual(True, result)


class TestGetVersionStr(TestCase):
    def test_version(self):
        version_str = get_version_str()
        self.assertEqual(type(version_str), str)
        parts = version_str.split(".")
        self.assertRegexpMatches(version_str, '^\d+.\d+.\d+$')


class TestWorkQueueProcessor(TestCase):
    def setUp(self):
        self.payload = None

    def do_work(self, payload):
        self.payload = payload

    def test_default_mismatch_version_raises(self):
        processor = WorkQueueProcessor(MagicMock(), 'test')
        processor.add_command('dowork', self.do_work)
        request = WorkRequest('dowork', 'somedata')
        request.version = '0.0.0'
        with self.assertRaises(ValueError) as err_context:
            processor.process_message(MagicMock(), MagicMock(), None, pickle.dumps(request))
        self.assertEqual(str(err_context.exception), 'Received version mismatch.')

    def upgrade_payload(self, work_request, our_version):
        work_request.payload = 'evenbetter'
        work_request.version = our_version

    def test_default_mismatch_version_with_override(self):
        # We pretend to upgrade the request payload
        processor = WorkQueueProcessor(MagicMock(), 'test', self.upgrade_payload)
        processor.add_command('dowork', self.do_work)
        request = WorkRequest('dowork', 'somedata')
        request.version = '0.0.0'
        processor.process_message(MagicMock(), MagicMock(), None, pickle.dumps(request))
        self.assertEqual('evenbetter', self.payload)
