from __future__ import absolute_import
from unittest import TestCase
from lando_messaging.clients import LandoWorkerClient, JobCommands
from mock import MagicMock, patch


class FakeJobDetails(object):
    """
    Can't mock this due to serialization
    """
    def __init__(self, id, name):
        self.id = id
        self.name = name


class TestLandoWorkerClient(TestCase):
    @patch('lando_messaging.clients.WorkQueueClient')
    def test_stage_job(self, mock_work_queue_client):
        lando_worker_client = LandoWorkerClient(MagicMock(), queue_name='lando')
        job_details = FakeJobDetails(124, "FlyRNASeq2")
        input_files = MagicMock()
        lando_worker_client.stage_job(credentials='', job_details=job_details,
                                             input_files=input_files, vm_instance_name='vm2')
        args, kwargs = mock_work_queue_client().send.call_args
        command = args[0]
        job_run_payload = args[1]
        self.assertEqual(JobCommands.STAGE_JOB, command)
        self.assertEqual(124, job_run_payload.job_id)
        self.assertEqual("FlyRNASeq2", job_run_payload.job_details.name)

    @patch('lando_messaging.clients.WorkQueueClient')
    def test_run_job(self, mock_work_queue_client):
        lando_worker_client = LandoWorkerClient(MagicMock(), queue_name='lando')
        job_details = FakeJobDetails(123, "FlyRNASeq")
        workflow = MagicMock(url='', object_name='', job_order='')
        lando_worker_client.run_job(job_details, workflow, vm_instance_name='vm1')
        args, kwargs = mock_work_queue_client().send.call_args
        command = args[0]
        job_run_payload = args[1]
        self.assertEqual(JobCommands.RUN_JOB, command)
        self.assertEqual(123, job_run_payload.job_id)
        self.assertEqual("FlyRNASeq", job_run_payload.job_details.name)

    @patch('lando_messaging.clients.WorkQueueClient')
    def test_store_job_output(self, mock_work_queue_client):
        lando_worker_client = LandoWorkerClient(MagicMock(), queue_name='lando')
        job_details = FakeJobDetails(125, "FlyRNASeq3")
        lando_worker_client.store_job_output(credentials='', job_details=job_details,
                                             vm_instance_name='vm2')
        args, kwargs = mock_work_queue_client().send.call_args
        command = args[0]
        job_run_payload = args[1]
        self.assertEqual(JobCommands.STORE_JOB_OUTPUT, command)
        self.assertEqual(125, job_run_payload.job_id)
        self.assertEqual("FlyRNASeq3", job_run_payload.job_details.name)

