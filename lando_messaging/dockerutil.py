"""
Utility only used to perform integration tests against docker images.
"""
import os
import time
from docker import Client

DOCKER_BASE_URL = 'unix://var/run/docker.sock'
DOCKER_VERSION = '1.21'


class DockerContainer(object):
    """
    Allows running/terminating a docker image.
    """
    def __init__(self, image_name, environment, ports):
        """
        Setup docker container info.
        :param image_name: str: name of the image to run
        :param environment: dict: environment variables to create in the container
        :param ports: [int]: list of ports to map 1:1 to the host
        """
        self.cli = Client(base_url=DOCKER_BASE_URL, version=DOCKER_VERSION)
        self.image_name = image_name
        self.container_id = None
        self.environment = environment
        self.ports = ports

    def run(self):
        """
        Start running the container.
        """
        # Pull if not local
        images = self.cli.images(self.image_name)
        if len(images) == 0:
            self.cli.pull(self.image_name)
        # Run
        port_bindings = {}
        for port in self.ports:
            port_bindings[port] = ('0.0.0.0', port)
        container = self.cli.create_container(self.image_name,
                                              environment=self.environment,
                                              ports=self.ports,
                                              host_config=self.cli.create_host_config(port_bindings=port_bindings))
        self.container_id = container.get('Id')
        result = self.cli.start(container=self.container_id)

    def destroy(self):
        """
        Stop and delete the container.
        """
        if self.container_id:
            self.cli.stop(container=self.container_id)
            # Do not remove containers when testing circle: https://circleci.com/docs/docker-btrfs-error/
            if os.environ.get('CIRCLECI') != 'true':
                self.cli.remove_container(container=self.container_id)
            self.container_id = None


class DockerRabbitmq(object):
    """
    Rabbitmq container with a default user.
    """
    IMAGE = 'rabbitmq:latest'
    HOST = "127.0.0.1"
    USER = "joe"
    PASSWORD = "secret"

    def __init__(self):
        environment = {
            "RABBITMQ_NODENAME": "my-rabbit",
            "RABBITMQ_DEFAULT_USER": DockerRabbitmq.USER,
            "RABBITMQ_DEFAULT_PASS": DockerRabbitmq.PASSWORD,
        }
        ports = [5672, 15672]
        self.container = DockerContainer(image_name=DockerRabbitmq.IMAGE, environment=environment, ports=ports)
        self.container.run()
        time.sleep(6)

    def destroy(self):
        self.container.destroy()
