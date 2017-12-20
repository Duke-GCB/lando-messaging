"""
Utility only used to perform integration tests against docker images.
"""
import os
import time
import docker

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
        self.cli = docker.from_env()
        self.image_name = image_name
        self.container_id = None
        self.environment = environment
        self.ports = ports

    def run(self):
        """
        Start running the container.
        """
        # Run
        port_bindings = {}
        for port in self.ports:
            port_bindings[port] = ('0.0.0.0', port)
        self.container = self.cli.containers.run(self.image_name,
                                                 environment=self.environment,
                                                 ports=self.ports,
                                                 detach=True)

    def destroy(self):
        """
        Stop and delete the container.
        """
        if self.container:
            self.container.kill()


class DockerRabbitmq(object):
    """
    Rabbitmq container with a default user.
    """
    IMAGE = 'rabbitmq:3.6.14'
    HOST = "0.0.0.0"
    USER = "joe"
    PASSWORD = "secret"

    def __init__(self):
        environment = {
            "RABBITMQ_NODENAME": "my-rabbit",
            "RABBITMQ_DEFAULT_USER": DockerRabbitmq.USER,
            "RABBITMQ_DEFAULT_PASS": DockerRabbitmq.PASSWORD,
        }
        ports = { '5672/tcp': 5672, '15672/tcp': 15672}
        self.container = DockerContainer(image_name=DockerRabbitmq.IMAGE, environment=environment, ports=ports)
        self.container.run()
        time.sleep(6)

    def destroy(self):
        self.container.destroy()
