from setuptools import setup

setup(name='lando_messaging',
      version='0.4',
      description='Lando workflow messaging component',
      url='https://github.com/Duke-GCB/lando-messaging',
      author='John Bradley',
      author_email='john.bradley@duke.edu',
      license='MIT',
      packages=['lando_messaging'],
      install_requires=[
         'pika==0.10.0',
      ],
     )
