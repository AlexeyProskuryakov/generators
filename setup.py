from pip.req import parse_requirements
from setuptools import setup

reqs = map(lambda x: str(x.req), parse_requirements("requirements.txt", session=False))

setup(name='generators',
      version='0.1',
      description='Generating content',
      author='alexeyka',
      author_email='stack2008@gmail.com',
      url='http://www.python.org/sigs/distutils-sig/',
      install_requires=reqs
      )
