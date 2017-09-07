#!/usr/bin/env python
# coding: utf-8
from setuptools import setup
from setuptools import find_packages


def get_packages():
    """ List of (sub)packages to install. """
    return find_packages('.', include=('pika_cli_client', ))


def get_requirements(filename):
    """ Read requirements from file. """
    with open(filename, 'r') as reqfile:
        for req_line in reqfile.readlines():
            req_line = req_line.strip()
            if req_line:
                yield req_line


def get_textfile(filename):
    """ Get contents from a text file. """
    with open(filename, 'rU') as fh:
        return fh.read()


def setup_package():
    """ build and run setup. """

    setup(
        name='pika-cli-client',
        description='Publish or consume messages using AMQP 0.91',
        long_description=get_textfile('README.md'),
        author='fredrikhl',
        url='https://github.com/fredrikhl/pika-cli-client',
        use_scm_version=True,
        setup_requires=['setuptools_scm'],
        install_requires=list(get_requirements('requirements.txt')),
        packages=get_packages(),
        # TODO: Add entry points?
    )


if __name__ == "__main__":
    setup_package()
