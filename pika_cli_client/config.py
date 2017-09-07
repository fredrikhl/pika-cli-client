#!/usr/bin/env python
# encoding: utf-8
""" Utils for setting up a pika client. """
from __future__ import absolute_import

import argparse
import getpass
import logging
import os
import pika
import ssl
import sys
import yaml

from collections import MutableMapping, OrderedDict


SSL_PROTOCOLS = dict((attr.replace('PROTOCOL_', '').replace('_', '.').lower(),
                      getattr(ssl, attr))
                     for attr in dir(ssl) if attr.startswith('PROTOCOL_'))

DEFAULT_CONFIG = {
    'auth.password': None,
    'auth.username': None,
    'conn.host': 'localhost',
    'conn.port': 5672,
    'conn.vhost': '/',
    'ssl.ciphers': [],  # TODO
    'ssl.enable': True,
    'ssl.version': sorted(SSL_PROTOCOLS.keys())[-1],
    'logging.level': 'INFO',
}

DEFAULT_CONF_NAME = 'pika-cli-config.yml'
DEFAULT_CONF_SYS = os.path.join(sys.prefix, 'etc', DEFAULT_CONF_NAME)
DEFAULT_CONF_USR = os.path.join(os.path.expanduser('~'), '.config',
                                DEFAULT_CONF_NAME)


class IniConfig(MutableMapping):

    VALUES = {
        ('auth', 'password'):  (None, str),
        ('auth', 'username'):  (None, str),
        ('conn', 'host'):  ('localhost', str),
        ('conn', 'port'):  (5672, int),
        ('conn', 'vhost'):  ('/', str),
        ('ssl', 'ciphers'):  ('', str),
        ('ssl', 'enable'):  (True, bool),
        ('ssl', 'version'):  (sorted(SSL_PROTOCOLS.keys())[-1], str),
        ('logging', 'level'):  ('INFO', str),
    }

    def __init__(self):
        self.config = None
        for k in self.VALUES:
            section, option = k
            default, type_ = self.VALUES[k]
            if not self.config.has_section(section):
                self.config.add_section(section)
            self.config.set(section, option, default)

    def __getitem__(self, item):
        try:
            section, option = item.split('.', 1)
            if not (section, option) in self.VALUES:
                raise ValueError()
        except ValueError:
            raise KeyError("No setting {}".format(item))

        default, type_ = self.VALUES[(section, option)]

        if type_ is int:
            return self.config.getint(section, option)
        elif type_ is float:
            return self.config.getfloat(section, option)
        elif type_ is bool:
            return self.config.getboolean(section, option)
        else:
            return self.config.get(section, option)

    def __setitem__(self, item, value):
        try:
            section, option = item.split('.', 1)
            if not (section, option) in self.VALUES:
                raise ValueError()
        except ValueError:
            raise KeyError("No setting {}".format(item))

        default, type_ = self.VALUES[(section, option)]
        value = type_(value)
        self.config.set(section, option, value)

    def __delitem__(self, item):
        try:
            section, option = item.split('.', 1)
            if not (section, option) in self.VALUES:
                raise ValueError()
        except ValueError:
            raise KeyError("No setting {}".format(item))
        default, type_ = self.VALUES[(section, option)]
        self.config.set(section, option, default)

    def __iter__(self):
        def gen():
            for section in sorted(self.config.sections()):
                for option in sorted(self.config.options(section)):
                    yield '{}.{}'.format(section, option)
        return iter(gen())

    def __len__(self):
        return len([opt for section in self.config.sections()
                    for opt in self.config.options(section)])

    def __repr__(self):
        return '<IniConfig>'


class Config(MutableMapping):

    def __init__(self, *args, **kwargs):
        self._defaults = DEFAULT_CONFIG
        self._settings = {}
        self.update(dict(*args, **kwargs))

    def __getitem__(self, item):
        if item not in self._defaults:
            raise KeyError("No setting {}".format(item))
        return self._settings.get(item, self._defaults[item])

    def __setitem__(self, item, value):
        if item not in self._defaults:
            raise KeyError("No setting {}".format(item))
        self._settings[item] = value

    def __delitem__(self, item):
        if item not in self._defaults:
            raise KeyError("No setting {}".format(item))
        if item not in self._settings:
            raise KeyError("{} not set".format(item))
        del self._settings[item]

    def __iter__(self):
        return iter(sorted(self._defaults))

    def __len__(self):
        return len(self._defaults)

    def __repr__(self):
        d = dict()
        for k in self._defaults:
            d[k] = self._defaults[k]
        for k in self._settings:
            d[k] = self._settings[k]
        return '{}({})'.format(self.__class__.__name__, repr(d))

    def load_dict(self, dict_value):
        for key in sorted(self._defaults):
            parts = key.split('.')
            t = dict_value
            for p in parts[:-1]:
                t = t.get(p, dict())
            if parts[-1] in t:
                self[key] = t[parts[-1]]

    def dump_dict(self):
        dump = OrderedDict()
        for key in sorted(self._defaults):
            parts = key.split('.')
            t = dump
            for p in parts[:-1]:
                t = t.setdefault(p, OrderedDict())
            t[parts[-1]] = self[key]
        return dump


def make_config_argparser(config, description):
    """ Make an argparser with connection params for pika.

    :param Config config: Configuration with defaults.
    :param str description: Script description.
    """
    parser = argparse.ArgumentParser(description=description)

    # Connection
    conn = parser.add_argument_group('Connection', 'MQ connection settings')

    # conn.*
    conn.add_argument(
        '-H', '--hostname',
        type=str,
        dest='host',
        metavar='HOST',
        default=config['conn.host'],
        help="MQ host (default: %(default)s)")
    conn.add_argument(
        '-P', '--port',
        type=int,
        dest='port',
        metavar='PORT',
        default=config['conn.port'],
        help="MQ port (default: %(default)s)")
    conn.add_argument(
        '-V', '--vhost',
        type=str,
        dest='vhost',
        default=config['conn.vhost'],
        metavar='VHOST',
        help="Vhost (default: %(default)s)")

    # ssl.*
    ssl_enable = conn.add_mutually_exclusive_group()
    ssl_enable.add_argument(
        '-s', '--ssl',
        action='store_true',
        dest='ssl',
        default=config['ssl.enable'],
        help="Use SSL (default: %(default)s)")
    ssl_enable.add_argument(
        '--no-ssl',
        action='store_false',
        dest='ssl',
        default=config['ssl.enable'],
        help="No not use SSL (default: %(default)s)")
    conn.add_argument(
        '--ssl-version',
        dest='ssl_version',
        choices=sorted(SSL_PROTOCOLS.keys()),
        default=config['ssl.version'],
        metavar='VERSION',
        help="Use SSL version %(metavar)s (default: %(default)s)")
    # TODO:
    #   conn.add_argument(
    #       '--ssl-ciphers',
    #       dest='ssl_ciphers')

    # auth.*
    auth = parser.add_argument_group('Authentication',
                                     'MQ authentication settings')
    auth.add_argument(
        '-u', '--username',
        type=str,
        dest='username',
        default=config['auth.username'],
        metavar='USER',
        help='Subscriber username (%(default)s)')
    pw = auth.add_mutually_exclusive_group()
    pw.add_argument(
        '-p', '--password',
        type=str,
        dest='password',
        default=config['auth.password'],
        metavar='PASS',
        help='Subscriber password')
    pw.add_argument(
        '--password-file',
        type=str,
        dest='password_file',
        default=None,
        metavar='FILE',
        help='Read subscriber password from file')

    # logging.*
    logs = parser.add_argument_group('Logging', 'Logging settings')
    logs.add_argument(
        '--log-level',
        dest='log_level',
        default=config['logging.level'],
        metavar='LEVEL',
        help="Set the logging level")

    return parser


def read_config(filename):
    """ Read a yaml file with a config dict. """
    if filename and os.path.isfile(filename):
        with open(filename, 'r') as config:
            settings = yaml.load(config)
            if isinstance(settings, dict):
                return settings
    return dict()


def get_config(ignore_sys=False, ignore_user=False, custom=None):
    """ Get config from default locations. """
    config = Config()
    configs = []
    if not ignore_sys:
        configs.append(DEFAULT_CONF_SYS)
    if not ignore_user:
        configs.append(DEFAULT_CONF_USR)
    if custom is not None:
        configs.append(os.path.expanduser(custom))

    for path in configs:
        if os.path.isfile(path):
            config.load_dict(read_config(path))
    return config


def get_creds(args):
    """ Get credentials from argparse namespace. """
    if args.password_file:
        # Read password file?
        try:
            with open(args.password_file, 'r') as fp:
                passwd = fp.readline()
                if passwd.strip():
                    args.password = passwd.strip()
        except Exception as e:
            raise SystemExit('Unable to open password file: {0}'.format(e))

    if args.username and not args.password:
        try:
            args.password = getpass.getpass()
        except Exception as e:
            raise SystemExit('Prompt terminated: {0}'.format(e))

    if args.username or args.password:
        return pika.PlainCredentials(args.username, args.password)
    return None


def get_conn(args):
    """ Get pika connection from argparse namespace. """
    creds = get_creds(args)

    ssl_opts = {}
    if args.ssl_version:
        ssl_opts['ssl_version'] = SSL_PROTOCOLS[args.ssl_version]

    # if args.ssl_ciphers:
        # ssl_opts['ciphers'] = args.ssl_ciphers

    return pika.ConnectionParameters(args.host,
                                     args.port,
                                     virtual_host=args.vhost,
                                     ssl=args.ssl,
                                     ssl_options=ssl_opts,
                                     credentials=creds)


def setup_logging(args):
    """ Setup basic stderr logging. """
    logging.basicConfig()
    root = logging.getLogger()
    root.setLevel(args.log_level)
