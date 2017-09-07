#!/usr/bin/env python
# encoding: utf-8
""" Blocking AMQP-0.91 publisher. """
from __future__ import absolute_import, print_function

import argparse
import logging
import pika
import sys

from . import config

logger = logging.getLogger(__name__)


def confirm_callback(frame):
    if type(frame.method) == pika.spec.Confirm.SelectOk:
        print('The channel is now in confirm mode')
    elif type(frame.method) == pika.spec.Basic.Nack:
        print('Message {0} lost!'.format(frame.method.delivery_tag))
        sys.exit(1)
    elif type(frame.method) == pika.spec.Basic.Ack:
        print('Message {0} delivered!'.format(frame.method.delivery_tag))
        sys.exit(0)


def make_parser(conf):
    parser = config.make_config_argparser(conf, __doc__)

    # Message
    msg = parser.add_argument_group('Message', 'MQ message')
    msg.add_argument(
        '-e', '--exchange',
        type=str,
        dest='exchange',
        default='amqp_exchange',
        metavar='EXCHANGE',
        help='Exchange (default: %(default)s)')
    msg.add_argument(
        '-k', '--routing-key',
        type=str,
        dest='routing_key',
        default='amqp_key',
        metavar='KEY',
        help='Routing key (default: %(default)s)')
    msg.add_argument(
        '-t', '--content-type',
        type=str,
        dest='content_type',
        default='text/plain',
        metavar='TYPE',
        help='Content type (default: %(default)s)')
    msg.add_argument(
        'message_file',
        nargs='?',
        type=argparse.FileType('r'),
        metavar='FILE',
        default=sys.stdin)

    return parser


def main(args=None):
    conf = config.get_config(custom='config.yml')
    parser = make_parser(conf)
    args = parser.parse_args(args)
    config.setup_logging(args)
    logger.debug("config: {!r}".format(conf))

    conn = pika.BlockingConnection(config.get_conn(args))
    channel = conn.channel()

    # channel.confirm_delivery()  # RabbitMQ extension

    print('Sending message...')
    result = channel.basic_publish(
        body=args.message_file.read(),
        exchange=args.exchange,
        properties=pika.BasicProperties(
            content_type=args.content_type,
            delivery_mode=2),  # persistent
        routing_key=args.routing_key,
        mandatory=True,  # return an unroutable message with a Return method
        immediate=False  # require an immediate consumer
    )

    conn.close()

    if args.message_file != sys.stdin:
        args.message_file.close()

    if result:
        logger.info("Published message")
    else:
        logger.error("Unable to publish message")


if __name__ == '__main__':
    main()
