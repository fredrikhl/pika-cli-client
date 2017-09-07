#!/usr/bin/env python
# encoding: utf-8
""" Blocking AMQP-0.91 consumer. """
from __future__ import absolute_import, print_function

import logging
import pika


# Try to prettify JSON
try:
    import json

    def pretty_json(body):
        try:
            parsed = json.loads(body)
            return json.dumps(parsed, indent=4, sort_keys=True)
        except:
            pass
        return body
except ImportError:
    def pretty_json(body):
        return body


from . import config

logger = logging.getLogger(__name__)


def block_format(strblock, prefix="", header="", indent=0):
    """ Indent and prefix lines of text. """
    def fmt_line(line):
        return '{!s} {!s} {!s}'.format(' ' * indent, prefix, line)

    def fmt_header(line):
        return "{!s}{!s}\n".format(' ' * indent, line) if line else ''

    return "{!s}{!s}\n".format(
        fmt_header(header),
        "\n".join(fmt_line(line) for line in strblock.split("\n")))


def consumer_callback(channel, method, header, body):

    logger.debug("New message")
    print(
        block_format("\n".join((
            "consumer: {m.consumer_tag}",
            "exchange: {m.exchange}",
            "routing-key: {m.routing_key}",
            "delivery-tag: {m.delivery_tag}",
            "redelivered: {m.redelivered}",
            "delivery-mode: {h.delivery_mode}",
            "content-type: {h.content_type}")).format(m=method, h=header),
                     prefix=">",
                     header="Metadata",
                     indent=2))
    if header.content_type == 'application/json':
        body = pretty_json(body)
    print(block_format(body, prefix=">>", header="Body", indent=2))
    # print('channel:', type(channel), repr(channel))
    # print('method:', type(method), repr(method))
    # print('header:', type(header), repr(header))

    channel.basic_ack(delivery_tag=method.delivery_tag)


def make_parser(conf):
    parser = config.make_config_argparser(conf, __doc__)

    # Consumer
    con = parser.add_argument_group('Consume', 'MQ consumer settings')

    con.add_argument(
        '-q', '--queue',
        metavar='QUEUE',
        type=str,
        dest='queue',
        default='amqp_queue',
        help='Queue (default: amqp_queue)')

    con.add_argument(
        '-t', '--tag',
        metavar='TAG',
        type=str,
        dest='consumer_tag',
        default='amqp_test',
        help='Consumer tag (default: amqp_test)')

    return parser


def main(args=None):
    conf = config.get_config(custom='config.yml')
    parser = make_parser(conf)
    args = parser.parse_args(args)
    config.setup_logging(args)
    logger.debug("config: {!r}".format(conf))

    conn_broker = pika.BlockingConnection(config.get_conn(args))

    channel = conn_broker.channel()

    channel.basic_consume(
        consumer_callback,  # the callback callable
        queue=args.queue,  # the queue to consume from
        no_ack=False,  # tell the broker to not expect a response
        exclusive=False,  # don’t allow other consumers on the queue
        consumer_tag=args.consumer_tag)

    logger.info("Waiting for messages...")
    channel.start_consuming()


if __name__ == '__main__':
    main()
