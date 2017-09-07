# Pika cli client

This is a simple cli client for publishing and consuming messages from a message
broker using AMQP 0.91.

It can be used to send custom messages to a consumer, or consume and inspect
messages from a publisher.

## Install

```bash
pip install <this-repo>
```

## Configuration

TODO: Document how the config works.


## Usage

```bash
# Consume
python -m pika_cli_client.consumer -h
# or
pika-cli-consume -h

# Publish
python -m pika_cli_client.publisher -h
# or
pika-cli-publish -h
```
