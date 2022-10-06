# Proof of Concept using message brokers

## Kafka-Streams

Demonstrates how to use Kafka to process files (data streams).

## RabbitMQ-MultipleWorkers

Demonstrates how to use RabbitMQ to have multiple workers (consumers) consuming from a single queue.

## RabbitMQ-PubSub

Demonstrates how to use RabbitMQ to have a single producer emitting messages to a multiple consumers at a same time.
Eg. Consumer and Logger

### Non critical messages

For a non critical message, we can just set the queue as `exclusive: true` so, if a consumer disconnects from the queue, the message is lost.

### Critical messages

For a critical message, we can set the queue as `autoDelete: false` so, when the consumer disconnects from the queue, it will not be deleted and the message will be _buffered_ until the consumer reconnects.

## RabbitMQ-Streams-RetryWithDelay

Demonstrates how to use RabbitMQ to process files (data streams) with retry and delay mechanism in a failure scenario. This example uses the `rabbitmq_delayed_message_exchange` plugin.

## Rabbit-RetryDelayedWithoutPlugin
Demonstrates how to use RabbitMQ to reprocess a message after a failure without using plugins, just with exchange and dead letter queue (with ttl).

## RabbitMQ-Topics
