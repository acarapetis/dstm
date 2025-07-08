"""A common interface for AMQP (pika) and SQS (boto3) messaging systems, and a simple
task queue implementation built on top of this interface."""

from .message import Message

__all__ = ["Message"]
