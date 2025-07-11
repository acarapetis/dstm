# dstm

[![PyPI](https://img.shields.io/pypi/v/dstm)](https://pypi.org/project/dstm/)

A simple task queue library compatible with AWS SQS or AMQP.

I'm writing dstm because I've been experiencing problems with celery. Celery is big and
complicated, and has many great features that I don't need; which makes tracking down
problems difficult. I just need something that does the absolute basics reliably. That's
what `dstm` is designed to be.

## Quick Start

Define a task in any python module, e.g. in `myapp.tasks.email`:

```python
from dstm import task

@task(queue="high-priority")
def send_email(recipient: str, content: str):
    ...
```

Submit an instance of the task to a message broker:

```python
from dstm import TaskBroker
from dstm.client.sqs import SQSClient

from myapp.tasks.email import send_email

broker = TaskBroker(
    SQSClient(boto3.client("sqs")),
    queue_prefix="myapp-",
)
broker.submit(send_email, "linus@example.com", "Hi tehre")
```

Run a worker that executes tasks from the broker:

```python
from dstm import TaskBroker
from dstm.client.sqs import SQSClient

broker = TaskBroker(
    SQSClient(boto3.client("sqs")),
    queue_prefix="myapp-",
)

broker.run_worker(queues=["high-priority"])
```

Or using the CLI:

```sh
python3 -m dstm worker \
    --broker-uri "sqs://" \
    --queue-prefix "myapp-" \
    --queues "high-priority"
```
