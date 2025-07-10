import json
import logging
from typing import Annotated

from typer import Option, Typer

from dstm.client.uri import client_from_uri
from dstm.tasks.broker import TaskBroker

cli = Typer()


@cli.callback()
def setup(verbose: Annotated[bool, Option("-v")] = False):
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    if not verbose:
        logging.getLogger("pika").setLevel(logging.WARNING)


@cli.command()
def worker(
    broker_uri: Annotated[str, Option(envvar="DSTM_BROKER_URI")],
    queues: Annotated[str, Option(envvar="DSTM_WORKER_QUEUES")] = "dstm",
    queue_prefix: Annotated[str, Option(envvar="DSTM_queue_PREFIX")] = "",
):
    queuelist = queues.split(",")
    broker = TaskBroker(queue_prefix=queue_prefix, client=client_from_uri(broker_uri))
    broker.create_queues(queues=queuelist)
    broker.run_worker(queues=queuelist)


@cli.command()
def submit(
    task: str,
    broker_uri: Annotated[str, Option(envvar="DSTM_BROKER_URI")],
    queue_prefix: Annotated[str, Option(envvar="DSTM_queue_PREFIX")] = "",
    args_json: Annotated[str, Option()] = "[]",
    kwargs_json: Annotated[str, Option()] = "{}",
):
    backend = TaskBroker(queue_prefix=queue_prefix, client=client_from_uri(broker_uri))
    args = json.loads(args_json)
    kwargs = json.loads(kwargs_json)
    backend.submit(backend.wiring.name_to_func(task), *args, **kwargs)


if __name__ == "__main__":
    cli()
