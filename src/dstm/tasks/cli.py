import json
import logging
from typing import Annotated

from typer import Option, Typer

from dstm.client.uri import client_from_uri
from dstm.tasks.backend import TaskBackend

cli = Typer()


@cli.callback()
def setup(verbose: Annotated[bool, Option("-v")] = False):
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    if not verbose:
        logging.getLogger("pika").setLevel(logging.WARNING)


@cli.command()
def worker(
    broker_uri: Annotated[str, Option(envvar="DSTM_BROKER_URI")],
    task_groups: Annotated[str, Option(envvar="DSTM_TASK_GROUPS")] = "dstm",
    topic_prefix: Annotated[str, Option(envvar="DSTM_TOPIC_PREFIX")] = "",
):
    tgroups = task_groups.split(",")
    backend = TaskBackend(topic_prefix=topic_prefix, client=client_from_uri(broker_uri))
    backend.create_topics(task_groups=tgroups)
    backend.run_worker(task_groups=tgroups)


@cli.command()
def submit(
    task: str,
    broker_uri: Annotated[str, Option(envvar="DSTM_BROKER_URI")],
    topic_prefix: Annotated[str, Option(envvar="DSTM_TOPIC_PREFIX")] = "",
    args_json: Annotated[str, Option()] = "[]",
    kwargs_json: Annotated[str, Option()] = "{}",
):
    backend = TaskBackend(topic_prefix=topic_prefix, client=client_from_uri(broker_uri))
    args = json.loads(args_json)
    kwargs = json.loads(kwargs_json)
    backend.submit(backend.wiring.name_to_func(task), *args, **kwargs)


if __name__ == "__main__":
    cli()
