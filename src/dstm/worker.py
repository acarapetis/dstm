"""Run an autowired dstm worker."""

from dstm.client.uri import client_from_uri
from dstm.task import TaskBackend, autowire
import os
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    backend = TaskBackend(
        topic_prefix=os.environ.get("DSTM_TOPIC_PREFIX", ""),
        wiring=autowire,
        client=client_from_uri(os.environ["DSTM_BROKER_URI"]),
    )
    task_groups = os.environ.get("DSTM_TASK_GROUPS", "dstm").split(",")
    backend.create_topics(task_groups=task_groups)
    backend.run_worker(task_groups=task_groups)
