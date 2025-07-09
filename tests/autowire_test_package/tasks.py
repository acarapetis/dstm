from dstm.tasks.task import task


@task(task_group="warren")
def name_rabbits(*, count: int):
    """Learn the names of some rabbits."""
    print(f"There are {count} rabbits and they're all called Peter", flush=True)
