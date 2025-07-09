from dstm.task import task


@task("warren")
def name_rabbits(*, count: int):
    """Learn the names of some rabbits."""
    print(f"There are {count} rabbits and they're all called Peter", flush=True)
