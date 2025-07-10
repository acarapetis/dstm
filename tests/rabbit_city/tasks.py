from dstm.tasks.wiring import HardWiring


def what_that_rabbit_do(name: str):
    print(f"{name} digs holes.")


def where_that_rabbit_live(name: str):
    print(f"{name} lives in a hole.")
    return "hole"


wiring = HardWiring(
    {
        "rabbits": {
            "what_do": what_that_rabbit_do,
            "where_live": where_that_rabbit_live,
        }
    }
)
