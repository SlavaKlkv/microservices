import structlog


logger = structlog.get_logger()


def print_hi(name: str) -> None:
    logger.info("printing greeting", name=name)
    print(f"Hi, {name}")


if __name__ == "__main__":
    logger.info("application started")
    print_hi("PyCharm")
    logger.info("application finished")