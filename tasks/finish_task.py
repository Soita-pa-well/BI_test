import logging

logging.basicConfig(level=logging.INFO)


def finish() -> None:
    logging.info('Сервис закончил работу')
