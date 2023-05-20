import logging
import sys

from app.core.CONFIG import LOGGING_LEVEL

console_logger = logging.getLogger("console")
formatter = logging.Formatter("%(asctime)s %(levelname)-10s %(module)-10s %(message)s")

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(formatter)

console_logger.addHandler(handler)
console_logger.setLevel(LOGGING_LEVEL)

if __name__ == "__main__":
    console_logger.critical("Критическая ошибка")
    console_logger.error("Ошибка")
    console_logger.debug("Отладочная информация")
    console_logger.info("Информационное сообщение")
