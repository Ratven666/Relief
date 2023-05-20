import logging
import os

from sqlalchemy import create_engine, MetaData

from app.core.logs.console_log_config import console_logger

from app.core.CONFIG import DATABASE_NAME, LOGGER
from app.core.db.TableInitializer import TableInitializer

path = os.path.join(".", DATABASE_NAME)
engine = create_engine(f'sqlite:///{path}')


db_metadata = MetaData()

Tables = TableInitializer(db_metadata)

logger = logging.getLogger(LOGGER)


def create_db():
    """
    Создает базу данных при ее отстутсвии
    :return: None
    """
    db_is_created = os.path.exists(path)
    if not db_is_created:
        db_metadata.create_all(engine)
        engine.dispose()
    else:
        logger.info("Такая БД уже есть!")
