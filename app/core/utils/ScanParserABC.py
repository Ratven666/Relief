import logging
from abc import ABC, abstractmethod

from sqlalchemy import select, desc

from app.core.CONFIG import LOGGER
from app.core.db.start_db import Tables, engine


class ScanParserABC(ABC):
    """
    Абстрактный класс парсера данных для скана
    """
    logger = logging.getLogger(LOGGER)

    def __str__(self):
        return f"Парсер типа: {self.__class__.__name__}"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def _check_file_extension(file_name, __supported_file_extensions__):
        """
        Проверяет соответствует ли расширение файла допустимому для парсера
        :param file_name: имя и путь до файла, который будет загружаться
        :param __supported_file_extensions__: список допустимых расширений для выбранного парсера
        :return: None
        """
        file_extension = f".{file_name.split('.')[-1]}"
        if file_extension not in __supported_file_extensions__:
            raise TypeError(f"Неправильный для парсера тип файла. "
                            f"Ожидаются файлы типа: {__supported_file_extensions__}")

    @staticmethod
    def _get_last_point_id():
        """
        Возвращает последний id для точки в таблице БД points
        :return: последний id для точки в таблице БД points
        """
        with engine.connect() as db_connection:
            stmt = (select(Tables.points_db_table.c.id).order_by(desc("id")))
            last_point_id = db_connection.execute(stmt).first()
            if last_point_id:
                return last_point_id[0]
            else:
                return 0

    @abstractmethod
    def parse(self, file_name: str):
        """
        Запускает процедуру парсинга
        :param file_name: имя и путь до файла, который будет загружаться
        :return:
        """
        pass
