import os
import sqlite3

from sqlalchemy import select, and_

from app.core.CONFIG import DATABASE_NAME
from app.core.base.Point import Point
from app.core.db.start_db import engine, Tables


class ScanIterator:
    """
    Фабрика иттераторов сканов для сканов из БД
    """
    def __init__(self, scan):
        self.__scan_iterator = None
        self.__scan = scan
        self.__chose_scan_iterator()

    def __chose_scan_iterator(self):
        """
        Выбирает иттератор скана на основании типа используемой БД
        :return:
        """
        db_type = engine.dialect.name
        if db_type == "sqlite":
            self.__scan_iterator = SqlLiteScanIterator(self.__scan)
        else:
            self.__scan_iterator = BaseScanIterator(self.__scan)

    def __iter__(self):
        return iter(self.__scan_iterator)


class BaseScanIterator:
    """
    Универсальный иттератор для сканов из БД
    Реализован средствами sqlalchemy
    """

    def __init__(self, scan):
        self.__scan = scan
        self.__engine = engine.connect()
        self.__select = select(Tables.points_db_table). \
            join(Tables.points_scans_db_table, Tables.points_scans_db_table.c.point_id == Tables.points_db_table.c.id). \
            where(and_(self.__scan.id == Tables.points_scans_db_table.c.scan_id,
                       Tables.points_scans_db_table.c.is_active == True))
        self.__query = self.__engine.execute(self.__select)
        self.__iterator = None

    def __iter__(self):
        self.__iterator = iter(self.__query)
        return self

    def __next__(self):
        try:
            row = next(self.__iterator)
            point = Point.parse_point_from_db_row(row)
            return point
        except StopIteration:
            self.__engine.close()
            raise StopIteration
        finally:
            self.__engine.close()


class SqlLiteScanIterator:
    """
    Иттератор скана из БД SQLite
    Реализован через стандартную библиотеку sqlite3
    """
    def __init__(self, scan):
        self.__path = os.path.join(".", DATABASE_NAME)
        self.scan_id = scan.id
        self.cursor = None
        self.generator = None

    def __iter__(self):
        connection = sqlite3.connect(self.__path)
        self.cursor = connection.cursor()
        self.generator = (Point.parse_point_from_db_row(data) for data in
                          self.cursor.execute("""SELECT p.id, p.X, p.Y, p.Z,
                                                    p.R, p.G, p.B
                                                    FROM points p
                                                    JOIN points_scans ps ON ps.point_id = p.id
                                                    WHERE ps.scan_id = (?) AND ps.is_active = True""", (self.scan_id,)))

        return self.generator

    def __next__(self):
        try:
            return next(self.generator)
        except StopIteration:
            self.cursor.close()
            raise StopIteration
        finally:
            self.cursor.close()
