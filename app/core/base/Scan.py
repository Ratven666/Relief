from abc import ABC, abstractmethod

from sqlalchemy import delete, select, insert

from app.core.CONFIG import POINTS_CHUNK_COUNT
from app.core.base.Point import PointABC
from app.core.db.start_db import Tables, engine
from app.core.utils.ScanIterator import ScanIterator
from app.core.utils.ScanLoader import ScanLoader
from app.core.utils.ScanTXTSaver import ScanTXTSaver
from app.core.utils.ScanTxtParser import ScanTxtParser


class ScanABC(ABC):
    """
    Абстрактный класс скана
    """

    def __init__(self, scan_name):
        self.id = None
        self.scan_name: str = scan_name
        self.len: int = 0
        self.min_X, self.max_X = None, None
        self.min_Y, self.max_Y = None, None
        self.min_Z, self.max_Z = None, None

    def __str__(self):
        return f"{self.__class__.__name__} "\
               f"[id: {self.id},\tName: {self.scan_name}\tLEN: {self.len}]"

    def __repr__(self):
        return f"{self.__class__.__name__} [ID: {self.id}]"

    def __len__(self):
        return self.len

    @abstractmethod
    def __iter__(self):
        pass


class Scan(ScanABC):
    """
    Скан связанный с базой данных
    Точки при переборе скана берутся напрямую из БД
    """

    def __init__(self, scan_name, db_connection=None):
        super().__init__(scan_name)
        self.__init_scan(db_connection)

    def __iter__(self):
        """
        Иттератор скана берет точки из БД
        """
        return iter(ScanIterator(self))

    def delete_scan(self, db_connection=None):
        """
        Удаляет запись скана из БД
        :param scan_id: id скана который требуется удалить из БД
        :param db_connection: Открытое соединение с БД
        :return: None
        """
        stmt = delete(Tables.scans_db_table).where(Tables.scans_db_table.c.id == self.id)
        if db_connection is None:
            with engine.connect() as db_connection:
                db_connection.execute(stmt)
                db_connection.commit()
        else:
            db_connection.execute(stmt)
            db_connection.commit()

    def load_scan_from_file(self, file_name,
                            scan_loader=ScanLoader(scan_parser=ScanTxtParser(chunk_count=POINTS_CHUNK_COUNT))):
        """
        Загружает точки в скан из файла
        Ведется запись в БД
        Обновляются метрики скана в БД
        :param scan_loader: объект определяющий логику работы с БД при загрузке точек (
        принимает в себя парсер определяющий логику работы с конкретным типом файлов)
        :type scan_loader: ScanLoader
        :param file_name: путь до файла из которого будут загружаться данные
        :return: None
        """
        scan_loader.load_data(self, file_name)

    def save_scan_in_file(self, file_name=None, scan_saver=ScanTXTSaver()):
        scan_saver.save_scan(self, file_name)

    @classmethod
    def get_scan_from_id(cls, scan_id: int):
        """
        Возвращает объект скана по id
        :param scan_id: id скана который требуется загрузить и вернуть из БД
        :return: объект ScanDB с заданным id
        """
        select_ = select(Tables.scans_db_table).where(Tables.scans_db_table.c.id == scan_id)
        with engine.connect() as db_connection:
            db_scan_data = db_connection.execute(select_).mappings().first()
            if db_scan_data is not None:
                return cls(db_scan_data["scan_name"])
            else:
                raise ValueError("Нет скана с таким id!!!")

    def __init_scan(self, db_connection=None):
        """
        Инициализирует скан при запуске
        Если скан с таким именем уже есть в БД - запускает копирование данных из БД в атрибуты скана
        Если такого скана нет - создает новую запись в БД
        :param db_connection: Открытое соединение с БД
        :return: None
        """
        def init_logic(db_conn):
            select_ = select(Tables.scans_db_table).where(Tables.scans_db_table.c.scan_name == self.scan_name)
            db_scan_data = db_conn.execute(select_).mappings().first()
            if db_scan_data is not None:
                self.__copy_scan_data(db_scan_data)
            else:
                stmt = insert(Tables.scans_db_table).values(scan_name=self.scan_name)
                db_conn.execute(stmt)
                db_conn.commit()
                self.__init_scan(db_conn)

        if db_connection is None:
            with engine.connect() as db_connection:
                init_logic(db_connection)
        else:
            init_logic(db_connection)

    def __copy_scan_data(self, db_scan_data: dict):
        """
        Копирует данные записи из БД в атрибуты скана
        :param db_scan_data: Результат запроса к БД
        :return: None
        """
        self.id = db_scan_data["id"]
        self.scan_name = db_scan_data["scan_name"]
        self.len = db_scan_data["len"]
        self.min_X, self.max_X = db_scan_data["min_X"], db_scan_data["max_X"]
        self.min_Y, self.max_Y = db_scan_data["min_Y"], db_scan_data["max_Y"]
        self.min_Z, self.max_Z = db_scan_data["min_Z"], db_scan_data["max_Z"]


class ScanLite(ScanABC):
    """
    Скан не связанный с базой данных
    Все данные, включая точки при переборе берутся из оперативной памяти
    """

    def __init__(self, scan_name):
        super().__init__(scan_name)
        self.__points = []

    def __iter__(self):
        return iter(self.__points)

    def __len__(self):
        return len(self.__points)

    def add_point(self, point):
        """
        Добавляет точку в скан
        :param point: объект класса Point
        :return: None
        """
        if isinstance(point, PointABC):
            self.__points.append(point)
            self.len += 1
        else:
            raise TypeError(f"Можно добавить только объект точки. "
                             f"Переданно - {type(point)}, {point}")

    @classmethod
    def create_from_another_scan(cls, scan, copy_with_points=True):
        """
        Создает скан типа ScanLite и копирует в него данные из другого скана
        :param scan: копируемый скан
        :param copy_with_points: определяет нужно ли копировать скан вместе с точками
        :type copy_with_points: bool
        :return: объект класса ScanLite
        """
        scan_lite = cls(scan.scan_name)
        scan_lite.id = scan.id
        scan_lite.len = 0
        scan_lite.min_X, scan_lite.min_Y, scan_lite.min_Z = scan.min_X, scan.min_Y, scan.min_Z
        scan_lite.max_X, scan_lite.max_Y, scan_lite.max_Z = scan.max_X, scan.max_Y, scan.max_Z
        if copy_with_points:
            scan_lite.__points = [point for point in scan]
            scan_lite.len = len(scan_lite.__points)
        return scan_lite
