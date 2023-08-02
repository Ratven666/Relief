import os
import sqlite3
import logging
import sys
from abc import ABC, abstractmethod
from threading import Lock
from pathlib import Path
from statistics import median

from sqlalchemy import func, create_engine, MetaData,\
    and_, select, insert, desc, delete, update,\
    Table, Column, Integer, Float, String, Boolean, ForeignKey


from PyQt6 import QtCore
from PyQt6.QtGui import QIcon
from PyQt6.QtWidgets import QApplication, QWidget, QFileDialog, QSlider, QMessageBox, QLabel, \
    QGridLayout, QVBoxLayout, QSpacerItem, \
    QTextEdit, QToolButton, QHBoxLayout, QSpinBox, QProgressBar, QPushButton, QSizePolicy


DATABASE_NAME = "TEMP_DB.sqlite"
POINTS_CHUNK_COUNT = 100_000
LOGGER = "console"
LOGGING_LEVEL = "DEBUG"


console_logger = logging.getLogger("console")
formatter = logging.Formatter("%(asctime)s %(levelname)-10s %(module)-10s %(message)s")

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(formatter)

console_logger.addHandler(handler)
console_logger.setLevel(LOGGING_LEVEL)


class SingletonMeta(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class TableInitializer(metaclass=SingletonMeta):
    """
    Объект инициализирующий и создающий таблицы в БД
    """

    def __init__(self, metadata):
        self.__db_metadata = metadata
        self.points_db_table = self.__create_points_db_table()
        self.scans_db_table = self.__create_scans_db_table()
        self.points_scans_db_table = self.__create_points_scans_db_table()
        self.imported_files_db_table = self.__create_imported_files_table()
        self.voxel_models_db_table = self.__create_voxel_models_db_table()
        self.voxels_db_table = self.__create_voxels_db_table()
        self.dem_models_db_table = self.__create_dem_models_db_table()
        self.dem_cell_db_table = self.__create_dem_cell_db_table()
        self.bi_cell_db_table = self.__create_bi_cell_db_table()

    def __create_points_db_table(self):
        points_db_table = Table("points", self.__db_metadata,
                                Column("id", Integer, primary_key=True),
                                Column("X", Float, nullable=False),
                                Column("Y", Float, nullable=False),
                                Column("Z", Float, nullable=False),
                                Column("R", Integer, default=0),
                                Column("G", Integer, default=0),
                                Column("B", Integer, default=0)
                                )
        return points_db_table

    def __create_scans_db_table(self):
        scans_db_table = Table("scans", self.__db_metadata,
                               Column("id", Integer, primary_key=True),
                               Column("scan_name", String, nullable=False, unique=True, index=True),
                               Column("len", Integer, default=0),
                               Column("min_X", Float),
                               Column("max_X", Float),
                               Column("min_Y", Float),
                               Column("max_Y", Float),
                               Column("min_Z", Float),
                               Column("max_Z", Float),
                               )
        return scans_db_table

    def __create_points_scans_db_table(self):
        points_scans_db_table = Table("points_scans", self.__db_metadata,
                                      Column("point_id", Integer, ForeignKey("points.id", ondelete="CASCADE"),
                                             primary_key=True),
                                      Column("scan_id", Integer, ForeignKey("scans.id", ondelete="CASCADE"),
                                             primary_key=True),
                                      Column("is_active", Boolean, default=True)
                                      )
        return points_scans_db_table

    def __create_imported_files_table(self):
        imported_files_table = Table("imported_files", self.__db_metadata,
                                     Column("id", Integer, primary_key=True),
                                     Column("file_name", String, nullable=False),
                                     Column("scan_id", Integer, ForeignKey("scans.id"))
                                     )
        return imported_files_table

    def __create_voxels_db_table(self):
        voxels_db_table = Table("voxels", self.__db_metadata,
                                Column("id", Integer, primary_key=True),
                                Column("vxl_name", String, nullable=False, unique=True, index=True),
                                Column("X", Float),
                                Column("Y", Float),
                                Column("Z", Float),
                                Column("step", Float, nullable=False),
                                Column("len", Integer, default=0),
                                Column("R", Integer, default=0),
                                Column("G", Integer, default=0),
                                Column("B", Integer, default=0),
                                Column("vxl_mdl_id", Integer, ForeignKey("voxel_models.id"))
                                )
        return voxels_db_table

    def __create_voxel_models_db_table(self):
        voxel_models_db_table = Table("voxel_models", self.__db_metadata,
                                      Column("id", Integer, primary_key=True),
                                      Column("vm_name", String, nullable=False, unique=True, index=True),
                                      Column("step", Float, nullable=False),
                                      Column("dx", Float, nullable=False),
                                      Column("dy", Float, nullable=False),
                                      Column("len", Integer, default=0),
                                      Column("X_count", Integer, default=0),
                                      Column("Y_count", Integer, default=0),
                                      Column("Z_count", Integer, default=0),
                                      Column("min_X", Float),
                                      Column("max_X", Float),
                                      Column("min_Y", Float),
                                      Column("max_Y", Float),
                                      Column("min_Z", Float),
                                      Column("max_Z", Float),
                                      Column("base_scan_id", Integer, ForeignKey("scans.id"))
                                      )
        return voxel_models_db_table

    def __create_dem_models_db_table(self):
        dem_models_db_table = Table("dem_models", self.__db_metadata,
                                    Column("id", Integer, primary_key=True),
                                    Column("base_voxel_model_id", Integer,
                                           ForeignKey("voxel_models.id")),
                                    Column("model_type", String, nullable=False),
                                    Column("model_name", String, nullable=False, unique=True),
                                    Column("MSE_data", Float, default=None)
                                    )
        return dem_models_db_table

    def __create_dem_cell_db_table(self):
        dem_cell_db_table = Table("dem_cells", self.__db_metadata,
                                  Column("voxel_id", Integer,
                                         ForeignKey("voxels.id", ondelete="CASCADE"),
                                         primary_key=True),
                                  Column("base_model_id", Integer,
                                         ForeignKey("dem_models.id", ondelete="CASCADE"),
                                         primary_key=True),
                                  Column("Avr_Z", Float),
                                  Column("r", Integer),
                                  Column("MSE", Float, default=None)
                                  )
        return dem_cell_db_table

    def __create_bi_cell_db_table(self):
        bi_cell_db_table = Table("bi_cells", self.__db_metadata,
                                 Column("voxel_id", Integer,
                                        ForeignKey("voxels.id", ondelete="CASCADE"),
                                        primary_key=True),
                                 Column("base_model_id", Integer,
                                        ForeignKey("dem_models.id", ondelete="CASCADE"),
                                        primary_key=True),
                                 Column("Z_ld", Float),
                                 Column("Z_lu", Float),
                                 Column("Z_rd", Float),
                                 Column("Z_ru", Float),
                                 Column("MSE_ld", Float),
                                 Column("MSE_lu", Float),
                                 Column("MSE_rd", Float),
                                 Column("MSE_ru", Float),
                                 Column("r", Integer),
                                 Column("MSE", Float, default=None)
                                 )
        return bi_cell_db_table


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


class Point:
    """
    Класс точки
    """
    __slots__ = ["id", "X", "Y", "Z", "R", "G", "B"]

    def __init__(self, X, Y, Z, R=0, G=0, B=0, id_=None):
        self.id = id_
        self.X, self.Y, self.Z = X, Y, Z
        self.R, self.G, self.B = R, G, B

    def __str__(self):
        return f"{self.__class__.__name__} " \
               f"[id: {self.id},\tx: {self.X} y: {self.Y} z: {self.Z},\t" \
               f"RGB: ({self.R},{self.G},{self.B})]"

    def __repr__(self):
        return f"{self.__class__.__name__} [id: {self.id}]"

    @classmethod
    def parse_point_from_db_row(cls, row: tuple):
        """
        Метод который создает и возвращает объект Point по данным читаемым из БД
        :param row: кортеж данных читаемых из БД
        :return: объект класса Point
        """
        return cls(id_=row[0], X=row[1], Y=row[2], Z=row[3], R=row[4], G=row[5], B=row[6])


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
            join(Tables.points_scans_db_table,
                 Tables.points_scans_db_table.c.point_id == Tables.points_db_table.c.id). \
            where(and_(self.__scan.id == Tables.points_scans_db_table.c.scan_id,
                       Tables.points_scans_db_table.c.is_active is True))
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


class ImportedFileDB:
    """
    Класс определяющий логику контроля повторной загрузки файла с данными
    """

    def __init__(self, file_name):
        self.__file_name = file_name
        self.__hash = None

    def is_file_already_imported_into_scan(self, scan):
        """
        Проверяет был ли этот файл уже загружен в скан

        :param scan: скан в который загружаются данные из файла
        :type scan: ScanDB
        :return: True / False
        """
        select_ = select(Tables.imported_files_db_table).where(
            and_(Tables.imported_files_db_table.c.file_name == self.__file_name,
                 Tables.imported_files_db_table.c.scan_id == scan.id))
        with engine.connect() as db_connection:
            imp_file = db_connection.execute(select_).first()
        if imp_file is None:
            return False
        return True

    def insert_in_db(self, scan):
        """
        Добавляет в таблицу БД imported_files данные о файле и скане в который он был загружен
        :param scan: скан в который загружаются данные из файла
        :type scan: ScanDB
        :return: None
        """
        with engine.connect() as db_connection:
            stmt = insert(Tables.imported_files_db_table).values(file_name=self.__file_name,
                                                                 scan_id=scan.id)
            db_connection.execute(stmt)
            db_connection.commit()


def calk_scan_metrics(scan_id):
    """
    Рассчитывает метрики скана средствами SQL
    :param scan_id: id скана для которого будет выполняться расчет метрик
    :return: словарь с метриками скана
    """
    with engine.connect() as db_connection:
        stmt = select(func.count(Tables.points_db_table.c.id).label("len"),
                      func.min(Tables.points_db_table.c.X).label("min_X"),
                      func.max(Tables.points_db_table.c.X).label("max_X"),
                      func.min(Tables.points_db_table.c.Y).label("min_Y"),
                      func.max(Tables.points_db_table.c.Y).label("max_Y"),
                      func.min(Tables.points_db_table.c.Z).label("min_Z"),
                      func.max(Tables.points_db_table.c.Z).label("max_Z")).where(and_(
                               Tables.points_scans_db_table.c.point_id == Tables.points_db_table.c.id,
                               Tables.points_scans_db_table.c.scan_id == Tables.scans_db_table.c.id,
                               Tables.points_scans_db_table.c.is_active == 1,
                               Tables.scans_db_table.c.id == scan_id
                      ))
        scan_metrics = dict(db_connection.execute(stmt).mappings().first())
        scan_metrics["id"] = scan_id
        return scan_metrics


def update_scan_metrics(scan):
    """
    Рассчитывает значения метрик скана по точкам загруженным в БД
    средствами SQL и обновляет их в самом скане
    :param scan: скан для которого рассчитываются и в котором обновляются метрики
    :return: скан с обновленными  метриками
    """
    scan_metrics = calk_scan_metrics(scan_id=scan.id)
    scan.len = scan_metrics["len"]
    scan.min_X, scan.max_X = scan_metrics["min_X"], scan_metrics["max_X"]
    scan.min_Y, scan.max_Y = scan_metrics["min_Y"], scan_metrics["max_Y"]
    scan.min_Z, scan.max_Z = scan_metrics["min_Z"], scan_metrics["max_Z"]
    return scan


def update_scan_in_db_from_scan_metrics(scan_metrics: dict):
    """
    Обновляет значения метрик скана в БД
    :param scan_metrics: словарь с метриками скана
    :return: None
    """
    with engine.connect() as db_connection:
        stmt = update(Tables.scans_db_table) \
            .where(Tables.scans_db_table.c.id == scan_metrics["id"]) \
            .values(scan_name=scan_metrics["scan_name"],
                    len=scan_metrics["len"],
                    min_X=scan_metrics["min_X"],
                    max_X=scan_metrics["max_X"],
                    min_Y=scan_metrics["min_Y"],
                    max_Y=scan_metrics["max_Y"],
                    min_Z=scan_metrics["min_Z"],
                    max_Z=scan_metrics["max_Z"])
        db_connection.execute(stmt)
        db_connection.commit()


def update_scan_in_db_from_scan(updated_scan, db_connection=None):
    """
    Обновляет значения метрик скана в БД
    :param updated_scan: Объект скана для которого обновляются метрики
    :param db_connection: Открытое соединение с БД
    :return: None
    """
    stmt = update(Tables.scans_db_table) \
        .where(Tables.scans_db_table.c.id == updated_scan.id) \
        .values(scan_name=updated_scan.scan_name,
                len=updated_scan.len,
                min_X=updated_scan.min_X,
                max_X=updated_scan.max_X,
                min_Y=updated_scan.min_Y,
                max_Y=updated_scan.max_Y,
                min_Z=updated_scan.min_Z,
                max_Z=updated_scan.max_Z)
    if db_connection is None:
        with engine.connect() as db_connection:
            db_connection.execute(stmt)
            db_connection.commit()
    else:
        db_connection.execute(stmt)
        db_connection.commit()


def update_scan_borders(scan, point):
    """
    Проверяет положение в точки в существующих границах скана
    и меняет их при выходе точки за их пределы
    :param scan: скан
    :param point: точка
    :return: None
    """
    if scan.min_X is None:
        scan.min_X, scan.max_X = point.X, point.X
        scan.min_Y, scan.max_Y = point.Y, point.Y
        scan.min_Z, scan.max_Z = point.Z, point.Z
    if point.X < scan.min_X:
        scan.min_X = point.X
    if point.X > scan.max_X:
        scan.max_X = point.X
    if point.Y < scan.min_Y:
        scan.min_Y = point.Y
    if point.Y > scan.max_Y:
        scan.max_Y = point.Y
    if point.Z < scan.min_Z:
        scan.min_Z = point.Z
    if point.Z > scan.max_Z:
        scan.max_Z = point.Z


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


class ScanTxtParser(ScanParserABC):
    """
    Парсер точек из текстового txt формата
    Формат данных:
        4.2517 -14.2273 33.4113 208 195 182 -0.023815 -0.216309 0.976035
          X        Y       Z     R   G   B      nX nY nZ (не обязательны и пока игнорируются)
    """
    __supported_file_extension__ = [".txt", ".ascii"]

    def __init__(self, chunk_count=POINTS_CHUNK_COUNT):
        self.__chunk_count = chunk_count
        self.__last_point_id = None

    def parse(self, file_name):
        """
        Запускает процедуру парсинга файла и возвращает списки словарей с данными для загрузки в БД
        размером не превышающим POINTS_CHUNK_COUNT
        При запуске выполняется процедурка проверки расширения файла
        :param file_name: путь до файла из которго будут загружаться данные
        :return: список точек готовый к загрузке в БД
        """
        self._check_file_extension(file_name, self.__supported_file_extension__)
        self.__last_point_id = self._get_last_point_id()

        with open(file_name, "rt", encoding="utf-8") as file:
            points = []
            for line in file:
                line = line.strip().split()
                self.__last_point_id += 1
                try:
                    if len(line) == 3:
                        point = {"id": self.__last_point_id,
                                 "X": line[0], "Y": line[1], "Z": line[2],
                                 }
                    elif len(line) == 6 or len(line) == 9:
                        point = {"id": self.__last_point_id,
                                 "X": line[0], "Y": line[1], "Z": line[2],
                                 "R": line[3], "G": line[4], "B": line[5]
                                 }
                    elif len(line) == 7:
                        point = {"id": self.__last_point_id,
                                 "X": line[0], "Y": line[1], "Z": line[2],
                                 "R": line[3], "G": line[4], "B": line[5],
                                 "is_ground": line[6]
                                 }
                except IndexError:
                    self.logger.critical(f"Структура \"{file_name}\" некорректна - \"{line}\"")
                    return
                points.append(point)
                if len(points) == self.__chunk_count:
                    yield points
                    points = []
            yield points


class ScanLoader:
    """
    Класс, определяющий логику загрузки точек в БД
    """
    __logger = logging.getLogger(LOGGER)

    def __init__(self, scan_parser=ScanTxtParser()):
        self.__scan_parser = scan_parser

    def load_data(self, scan, file_name: str):
        """
        Загрузка данных из файла в базу данных

        :param scan: скан в который загружаются данные из файла
        :type scan: ScanDB
        :param file_name: путь до файла с данными
        :type file_name: str
        :return: None

        При выполнении проверяется был ли ранее произведен импорт в этот скан из этого файла.
        Если файл ранее не импортировался - происходит загрузка.
        Полсле загрузки данных рассчитываются новые метрики скана, которые обновляют его свойства в БД
        Файл с данными записывается в таблицу imported_files
        """
        imp_file = ImportedFileDB(file_name)

        if imp_file.is_file_already_imported_into_scan(scan):
            self.__logger.info(f"Файл \"{file_name}\" уже загружен в скан \"{scan.scan_name}\"")
            return

        with engine.connect() as db_connection:
            for points in self.__scan_parser.parse(file_name):
                points_scans = self.__get_points_scans_list(scan, points)
                self.__insert_to_db(points, points_scans, db_connection)
                self.__logger.info(f"Пакет точек загружен в БД")
            db_connection.commit()
        scan = update_scan_metrics(scan)
        update_scan_in_db_from_scan(scan)
        imp_file.insert_in_db(scan)
        self.__logger.info(f"Точки из файла \"{file_name}\" успешно"
                           f" загружены в скан \"{scan.scan_name}\"")

    @staticmethod
    def __get_points_scans_list(scan, points):
        """
        Собирает список словарей для пакетной загрузки в таблицу points_scans_db_table

        :param scan: скан в который загружаются данные из файла
        :type scan: ScanDB
        :param points: список точек полученный из парсера
        :type points: list
        :return: список словарей для пакетной загрузки в таблицу points_scans_db_table
        """
        points_scans = []
        for point in points:
            points_scans.append({"point_id": point["id"], "scan_id": scan.id})
        return points_scans

    @staticmethod
    def __insert_to_db(points, points_scans, db_engine_connection):
        """
        Загружает данные о точках и их связях со сканами в БД
        :param points: список словарей для пакетной загрузки в таблицу points_db_table
        :param points_scans: список словарей для пакетной загрузки в таблицу points_scans_db_table
        :param db_engine_connection: открытое соединение с БД
        :return: None
        """
        db_engine_connection.execute(Tables.points_db_table.insert(), points)
        db_engine_connection.execute(Tables.points_scans_db_table.insert(), points_scans)

    @property
    def scan_parser(self):
        return self.__scan_parser

    @scan_parser.setter
    def scan_parser(self, parser: ScanParserABC):
        if isinstance(parser, ScanParserABC):
            self.__scan_parser = parser
        else:
            raise TypeError(f"Нужно передать объект парсера! "
                            f"Переданно - {type(parser)}, {parser}")


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


class VoxelABC(ABC):
    """
    Абстрактный класс вокселя
    """

    logger = logging.getLogger(LOGGER)

    def __init__(self, X, Y, Z, step, vxl_mdl_id):
        self.id = None
        self.X = X
        self.Y = Y
        self.Z = Z
        self.step = step
        self.vxl_mdl_id = vxl_mdl_id
        self.vxl_name = self.__name_generator()
        self.len = 0
        self.R, self.G, self.B = 0, 0, 0

    def __name_generator(self):
        """
        Конструктор имени вокселя
        :return: None
        """
        return (f"VXL_VM:{self.vxl_mdl_id}_s{self.step}_"
                f"X:{round(self.X, 5)}_"
                f"Y:{round(self.Y, 5)}_"
                f"Z:{round(self.Z, 5)}"
                )

    def __str__(self):
        return (f"{self.__class__.__name__} "
                f"[id: {self.id},\tName: {self.vxl_name}\t\t"
                f"X: {round(self.X, 5)}\tY: {round(self.Y, 5)}\tZ: {round(self.Z, 5)}]"
                )

    def __repr__(self):
        return f"{self.__class__.__name__} [ID: {self.id}]"

    def __len__(self):
        return self.len


class Voxel(VoxelABC):
    """
    Воксель связанный с базой данных
    """

    __slots__ = ["id", "X", "Y", "Z", "step", "vxl_mdl_id", "vxl_name", "len", "R", "G", "B"]

    def __init__(self, X, Y, Z, step, vxl_mdl_id, db_connection=None):
        super().__init__(X, Y, Z, step, vxl_mdl_id)
        self.__init_voxel(db_connection)

    @staticmethod
    def delete_voxel(voxel_id, db_connection=None):
        """
        Удаляет запись вокселя из БД
        :param voxel_id: id вокселя который требуется удалить из БД
        :param db_connection: Открытое соединение с БД
        :return: None
        """
        stmt = delete(Tables.voxels_db_table).where(Tables.voxels_db_table.c.id == voxel_id)
        if db_connection is None:
            with engine.connect() as db_connection:
                db_connection.execute(stmt)
                db_connection.commit()
        else:
            db_connection.execute(stmt)
            db_connection.commit()

    def __init_voxel(self, db_connection=None):
        """
        Инициализирует воксель при запуске
        Если воксель с таким именем уже есть в БД - запускает копирование данных из БД в атрибуты скана
        Если такого вокселя нет - создает новую запись в БД
        :param db_connection: Открытое соединение с БД
        :return: None
        """
        def init_logic(db_conn):
            select_ = select(Tables.voxels_db_table).where(Tables.voxels_db_table.c.vxl_name == self.vxl_name)
            db_voxel_data = db_conn.execute(select_).mappings().first()
            if db_voxel_data is not None:
                self.__copy_voxel_data(db_voxel_data)
            else:
                stmt = insert(Tables.voxels_db_table).values(vxl_name=self.vxl_name,
                                                             X=self.X,
                                                             Y=self.Y,
                                                             Z=self.Z,
                                                             step=self.step,
                                                             vxl_mdl_id=self.vxl_mdl_id,
                                                             )
                db_conn.execute(stmt)
                db_conn.commit()
                self.__init_voxel(db_conn)

        if db_connection is None:
            with engine.connect() as db_connection:
                init_logic(db_connection)
        else:
            init_logic(db_connection)

    def __copy_voxel_data(self, db_voxel_data: dict):
        """
        Копирует данные записи из БД в атрибуты вокселя
        :param db_voxel_data: Результат запроса к БД
        :return: None
        """
        self.id = db_voxel_data["id"]
        self.X = db_voxel_data["X"]
        self.Y = db_voxel_data["Y"]
        self.Z = db_voxel_data["Z"]
        self.step = db_voxel_data["step"]
        self.vxl_mdl_id = db_voxel_data["vxl_mdl_id"]
        self.vxl_name = db_voxel_data["vxl_name"]
        self.len = db_voxel_data["len"]
        self.R = db_voxel_data["R"]
        self.G = db_voxel_data["G"]
        self.B = db_voxel_data["B"]


class VoxelLite(VoxelABC):
    """
    Воксель не связанный с базой данных
    """
    __slots__ = ["id", "X", "Y", "Z", "step", "vxl_mdl_id", "vxl_name", "len", "R", "G", "B"]

    def __init__(self, X, Y, Z, step, vxl_mdl_id):
        super().__init__(X, Y, Z, step, vxl_mdl_id)


class VMFullBaseIterator:
    """
    Иттератор полной воксельной модели
    """
    def __init__(self, vxl_mdl):
        self.vxl_mdl = vxl_mdl
        self.x = 0
        self.y = 0
        self.z = 0
        self.X_count, self.Y_count, self.Z_count = vxl_mdl.X_count, vxl_mdl.Y_count, vxl_mdl.Z_count

    def __iter__(self):
        return self

    def __next__(self):
        for vxl_z in range(self.z, self.Z_count):
            for vxl_y in range(self.y, self.Y_count):
                for vxl_x in range(self.x, self.X_count):
                    self.x += 1
                    return self.vxl_mdl.voxel_structure[vxl_z][vxl_y][vxl_x]
                self.y += 1
                self.x = 0
            self.z += 1
            self.y = 0
        raise StopIteration


def update_voxel_model_in_db_from_voxel_model(updated_voxel_model, db_connection=None):
    """
    Обновляет значения метрик воксельной модели в БД
    :param updated_voxel_model: Объект воксельной модели для которой обновляются метрики
    :param db_connection: Открытое соединение с БД
    :return: None
    """
    stmt = update(Tables.voxel_models_db_table) \
        .where(Tables.voxel_models_db_table.c.id == updated_voxel_model.id) \
        .values(id=updated_voxel_model.id,
                vm_name=updated_voxel_model.vm_name,
                step=updated_voxel_model.step,
                len=updated_voxel_model.len,
                X_count=updated_voxel_model.X_count,
                Y_count=updated_voxel_model.Y_count,
                Z_count=updated_voxel_model.Z_count,
                min_X=updated_voxel_model.min_X,
                max_X=updated_voxel_model.max_X,
                min_Y=updated_voxel_model.min_Y,
                max_Y=updated_voxel_model.max_Y,
                min_Z=updated_voxel_model.min_Z,
                max_Z=updated_voxel_model.max_Z,
                base_scan_id=updated_voxel_model.base_scan_id)
    if db_connection is None:
        with engine.connect() as db_connection:
            db_connection.execute(stmt)
            db_connection.commit()
    else:
        db_connection.execute(stmt)
        db_connection.commit()


class FastVMSeparator:
    """
    Быстрый сепоратор воксельной модели через создание
    полной воксельной структуры в оперативной памяти
    """

    def __init__(self):
        self.voxel_model = None
        self.voxel_structure = None

    def separate_voxel_model(self, voxel_model, scan):
        """
        Общая логика разбиения воксельной модели
        :param voxel_model: воксельная модель
        :param scan: скан
        :return: None
        1. Создается полная воксельная структура
        2. Скан разбивается на отдельные воксели
        3. Загружает метрики сканов и вокселей игнорируя пустые
        """
        voxel_model.logger.info(f"Начато создание структуры {voxel_model.vm_name}")
        self.__create_full_vxl_struct(voxel_model)
        voxel_model.logger.info(f"Структура {voxel_model.vm_name} создана")
        voxel_model.logger.info(f"Начат расчет метрик сканов и вокселей")
        self.__update_scan_and_voxel_data(scan)
        voxel_model.logger.info(f"Расчет метрик сканов и вокселей завершен")
        voxel_model.logger.info(f"Начата загрузка метрик сканов и вокселей в БД")
        self.__load_voxel_data_in_db()
        voxel_model.logger.info(f"Загрузка метрик сканов и вокселей в БД завершена")

    def __create_full_vxl_struct(self, voxel_model):
        """
        Создается полная воксельная структура
        :param voxel_model: воксельная модель
        :return: None
        """
        self.voxel_model = voxel_model
        self.voxel_structure = [[[VoxelLite(voxel_model.min_X + x * voxel_model.step,
                                            voxel_model.min_Y + y * voxel_model.step,
                                            voxel_model.min_Z + z * voxel_model.step,
                                            voxel_model.step, voxel_model.id)
                                  for x in range(voxel_model.X_count)]
                                 for y in range(voxel_model.Y_count)]
                                for z in range(voxel_model.Z_count)]
        self.voxel_model.voxel_structure = self.voxel_structure

    def __update_scan_and_voxel_data(self, scan):
        """
        Пересчитывает метрики сканов и вокселей по базовому скану scan
        :param scan: скан по которому разбивается воксельная модель
        :return: None
        """
        for point in scan:
            vxl_md_X = int((point.X - self.voxel_model.min_X) // self.voxel_model.step)
            vxl_md_Y = int((point.Y - self.voxel_model.min_Y) // self.voxel_model.step)
            if self.voxel_model.is_2d_vxl_mdl:
                vxl_md_Z = 0
            else:
                vxl_md_Z = int((point.Z - self.voxel_model.min_Z) // self.voxel_model.step)
            self.__update_voxel_data(self.voxel_structure[vxl_md_Z][vxl_md_Y][vxl_md_X], point)
        self.__init_voxels_id()

    @staticmethod
    def __update_voxel_data(voxel, point):
        """
        Обновляет значения метрик вокселя (цвет и количество точек)
        :param voxel: обновляемый воксель
        :param point: точка, попавшая в воксель
        :return: None
        """
        voxel.R = (voxel.R * voxel.len + point.R) / (voxel.len + 1)
        voxel.G = (voxel.G * voxel.len + point.G) / (voxel.len + 1)
        voxel.B = (voxel.B * voxel.len + point.B) / (voxel.len + 1)
        voxel.len += 1

    def __init_voxels_id(self):
        """
        Инициирует в воксели модели id
        :return: None
        """
        last_voxels_id_stmt = (select(Tables.voxels_db_table.c.id).order_by(desc("id")))
        with engine.connect() as db_connection:
            last_voxel_id = db_connection.execute(last_voxels_id_stmt).first()
        last_voxel_id = last_voxel_id[0] if last_voxel_id else 0
        for voxel in iter(VMFullBaseIterator(self.voxel_model)):
            last_voxel_id += 1
            voxel.id = last_voxel_id

    def __load_voxel_data_in_db(self):
        """
        Загружает значения метрик сканов и вокселей в БД
        игнорируя пустые воксели
        :return: None
        """
        voxels = []
        voxel_counter = 0
        for voxel in iter(VMFullBaseIterator(self.voxel_model)):
            if len(voxel) == 0:
                continue
            voxels.append({"id": voxel.id,
                           "vxl_name": voxel.vxl_name,
                           "X": voxel.X,
                           "Y": voxel.Y,
                           "Z": voxel.Z,
                           "step": voxel.step,
                           "len": voxel.len,
                           "R": round(voxel.R),
                           "G": round(voxel.G),
                           "B": round(voxel.B),
                           "vxl_mdl_id": voxel.vxl_mdl_id
                           })
            voxel_counter += 1
        with engine.connect() as db_connection:
            db_connection.execute(Tables.voxels_db_table.insert(), voxels)
            db_connection.commit()
        self.voxel_model.len = voxel_counter
        update_voxel_model_in_db_from_voxel_model(self.voxel_model)


class VoxelModelABC(ABC):
    """
    Абстрактный класс воксельной модели
    """
    logger = logging.getLogger(LOGGER)

    def __init__(self, scan, step, dx, dy, is_2d_vxl_mdl=True):
        self.id = None
        self.is_2d_vxl_mdl = is_2d_vxl_mdl
        self.step = step
        self.dx, self.dy = dx, dy
        self.vm_name: str = self.__name_generator(scan)
        self.len: int = 0
        self.X_count, self.Y_count, self.Z_count = None, None, None
        self.min_X, self.max_X = None, None
        self.min_Y, self.max_Y = None, None
        self.min_Z, self.max_Z = None, None
        self.base_scan_id = None

    def __name_generator(self, scan):
        """
        Конструктор имени воксельной модели
        :param scan: базовый скан, по которому создается модель
        :return: None
        """
        vm_type = "2D" if self.is_2d_vxl_mdl else "3D"
        return f"VM_{vm_type}_Sc:{scan.scan_name}_st:{self.step}_dx:{self.dx}_dy:{self.dy}"

    def __str__(self):
        return f"{self.__class__.__name__} " \
               f"[id: {self.id},\tName: {self.vm_name}\tLEN: (x:{self.X_count} * y:{self.Y_count} *" \
               f" z:{self.Z_count})={self.len}]"

    def __repr__(self):
        return f"{self.__class__.__name__} [ID: {self.id}]"

    def __len__(self):
        return self.len

    @abstractmethod
    def __iter__(self):
        pass


class VMRawIterator:
    """
    Универсальный иттератор вокселльной модели из БД
    Реализован средствами sqlalchemy
    """

    def __init__(self, vxl_model):
        self.__vxl_model = vxl_model
        self.__engine = engine.connect()
        self.__select = select(Tables.voxels_db_table).where(self.__vxl_model.id == Tables.voxels_db_table.c.vxl_mdl_id)
        self.__query = self.__engine.execute(self.__select).mappings()
        self.__iterator = None

    def __iter__(self):
        self.__iterator = iter(self.__query)
        return self

    def __next__(self):
        try:
            row = next(self.__iterator)
            voxel = VoxelLite(X=row["X"], Y=row["Y"], Z=row["Z"],
                              step=row["step"],
                              vxl_mdl_id=row["vxl_mdl_id"])
            voxel.id = row["id"]
            voxel.R, voxel.G, voxel.B = row["R"], row["G"], row["B"]
            voxel.len = row["len"]
            voxel.vxl_name = row["vxl_name"]
            return voxel
        except StopIteration:
            self.__engine.close()
            raise StopIteration
        finally:
            self.__engine.close()


class VoxelModel(VoxelModelABC):
    """
    Воксельная модель связанная с базой данных
    """

    def __init__(self, scan, step, dx=0.0, dy=0.0, is_2d_vxl_mdl=True,
                 voxel_model_separator=FastVMSeparator()):
        super().__init__(scan, step, dx, dy, is_2d_vxl_mdl)
        self.voxel_model_separator = voxel_model_separator
        self.__init_vxl_mdl(scan)
        self.voxel_structure = None

    def __iter__(self):
        return iter(VMRawIterator(self))

    def __init_vxl_mdl(self, scan):
        """
        Инициализирует воксельную модель при запуске
        Если воксельная модеьл с таким именем уже есть в БД - запускает копирование данных из БД в атрибуты модели
        Если такой воксельной модели нет - создает новую запись в БД и запускает процедуру рабиения скана на воксели
        по логике переданного в конструкторе воксельной модели разделителя voxel_model_separator
        :return: None
        """
        select_ = select(Tables.voxel_models_db_table).where(Tables.voxel_models_db_table.c.vm_name == self.vm_name)

        with engine.connect() as db_connection:
            db_vm_data = db_connection.execute(select_).mappings().first()
            if db_vm_data is not None:
                self.__copy_vm_data(db_vm_data)
            else:
                self.__calc_vxl_md_metric(scan)
                self.base_scan_id = scan.id
                stmt = insert(Tables.voxel_models_db_table).values(vm_name=self.vm_name,
                                                                   step=self.step,
                                                                   dx=self.dx,
                                                                   dy=self.dy,
                                                                   len=self.len,
                                                                   X_count=self.X_count,
                                                                   Y_count=self.Y_count,
                                                                   Z_count=self.Z_count,
                                                                   min_X=self.min_X,
                                                                   max_X=self.max_X,
                                                                   min_Y=self.min_Y,
                                                                   max_Y=self.max_Y,
                                                                   min_Z=self.min_Z,
                                                                   max_Z=self.max_Z,
                                                                   base_scan_id=self.base_scan_id
                                                                   )
                db_connection.execute(stmt)
                db_connection.commit()
                stmt = (select(Tables.voxel_models_db_table.c.id).order_by(desc("id")))
                self.id = db_connection.execute(stmt).first()[0]
                self.voxel_model_separator.separate_voxel_model(self, scan)

    def __calc_vxl_md_metric(self, scan):
        """
        Рассчитывает границы воксельной модели и максимальное количество вокселей
        исходя из размера вокселя и границ скана
        :param scan: скан на основе которого рассчитываются границы модели
        :return: None
        """
        if len(scan) == 0:
            return None
        self.min_X = (scan.min_X // self.step * self.step) - ((1 - self.dx) % 1 * self.step)
        self.min_Y = (scan.min_Y // self.step * self.step) - ((1 - self.dy) % 1 * self.step)
        self.min_Z = scan.min_Z // self.step * self.step

        self.max_X = (scan.max_X // self.step + 1) * self.step + ((self.dx % 1) * self.step)
        self.max_Y = (scan.max_Y // self.step + 1) * self.step + ((self.dy % 1) * self.step)
        self.max_Z = (scan.max_Z // self.step + 1) * self.step

        self.X_count = round((self.max_X - self.min_X) / self.step)
        self.Y_count = round((self.max_Y - self.min_Y) / self.step)
        if self.is_2d_vxl_mdl:
            self.Z_count = 1
        else:
            self.Z_count = round((self.max_Z - self.min_Z) / self.step)
        self.len = self.X_count * self.Y_count * self.Z_count

    def __copy_vm_data(self, db_vm_data: dict):
        """
        Копирует данные записи из БД в атрибуты вокселбной модели
        :param db_vm_data: Результат запроса к БД
        :return: None
        """
        self.id = db_vm_data["id"]
        self.vm_name = db_vm_data["vm_name"]
        self.step = db_vm_data["step"]
        self.dx = db_vm_data["dx"]
        self.dy = db_vm_data["dy"]
        self.len = db_vm_data["len"]
        self.X_count, self.Y_count, self.Z_count = db_vm_data["X_count"], db_vm_data["Y_count"], db_vm_data["Z_count"]
        self.min_X, self.max_X = db_vm_data["min_X"], db_vm_data["max_X"]
        self.min_Y, self.max_Y = db_vm_data["min_Y"], db_vm_data["max_Y"]
        self.min_Z, self.max_Z = db_vm_data["min_Z"], db_vm_data["max_Z"]
        self.base_scan_id = db_vm_data["base_scan_id"]
        if self.Z_count == 1:
            self.is_2d_vxl_mdl = True
        else:
            self.is_2d_vxl_mdl = False


class CellABC(ABC):
    """
    Абстрактный класс ячейки сегментированной модели
    """

    @abstractmethod
    def get_z_from_xy(self, x, y):
        """
        Рассчитывает отметку точки (x, y) в ячейке
        :param x: координата x
        :param y: координата y
        :return: координата z для точки (x, y)
        """
        pass

    @abstractmethod
    def get_mse_z_from_xy(self, x, y):
        """
        Рассчитывает СКП отметки точки (x, y) в ячейке
        :param x: координата x
        :param y: координата y
        :return: СКП координаты z для точки (x, y)
        """
        pass

    @abstractmethod
    def get_db_raw_data(self):
        pass

    @abstractmethod
    def _save_cell_data_in_db(self, db_connection):
        """
        Сохраняет данные ячейки из модели в БД
        :param db_connection: открытое соединение с БД
        :return: None
        """
        pass

    def _load_cell_data_from_db(self, db_connection):
        """
        Загружает данные ячейки из БД в модель
        :param db_connection: открытое соединение с БД
        :return: None
        """
        select_ = select(self.db_table) \
            .where(and_(self.db_table.c.voxel_id == self.voxel.id,
                        self.db_table.c.base_model_id == self.dem_model.id))
        db_cell_data = db_connection.execute(select_).mappings().first()
        if db_cell_data is not None:
            self._copy_cell_data(db_cell_data)

    @abstractmethod
    def _copy_cell_data(self, db_cell_data):
        """
        Копирует данные из записи БД в атрибуты ячейки
        :param db_cell_data: загруженные из БД данные
        :return: None
        """
        pass


class DemCell(CellABC):
    """
    Класс ячейки стандартной DEM модели
    """
    db_table = Tables.dem_cell_db_table

    def __init__(self, voxel, dem_model):
        self.voxel = voxel
        self.dem_model = dem_model
        self.voxel_id = None
        self.avr_z = None
        self.r = len(self.voxel) - 1
        self.mse = None

    def get_z_from_xy(self, x, y):
        """
        Рассчитывает отметку точки (x, y) в ячейке
        :param x: координата x
        :param y: координата y
        :return: координата z для точки (x, y)
        """
        return self.avr_z

    def get_mse_z_from_xy(self, x, y):
        return self.mse

    def get_db_raw_data(self):
        return {"voxel_id": self.voxel.id,
                "base_model_id": self.dem_model.id,
                "Avr_Z": self.avr_z,
                "r": self.r,
                "MSE": self.mse}

    def _save_cell_data_in_db(self, db_connection):
        """
        Сохраняет данные ячейки из модели в БД
        :param db_connection: открытое соединение с БД
        :return: None
        """
        stmt = insert(Tables.dem_cell_db_table).values(voxel_id=self.voxel.id,
                                                       base_model_id=self.dem_model.id,
                                                       Avr_Z=self.avr_z,
                                                       r=self.r,
                                                       MSE=self.mse,
                                                       )
        db_connection.execute(stmt)

    def _copy_cell_data(self, db_cell_data):
        """
        Копирует данные из записи БД в атрибуты ячейки
        :param db_cell_data: загруженные из БД данные
        :return: None
        """
        self.voxel_id = db_cell_data["voxel_id"]
        self.base_model_id = db_cell_data["base_model_id"]
        self.avr_z = db_cell_data["Avr_Z"]
        self.r = db_cell_data["r"]
        self.mse = db_cell_data["MSE"]

    def __str__(self):
        return f"{self.__class__.__name__} [ID: {self.voxel.id},\tavr_z: {self.avr_z:.3f}\t" \
               f"MSE: {self.mse:.3f}\tr: {self.r}]"

    def __repr__(self):
        return f"{self.__class__.__name__} [ID: {self.voxel.id}]"


class BiCell(CellABC):
    """
    Класс ячейки модели с билинейной интерполяцией между вершинами ячейки
    """
    db_table = Tables.bi_cell_db_table

    def __init__(self, cell, dem_model):
        self.cell = cell
        self.voxel = cell.voxel
        self.dem_model = dem_model
        self.voxel_id = None
        self.base_model_id = None
        self.r = len(self.voxel) - 4
        self.left_down = {"X": self.voxel.X, "Y": self.voxel.Y, "Z": None, "MSE": None}
        self.left_up = {"X": self.voxel.X, "Y": self.voxel.Y + self.voxel.step, "Z": None, "MSE": None}
        self.right_down = {"X": self.voxel.X + self.voxel.step, "Y": self.voxel.Y, "Z": None, "MSE": None}
        self.right_up = {"X": self.voxel.X + self.voxel.step, "Y": self.voxel.Y + self.voxel.step, "Z": None,
                         "MSE": None}
        self.mse = None

    def get_z_from_xy(self, x, y):
        """
        Рассчитывает отметку точки (x, y) в ячейке
        :param x: координата x
        :param y: координата y
        :return: координата z для точки (x, y)
        """
        try:
            x1, x2 = self.left_down["X"], self.right_down["X"]
            y1, y2 = self.left_down["Y"], self.left_up["Y"]
            r1 = ((x2 - x)/(x2 - x1)) * self.left_down["Z"] + ((x - x1)/(x2 - x1)) * self.right_down["Z"]
            r2 = ((x2 - x)/(x2 - x1)) * self.left_up["Z"] + ((x - x1)/(x2 - x1)) * self.right_up["Z"]
            z = ((y2 - y)/(y2 - y1)) * r1 + ((y - y1)/(y2 - y1)) * r2
        except TypeError:
            z = None
        return z

    def get_mse_z_from_xy(self, x, y):
        raise NotImplementedError

    def get_db_raw_data(self):
        return {"voxel_id": self.voxel.id,
                "base_model_id": self.dem_model.id,
                "Z_ld": self.left_down["Z"],
                "Z_lu": self.left_up["Z"],
                "Z_rd": self.right_down["Z"],
                "Z_ru": self.right_up["Z"],
                "MSE_ld": self.left_down["MSE"],
                "MSE_lu": self.left_up["MSE"],
                "MSE_rd": self.right_down["MSE"],
                "MSE_ru": self.right_up["MSE"],
                "r": self.r,
                "MSE": self.mse}

    def _save_cell_data_in_db(self, db_connection):
        """
        Сохраняет данные ячейки из модели в БД
        :param db_connection: открытое соединение с БД
        :return: None
        """
        stmt = insert(self.db_table).values(voxel_id=self.voxel.id,
                                            base_model_id=self.dem_model.id,
                                            Z_ld=self.left_down["Z"],
                                            Z_lu=self.left_up["Z"],
                                            Z_rd=self.right_down["Z"],
                                            Z_ru=self.right_up["Z"],
                                            MSE_ld=self.left_down["MSE"],
                                            MSE_lu=self.left_up["MSE"],
                                            MSE_rd=self.right_down["MSE"],
                                            MSE_ru=self.right_up["MSE"],
                                            r=self.r,
                                            MSE=self.mse,
                                            )
        db_connection.execute(stmt)

    def _copy_cell_data(self, db_cell_data):
        """
        Копирует данные из записи БД в атрибуты ячейки
        :param db_cell_data: загруженные из БД данные
        :return: None
        """
        self.voxel_id = db_cell_data["voxel_id"]
        self.base_model_id = db_cell_data["base_model_id"]
        self.left_down["Z"], self.left_down["MSE"] = db_cell_data["Z_ld"], db_cell_data["MSE_ld"]
        self.left_up["Z"], self.left_up["MSE"] = db_cell_data["Z_lu"], db_cell_data["MSE_lu"]
        self.right_down["Z"], self.right_down["MSE"] = db_cell_data["Z_rd"], db_cell_data["MSE_rd"]
        self.right_up["Z"], self.right_down["MSE"] = db_cell_data["Z_ru"], db_cell_data["MSE_ru"]
        self.r = db_cell_data["r"]
        self.mse = db_cell_data["MSE"]

    def __str__(self):
        return f"{self.__class__.__name__} [ID: {self.voxel.id},\tbi_model: {self.dem_model}\t" \
               f"MSE: {self.mse:.3f}\tr: {self.r}]"

    def __repr__(self):
        return f"{self.__class__.__name__} [ID: {self.voxel.id}]"


class SegmentedModelABC(ABC):
    """
    Абстрактный класс сегментированной модели
    """

    logger = logging.getLogger(LOGGER)
    db_table = Tables.dem_models_db_table

    def __init__(self, voxel_model, element_class):
        self.base_voxel_model_id = voxel_model.id
        self.voxel_model = voxel_model
        self._model_structure = {}
        self._create_model_structure(element_class)
        self.__init_model()

    def __iter__(self):
        return iter(self._model_structure.values())

    def __str__(self):
        return f"{self.__class__.__name__} [ID: {self.id},\tmodel_name: {self.model_name}]"

    def __repr__(self):
        return f"{self.__class__.__name__} [ID: {self.id}]"

    @abstractmethod
    def _calk_segment_model(self):
        """
        Метод определяющий логику создания конкретной модели
        :return: None
        """
        pass

    def _create_model_structure(self, element_class):
        """
        Создание структуры сегментированной модели
        :param element_class: Класс ячейки конкретной модели
        :return: None
        """
        for voxel in self.voxel_model:
            model_key = f"{voxel.X:.5f}_{voxel.Y:.5f}_{voxel.Z:.5f}"
            self._model_structure[model_key] = element_class(voxel, self)

    def get_model_element_for_point(self, point):
        """
        Возвращает ячейку содержащую точку point
        :param point: точка для которой нужна соответствующая ячейка
        :return: объект ячейки модели, содержащая точку point
        """
        vxl_md_X = int((point.X - self.voxel_model.min_X) // self.voxel_model.step)
        vxl_md_Y = int((point.Y - self.voxel_model.min_Y) // self.voxel_model.step)
        X = self.voxel_model.min_X + vxl_md_X * self.voxel_model.step
        Y = self.voxel_model.min_Y + vxl_md_Y * self.voxel_model.step
        if self.voxel_model.is_2d_vxl_mdl is False:
            Z = point.Z // self.voxel_model.step * self.voxel_model.step
        else:
            Z = self.voxel_model.min_Z
        model_key = f"{X:.5f}_{Y:.5f}_{Z:.5f}"
        return self._model_structure.get(model_key, None)

    def _calk_model_mse(self, db_connection):
        """
        Расчитывает СКП всей модели по СКП отдельных ячеек
        :param db_connection: открытое соединение с БД
        :return: None
        """
        vv = 0
        sum_of_r = 0
        for cell in self:
            if cell.r > 0 and cell.mse is not None:
                vv += (cell.mse ** 2) * cell.r
                sum_of_r += cell.r
        try:
            self.mse_data = (vv / sum_of_r) ** 0.5
        except ZeroDivisionError:
            self.mse_data = None
        stmt = update(self.db_table).values(MSE_data=self.mse_data).where(self.db_table.c.id == self.id)
        db_connection.execute(stmt)
        db_connection.commit()
        self.logger.info(f"Расчет СКП модели {self.model_name} завершен и загружен в БД")

    def _load_cell_data_from_db(self, db_connection):
        """
        Загружает данные всех ячеек модели из БД
        :param db_connection: открытое соединение с БД
        :return: None
        """
        for cell in self._model_structure.values():
            cell._load_cell_data_from_db(db_connection)

    def _save_cell_data_in_db(self, db_connection):
        """
        Сохраняет данные из всех ячеек модели в БД
        :param db_connection: открытое соединение с БД
        :return: None
        """
        for cell in self._model_structure.values():
            cell._save_cell_data_in_db(db_connection)

    def _get_last_model_id(self):
        """
        Возвращает последний id для сегментированной модели в таблице БД dem_models
        :return: последний id для сегментированной модели в таблице БД dem_models
        """
        with engine.connect() as db_connection:
            stmt = (select(self.db_table.c.id).order_by(desc("id")))
            last_model_id = db_connection.execute(stmt).first()
            if last_model_id:
                return last_model_id[0]
            else:
                return 0

    def _copy_model_data(self, db_model_data: dict):
        """
        Копирует данные из записи БД в атрибуты сегментированной модели
        :param db_model_data: Данные записи из БД
        :return: None
        """
        self.id = db_model_data["id"]
        self.base_voxel_model_id = db_model_data["base_voxel_model_id"]
        self.model_type = db_model_data["model_type"]
        self.model_name = db_model_data["model_name"]
        self.mse_data = db_model_data["MSE_data"]

    def __init_model(self):
        """
        Инициализирует сегментированную модель при запуске
        Если модель для воксельной модели нужного типа уже есть в БД - запускает
        копирование данных из БД в атрибуты модели
        Если такой модели нет - создает новую модели и запись в БД
        :return: None
        """
        select_ = select(self.db_table) \
            .where(and_(self.db_table.c.base_voxel_model_id == self.voxel_model.id,
                        self.db_table.c.model_type == self.model_type))

        with engine.connect() as db_connection:
            db_model_data = db_connection.execute(select_).mappings().first()
            if db_model_data is not None:
                self._copy_model_data(db_model_data)
                self._load_cell_data_from_db(db_connection)
                self.logger.info(f"Загрузка {self.model_name} модели завершена")
            else:
                stmt = insert(self.db_table).values(base_voxel_model_id=self.voxel_model.id,
                                                    model_type=self.model_type,
                                                    model_name=self.model_name,
                                                    MSE_data=self.mse_data
                                                    )
                db_connection.execute(stmt)
                db_connection.commit()
                self.id = self._get_last_model_id()
                self._calk_segment_model()
                self._calk_model_mse(db_connection)
                self._save_cell_data_in_db(db_connection)
                db_connection.commit()
                self.logger.info(f"Расчет модели {self.model_name} завершен и загружен в БД\n")

    def _calk_cell_mse(self, base_scan):
        """
        Расчитываает СКП в ячейках сегментированной модели от точек базового скана
        :param base_scan: базовый скан из воксельной модели
        :return: None
        """
        for point in base_scan:
            try:
                cell = self.get_model_element_for_point(point)
                cell_z = cell.get_z_from_xy(point.X, point.Y)
                if cell_z is None:
                    continue
            except AttributeError:
                continue
            try:
                cell.vv += (point.Z - cell_z) ** 2
            except AttributeError:
                cell.vv = (point.Z - cell_z) ** 2

        for cell in self:
            if cell.r > 0:
                try:
                    cell.mse = (cell.vv / cell.r) ** 0.5
                except AttributeError:
                    cell.mse = None
        self.logger.info(f"Расчет СКП высот в ячейках модели {self.model_name} завершен")

    def delete_model(self, db_connection=None):
        stmt_1 = delete(self.db_table).where(self.db_table.c.id == self.id)
        stmt_2 = delete(self.cell_type.db_table).where(self.cell_type.db_table.c.base_model_id == self.id)
        if db_connection is None:
            with engine.connect() as db_connection:
                db_connection.execute(stmt_1)
                db_connection.commit()
                db_connection.execute(stmt_2)
                db_connection.commit()
        else:
            db_connection.execute(stmt_1)
            db_connection.commit()
            db_connection.execute(stmt_2)
            db_connection.commit()
        self.logger.info(f"Удаление модели {self.model_name} из БД завершено\n")


class DemModel(SegmentedModelABC):
    """
    Стандартная DEM модель связанная с базой данных
    """

    def __init__(self, voxel_model):
        self.model_type = "DEM"
        self.model_name = f"{self.model_type}_from_{voxel_model.vm_name}"
        self.mse_data = None
        self.cell_type = DemCell
        super().__init__(voxel_model, self.cell_type)

    def _calk_segment_model(self):
        """
        Метод определяющий логику создания стандартной DEM модели
        :return: None
        """
        self.logger.info(f"Начат расчет модели {self.model_name}")
        base_scan = Scan.get_scan_from_id(self.voxel_model.base_scan_id)
        self.__calk_average_z(base_scan)
        self._calk_cell_mse(base_scan)

    def __calk_average_z(self, base_scan):
        """
        Расчет средней высотной отметки между точками в ячейке
        :param base_scan: базовый скан воксельной модели
        :return: None
        """
        for point in base_scan:
            dem_cell = self.get_model_element_for_point(point)
            if dem_cell is None:
                continue
            try:
                dem_cell.avr_z = (dem_cell.avr_z * dem_cell.len + point.Z) / (dem_cell.len + 1)
                dem_cell.len += 1
            except AttributeError:
                dem_cell.avr_z = point.Z
                dem_cell.len = 1
        self.logger.info(f"Расчет средних высот модели {self.model_name} завершен")


class BiModel(SegmentedModelABC):
    """
    Билинейно-интерполяционная модель связанная с базой данных
    """

    __base_models_classes = {"BI_DEM_WITH_MSE": DemModel,
                             "BI_DEM_WITHOUT_MSE": DemModel,
                             }

    def __init__(self, voxel_model, base_model_type, enable_mse=True):
        self.model_type = f"BI_{base_model_type}_WITH_MSE"
        self.model_name = f"{self.model_type}_from_{voxel_model.vm_name}"
        self.mse_data = None
        self.__enable_mse = enable_mse
        self.cell_type = BiCell
        super().__init__(voxel_model, self.cell_type)

    def _calk_segment_model(self):
        """
        Метод определяющий логику создания билинейной модели
        :return: None
        """
        self.logger.info(f"Начат расчет модели {self.model_name}")
        base_scan = Scan.get_scan_from_id(self.voxel_model.base_scan_id)
        self.__calk_cells_z()
        self._calk_cell_mse(base_scan)

    def __calk_cells_z(self):
        """
        Расчитывает средние отметки и СКП в узлах модели
        :return: None
        """
        for cell in self:
            n_s = self.__get_cell_neighbour_structure(cell)
            cell.left_down["Z"], cell.left_down["MSE"] = self.__calk_mean_z([[n_s[0][0], n_s[0][1]],
                                                                             [n_s[1][0], n_s[1][1]]])
            cell.left_up["Z"], cell.left_up["MSE"] = self.__calk_mean_z([[n_s[0][1], n_s[0][2]],
                                                                         [n_s[1][1], n_s[1][2]]])
            cell.right_down["Z"], cell.right_down["MSE"] = self.__calk_mean_z([[n_s[1][0], n_s[1][1]],
                                                                               [n_s[2][0], n_s[2][1]]])
            cell.right_up["Z"], cell.right_up["MSE"] = self.__calk_mean_z([[n_s[1][1], n_s[1][2]],
                                                                           [n_s[2][1], n_s[2][2]]])
        self.logger.info(f"Расчет средних высот модели {self.model_name} завершен")

    def __get_cell_neighbour_structure(self, cell):
        """
        Создает структуру соседних ячеек относительно ячейки cell
        :param cell: чентральная ячейка относительно которой ищутся соседи
        :return: 3х3 масив с ячейками относительно ячейки cell
        """
        step = cell.voxel.step
        x0, y0 = cell.voxel.X + step / 2, cell.voxel.Y + step / 2
        neighbour_structure = [[(-step, -step), (-step, 0), (-step, step)],
                               [(0, -step), (0, 0), (0, step)],
                               [(step, -step), (step, 0), (step, step)]]
        for x in range(3):
            for y in range(3):
                dx, dy = neighbour_structure[x][y]
                point = Point(X=x0 + dx, Y=y0 + dy, Z=0, R=0, G=0, B=0)
                try:
                    cell = self.get_model_element_for_point(point)
                    neighbour_structure[x][y] = cell
                except KeyError:
                    neighbour_structure[x][y] = None
        return neighbour_structure

    def __calk_mean_z(self, n_s):
        """
        Определяет логику расчета средней отметки в вершине ячейки относительно того
        нужно ли учитывать СКП  поверхности в ячейках
        :param n_s: 2х2 масив ячеек для общей точки которых выполняется расчет
        :return: средняя высота и СКП точки
        """
        z, mse = self.__prepare_data_to_calk_mean_z(n_s)
        if self.__enable_mse:
            avr_z, mse = self.__calculate_weighted_average(z, mse)
        else:
            avr_z, mse = self.__calculate_average(z)
        return avr_z, mse

    @staticmethod
    def __prepare_data_to_calk_mean_z(n_s):
        """
        Возвращает данные для расчет высот и СКП
        :param n_s: 2х2 масив ячеек для общей точки которых выполняется расчет
        :return: списки высот и СКП общей точки
        """
        z, mse = [], []
        if n_s[0][0] is not None:
            z.append(n_s[0][0].cell.get_z_from_xy(n_s[0][0].voxel.X + n_s[0][0].voxel.step,
                                                  n_s[0][0].voxel.Y + n_s[0][0].voxel.step))
            mse.append(n_s[0][0].cell.get_mse_z_from_xy(n_s[0][0].voxel.X + n_s[0][0].voxel.step,
                                                        n_s[0][0].voxel.Y + n_s[0][0].voxel.step))
        if n_s[0][1] is not None:
            z.append(n_s[0][1].cell.get_z_from_xy(n_s[0][1].voxel.X + n_s[0][1].voxel.step,
                                                  n_s[0][1].voxel.Y))
            mse.append(n_s[0][1].cell.get_mse_z_from_xy(n_s[0][1].voxel.X + n_s[0][1].voxel.step,
                                                        n_s[0][1].voxel.Y))
        if n_s[1][0] is not None:
            z.append(n_s[1][0].cell.get_z_from_xy(n_s[1][0].voxel.X,
                                                  n_s[1][0].voxel.Y + n_s[1][0].voxel.step))
            mse.append(n_s[1][0].cell.get_mse_z_from_xy(n_s[1][0].voxel.X,
                                                        n_s[1][0].voxel.Y + n_s[1][0].voxel.step))
        if n_s[1][1] is not None:
            z.append(n_s[1][1].cell.get_z_from_xy(n_s[1][1].voxel.X,
                                                  n_s[1][1].voxel.Y))
            mse.append(n_s[1][1].cell.get_mse_z_from_xy(n_s[1][1].voxel.X,
                                                        n_s[1][1].voxel.Y))
        return z, mse

    @staticmethod
    def __calculate_weighted_average(z, mse):
        """
        Расчитывает среднюю отметку и СКП учитывая СКП в поверхностях ячеек
        :param z: список высот
        :param mse: список СКП
        :return: средняя высота и ее СКП
        """
        sum_p = 0
        sum_of_pz = None
        for idx, mse in enumerate(mse):
            try:
                p = 1 / (mse ** 2)
            except TypeError:
                continue
            except ZeroDivisionError:
                return z[idx], 0
            try:
                sum_of_pz += p * z[idx]
            except TypeError:
                sum_of_pz = p * z[idx]
            sum_p += p
        try:
            avr_z = sum_of_pz / sum_p
            mse = 1 / (sum_p ** 0.5)
        except TypeError:
            avr_z = None
            mse = None
        return avr_z, mse

    @staticmethod
    def __calculate_average(z):
        """
        Расчитывает среднюю отметку и СКП НЕ учитывая СКП поверхностях ячеек
        :param z: список высот
        :return: средняя высота, СКП = None
        """
        z = [el for el in z if z is not None]
        try:
            avr_z, mse = sum(z) / len(z), None
        except TypeError:
            avr_z, mse = None, None
        return avr_z, mse

    def _create_model_structure(self, element_class):
        """
        Создает структуру модели, учитывая тип базовой сегментированой модели
        :param element_class: Тип элемента базовой модели (ывбирается из словаря self.__base_models_classes)
        :return: None
        """
        base_segment_model = self.__base_models_classes[self.model_type](self.voxel_model)
        for cell in base_segment_model:
            try:
                voxel = cell.voxel
            except AttributeError:
                continue
            model_key = f"{voxel.X:.5f}_{voxel.Y:.5f}_{voxel.Z:.5f}"
            self._model_structure[model_key] = element_class(cell, self)

    def delete_model(self, db_connection=None):
        super().delete_model(db_connection)
        base_segment_model = self.__base_models_classes[self.model_type](self.voxel_model)
        base_segment_model.delete_model(db_connection)
        self.logger.info(f"Удаление модели {self.model_name} из БД завершено\n")


class PointFilterABC(ABC):
    logger = logging.getLogger(LOGGER)

    def __init__(self, scan):
        self.scan = scan

    def __scan_name_generator(self):
        return f"{self.scan.scan_name}_FB_{self.__class__.__name__}"

    @abstractmethod
    def _filter_logic(self, point):
        pass

    def filter_scan(self):
        self.__write_temp_points_scans_file(self.scan)
        with engine.connect() as db_connection:
            stmt = delete(Tables.points_scans_db_table) \
                .where(Tables.points_scans_db_table.c.scan_id == self.scan.id)
            db_connection.execute(stmt)
            db_connection.commit()
            for data in self.__parse_temp_points_scans_file():
                db_connection.execute(Tables.points_scans_db_table.insert(), data)
                self.logger.info(f"Пакет отфильтрованных точек загружен в БД")
            db_connection.commit()
        update_scan_metrics(self.scan)
        update_scan_in_db_from_scan(self.scan)
        return Scan(self.scan.scan_name)

    def __write_temp_points_scans_file(self, scan):
        """
        Записывает временный файл, определяющий связь точек со сканом вокселя
        :param scan: базовый скан
        :return: None
        Рассчитывает и записывает во временный файл пару id точки и скана в вокселе
        Обновляет занчения метрик скана и вокселя в который попадает текущая точка
        """
        with open("temp_file.txt", "w", encoding="UTF-8") as file:
            select_0 = select(Tables.points_scans_db_table).\
                where(and_(self.scan.id == Tables.points_scans_db_table.c.scan_id,
                           Tables.points_scans_db_table.c.is_active == False))
            with engine.connect() as db_connection:
                db_points_data = db_connection.execute(select_0).mappings()
                for row in db_points_data:
                    file.write(f"{row['point_id']}, {scan.id}, 0\n")
            for point in scan:
                if self._filter_logic(point) is True:
                    file.write(f"{point.id}, {scan.id}, 1\n")
                else:
                    file.write(f"{point.id}, {scan.id}, 0\n")

    @staticmethod
    def __parse_temp_points_scans_file():
        """
        Читает временный файл, определяющий связь точек со сканом вокселя,
        выдает пакетами данные для загрузки их в БД
        в конце удаляет временный файл
        :return: пакеты точек для загрузки в БД
        """
        points_scans = []
        try:
            with open("temp_file.txt", "r", encoding="UTF-8") as file:
                for line in file:
                    data = [int(p_ps) for p_ps in line.strip().split(",")]
                    points_scans.append({"point_id": data[0], "scan_id": data[1], "is_active": data[2]})
                    if len(points_scans) == POINTS_CHUNK_COUNT:
                        yield points_scans
                        points_scans = []
            yield points_scans
        finally:
            try:
                os.remove("temp_file.txt")
            except FileNotFoundError:
                pass


class PointFilterMaxV(PointFilterABC):

    def __init__(self, scan, dem_model, max_v=2):
        super().__init__(scan)
        self.dem_model = dem_model
        self.max_v = max_v

    def _filter_logic(self, point):
        cell = self.dem_model.get_model_element_for_point(point)
        if cell is None or cell.mse is None:
            return False
        try:
            cell_z = cell.get_z_from_xy(point.X, point.Y)
        except TypeError:
            return False
        v = point.Z - cell_z
        if v <= self.max_v:
            return True
        else:
            return False


class PointFilterMedian(PointFilterABC):

    def __init__(self, scan, dem_model, k_value=2.5):
        super().__init__(scan)
        self.dem_model = dem_model
        self.MSE = self.dem_model.mse_data
        self.median = self.__calk_median_mse()
        self.k_value = k_value

    def __calk_median_mse(self):
        cell_mse = [cell.mse for cell in self.dem_model if cell.mse is not None]
        return median(cell_mse)

    def _filter_logic(self, point):
        cell = self.dem_model.get_model_element_for_point(point)
        if cell is None or cell.mse is None:
            return False
        try:
            cell_z = cell.get_z_from_xy(point.X, point.Y)
        except TypeError:
            return False
        v = point.Z - cell_z
        if v <= self.median * self.k_value:
            return True
        else:
            return False


class GroundFilter:
    logger = logging.getLogger(LOGGER)

    def __init__(self, path, n, step, k_value, n_vm=4, max_v=1):
        create_db()
        self.path = Path(path)
        self.scan = self.create_scan()
        self.n = n
        self.step = step
        self.k_value = k_value
        self.max_v = max_v
        self.n_vm = n_vm
        self.voxels_models = self.create_voxel_models()

    def create_scan(self):
        scan = Scan(self.path.stem)
        scan.load_scan_from_file(str(self.path))
        return scan

    def create_voxel_models(self):
        voxels_models = []
        for n_vm in range(self.n_vm):
            delta = round(1 / self.n_vm * n_vm, 2)
            vm = VoxelModel(self.scan, self.step, dx=delta, dy=delta)
            voxels_models.append(vm)
        return voxels_models

    def filter_scan(self):
        base_dir = self.path.parent
        for idx in range(self.n):
            vm = self.voxels_models[idx % len(self.voxels_models)]
            dem_model = BiModel(vm, "DEM")
            pf = PointFilterMedian(self.scan, dem_model, self.k_value)
            self.write_mse(f"{os.path.join(base_dir, self.scan.scan_name)}_log.txt", pf, idx, vm)
            if pf.median * self.k_value < self.max_v:
                pf.filter_scan()
            else:
                PointFilterMaxV(self.scan, dem_model, self.max_v).filter_scan()
            dem_model.delete_model()
            yield 1
        self.save_ground_scan(self.scan, f"{os.path.join(base_dir, self.scan.scan_name)}_ground_points.txt")
        self.save_not_ground_scan(self.scan, f"{os.path.join(base_dir, self.scan.scan_name)}_not_ground_points.txt")
        engine.dispose()
        os.remove(os.path.join(".", DATABASE_NAME))
        yield 1

    def write_mse(self, path, pfm, n, vm):
        with open(path, "a", encoding="utf-8") as file:
            data = f"№:{n+1}\tvm_name:{vm.vm_name}\tscan_len:{self.scan.len}\tMSE:{pfm.MSE:.4f}\tMedian:{pfm.median:.4f}\n"
            file.write(data)

    def save_ground_scan(self, scan, file_name):
        select_ = select(Tables.points_db_table). \
            join(Tables.points_scans_db_table, Tables.points_scans_db_table.c.point_id == Tables.points_db_table.c.id). \
            where(and_(self.scan.id == Tables.points_scans_db_table.c.scan_id,
                       Tables.points_scans_db_table.c.is_active == True))
        with open(file_name, "w", encoding="UTF-8") as file:
            with engine.connect() as db_connection:
                db_points_data = db_connection.execute(select_)
                for row in db_points_data:
                    point = Point.parse_point_from_db_row(row)
                    point_line = f"{point.X} {point.Y} {point.Z} {point.R} {point.G} {point.B}\n"
                    file.write(point_line)
        self.logger.info(f"Сохранение скана {scan} в файл {file_name} завершено")

    def save_not_ground_scan(self, scan, file_name):
        select_ = select(Tables.points_db_table). \
            join(Tables.points_scans_db_table, Tables.points_scans_db_table.c.point_id == Tables.points_db_table.c.id). \
            where(and_(self.scan.id == Tables.points_scans_db_table.c.scan_id,
                       Tables.points_scans_db_table.c.is_active == False))
        with open(file_name, "w", encoding="UTF-8") as file:
            with engine.connect() as db_connection:
                db_points_data = db_connection.execute(select_)
                for row in db_points_data:
                    point = Point.parse_point_from_db_row(row)
                    point_line = f"{point.X} {point.Y} {point.Z} {point.R} {point.G} {point.B}\n"
                    file.write(point_line)
        self.logger.info(f"Сохранение скана {scan} в файл {file_name} завершено")


class UiRelief(QWidget):

    def __init__(self):
        super().__init__()
        self.setupUi()
        self.k = self.k_value_slider.value()
        self.n = self.n_counter_slider.value()
        self.step = self.grid_size_slider.value()
        self.filepath = None
        self.file_path_button.clicked.connect(self.open_file_dialog)
        self.file_path_text.textChanged.connect(self.filepath_from_text_line)
        self.k_value_slider.valueChanged.connect(self.sliders_update)
        self.n_counter_slider.valueChanged.connect(self.sliders_update)
        self.grid_size_slider.valueChanged.connect(self.sliders_update)
        self.start_button.clicked.connect(self.start_filtering)
        self.setWindowIcon(QIcon('icon.ico'))
        self.progress = 0

    def start_filtering(self):
        self.start_button.setEnabled(False)
        self.progress = 0
        self.progressBar.setProperty("value", 0)
        gf = GroundFilter(self.filepath, self.n, self.step, self.k)
        self.update_progress_bar()
        for _ in gf.filter_scan():
            self.update_progress_bar()
        dig = QMessageBox(self)
        dig.setWindowTitle("Result")
        dig.setText("Фильтрация скана завершена!")
        dig.setIcon(QMessageBox.Icon.Information)
        dig.exec()

    def update_progress_bar(self):
        self.progress += 1 / (self.n+2) * 100
        self.progressBar.setProperty("value", round(self.progress))

    def filepath_from_text_line(self):
        self.filepath = self.file_path_text.toPlainText()
        self.start_button.setEnabled(True)

    def open_file_dialog(self):
        filename, ok = QFileDialog.getOpenFileName(
            self,
            "Select a File",
            ".",
            "Scan (*.txt *.ascii)"
        )
        if filename:
            path = Path(filename)
            self.file_path_text.setText(str(filename))
            self.filepath = str(path)
            self.start_button.setEnabled(True)

    def sliders_update(self):
        self.k = self.k_value_slider.value()
        self.n = self.n_counter_slider.value()
        self.step = self.grid_size_slider.value()

    def setupUi(self):
        self.setObjectName("Relief")
        self.resize(564, 279)
        self.setMinimumSize(QtCore.QSize(494, 224))
        self.setMaximumSize(QtCore.QSize(16777215, 224))
        self.gridLayout_3 = QGridLayout(self)
        self.gridLayout_3.setObjectName("gridLayout_3")
        self.verticalLayout_4 = QVBoxLayout()
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.gridLayout_4 = QGridLayout()
        self.gridLayout_4.setObjectName("gridLayout_4")
        self.gridLayout_5 = QGridLayout()
        self.gridLayout_5.setObjectName("gridLayout_5")
        self.grid_size_slider = QSlider(parent=self)
        self.grid_size_slider.setMinimum(1)
        self.grid_size_slider.setMaximum(20)
        self.grid_size_slider.setProperty("value", 5)
        self.grid_size_slider.setOrientation(QtCore.Qt.Orientation.Horizontal)
        self.grid_size_slider.setObjectName("grid_size_slider")
        self.grid_size_slider.setTickPosition(QSlider.TickPosition.TicksAbove)
        self.grid_size_slider.setPageStep(1)

        self.gridLayout_5.addWidget(self.grid_size_slider, 1, 1, 1, 1)
        self.horizontalLayout_4 = QHBoxLayout()
        self.horizontalLayout_4.setObjectName("horizontalLayout_4")
        self.label_8 = QLabel(parent=self)
        self.label_8.setObjectName("label_8")
        self.horizontalLayout_4.addWidget(self.label_8)
        spacerItem = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding,
                                 QSizePolicy.Policy.Minimum)
        self.horizontalLayout_4.addItem(spacerItem)
        self.label_9 = QLabel(parent=self)
        self.label_9.setObjectName("label_9")
        self.horizontalLayout_4.addWidget(self.label_9)
        self.gridLayout_5.addLayout(self.horizontalLayout_4, 0, 1, 1, 1)
        self.gridLayout_4.addLayout(self.gridLayout_5, 1, 1, 1, 1)
        self.verticalLayout_5 = QVBoxLayout()
        self.verticalLayout_5.setObjectName("verticalLayout_5")
        spacerItem1 = QSpacerItem(0, 0, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout_5.addItem(spacerItem1)
        self.file_path_text = QTextEdit(parent=self)
        self.file_path_text.setMinimumSize(QtCore.QSize(0, 20))
        self.file_path_text.setMaximumSize(QtCore.QSize(16777215, 25))
        self.file_path_text.setObjectName("file_path_text")
        self.verticalLayout_5.addWidget(self.file_path_text)
        spacerItem2 = QSpacerItem(20, 10, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout_5.addItem(spacerItem2)
        self.gridLayout_4.addLayout(self.verticalLayout_5, 0, 1, 1, 1)
        self.label_10 = QLabel(parent=self)
        self.label_10.setAlignment(
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignTrailing | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.label_10.setObjectName("label_10")
        self.gridLayout_4.addWidget(self.label_10, 1, 0, 1, 1)
        self.label_3 = QLabel(parent=self)
        self.label_3.setAlignment(
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignTrailing | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.label_3.setObjectName("label_3")
        self.gridLayout_4.addWidget(self.label_3, 3, 0, 1, 1)
        self.file_path_button = QToolButton(parent=self)
        self.file_path_button.setObjectName("file_path_button")
        self.gridLayout_4.addWidget(self.file_path_button, 0, 2, 1, 1)
        self.gridLayout_2 = QGridLayout()
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.k_value_slider = QSlider(parent=self)
        self.k_value_slider.setMinimum(1)
        self.k_value_slider.setMaximum(6)
        self.k_value_slider.setProperty("value", 4)
        self.k_value_slider.setOrientation(QtCore.Qt.Orientation.Horizontal)
        self.k_value_slider.setObjectName("k_value_slider")
        self.k_value_slider.setTickPosition(QSlider.TickPosition.TicksAbove)
        self.k_value_slider.setPageStep(1)

        self.gridLayout_2.addWidget(self.k_value_slider, 1, 1, 1, 1)
        self.horizontalLayout_2 = QHBoxLayout()
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.label_4 = QLabel(parent=self)
        self.label_4.setObjectName("label_4")
        self.horizontalLayout_2.addWidget(self.label_4)
        spacerItem3 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding,
                                  QSizePolicy.Policy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem3)
        self.label_5 = QLabel(parent=self)
        self.label_5.setObjectName("label_5")
        self.horizontalLayout_2.addWidget(self.label_5)
        self.gridLayout_2.addLayout(self.horizontalLayout_2, 0, 1, 1, 1)
        self.gridLayout_4.addLayout(self.gridLayout_2, 2, 1, 1, 1)
        self.gridLayout = QGridLayout()
        self.gridLayout.setObjectName("gridLayout")
        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label = QLabel(parent=self)
        self.label.setObjectName("label")
        self.horizontalLayout.addWidget(self.label)
        spacerItem4 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding,
                                  QSizePolicy.Policy.Minimum)
        self.horizontalLayout.addItem(spacerItem4)
        self.label_2 = QLabel(parent=self)
        self.label_2.setObjectName("label_2")
        self.horizontalLayout.addWidget(self.label_2)
        self.gridLayout.addLayout(self.horizontalLayout, 0, 1, 1, 1)
        self.n_counter_slider = QSlider(parent=self)
        self.n_counter_slider.setMinimum(1)
        self.n_counter_slider.setMaximum(30)
        self.n_counter_slider.setProperty("value", 10)
        self.n_counter_slider.setSliderPosition(10)
        self.n_counter_slider.setOrientation(QtCore.Qt.Orientation.Horizontal)
        self.n_counter_slider.setObjectName("n_counter_slider")
        self.n_counter_slider.setTickPosition(QSlider.TickPosition.TicksAbove)
        self.n_counter_slider.setPageStep(1)

        self.gridLayout.addWidget(self.n_counter_slider, 1, 1, 1, 1)
        self.gridLayout_4.addLayout(self.gridLayout, 3, 1, 1, 1)
        self.label_6 = QLabel(parent=self)
        self.label_6.setAlignment(
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignTrailing | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.label_6.setObjectName("label_6")
        self.gridLayout_4.addWidget(self.label_6, 2, 0, 1, 1)
        self.label_7 = QLabel(parent=self)
        self.label_7.setMaximumSize(QtCore.QSize(16777215, 100))
        self.label_7.setAlignment(
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignTrailing | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.label_7.setObjectName("label_7")
        self.gridLayout_4.addWidget(self.label_7, 0, 0, 1, 1)
        self.verticalLayout = QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        spacerItem5 = QSpacerItem(20, 10, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout.addItem(spacerItem5)
        self.n_slider_box = QSpinBox(parent=self)
        self.n_slider_box.setMinimum(1)
        self.n_slider_box.setMaximum(30)
        self.n_slider_box.setProperty("value", 10)
        self.n_slider_box.setObjectName("n_slider_box")
        self.verticalLayout.addWidget(self.n_slider_box)
        self.gridLayout_4.addLayout(self.verticalLayout, 3, 2, 1, 1)
        self.verticalLayout_2 = QVBoxLayout()
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        spacerItem6 = QSpacerItem(20, 10, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout_2.addItem(spacerItem6)
        self.k_value_box = QSpinBox(parent=self)
        self.k_value_box.setMinimum(1)
        self.k_value_box.setMaximum(6)
        self.k_value_box.setProperty("value", 4)
        self.k_value_box.setObjectName("k_value_box")
        self.verticalLayout_2.addWidget(self.k_value_box)
        self.gridLayout_4.addLayout(self.verticalLayout_2, 2, 2, 1, 1)
        self.verticalLayout_3 = QVBoxLayout()
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        spacerItem7 = QSpacerItem(20, 10, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout_3.addItem(spacerItem7)
        self.grid_size_box = QSpinBox(parent=self)
        self.grid_size_box.setMinimum(1)
        self.grid_size_box.setMaximum(20)
        self.grid_size_box.setProperty("value", 5)
        self.grid_size_box.setObjectName("grid_size_box")
        self.verticalLayout_3.addWidget(self.grid_size_box)
        self.gridLayout_4.addLayout(self.verticalLayout_3, 1, 2, 1, 1)
        self.verticalLayout_4.addLayout(self.gridLayout_4)
        self.horizontalLayout_3 = QHBoxLayout()
        self.horizontalLayout_3.setObjectName("horizontalLayout_3")
        self.progressBar = QProgressBar(parent=self)
        self.progressBar.setProperty("value", 0)
        self.progressBar.setTextVisible(True)
        self.progressBar.setObjectName("progressBar")

        self.horizontalLayout_3.addWidget(self.progressBar)
        self.start_button = QPushButton(parent=self)
        self.start_button.setStyleSheet("background-color: rgb(170, 255, 127);")
        self.start_button.setFlat(False)
        self.start_button.setObjectName("start_button")
        self.start_button.setEnabled(False)
        self.horizontalLayout_3.addWidget(self.start_button)
        self.verticalLayout_4.addLayout(self.horizontalLayout_3)
        self.gridLayout_3.addLayout(self.verticalLayout_4, 0, 0, 1, 1)
        self.retranslateUi()
        self.n_counter_slider.valueChanged['int'].connect(self.n_slider_box.setValue)  # type: ignore
        self.n_slider_box.valueChanged['int'].connect(self.n_counter_slider.setValue)  # type: ignore
        self.k_value_slider.valueChanged['int'].connect(self.k_value_box.setValue)  # type: ignore
        self.k_value_box.valueChanged['int'].connect(self.k_value_slider.setValue)  # type: ignore
        self.grid_size_slider.valueChanged['int'].connect(self.grid_size_box.setValue)  # type: ignore
        self.grid_size_box.valueChanged['int'].connect(self.grid_size_slider.setValue)  # type: ignore
        QtCore.QMetaObject.connectSlotsByName(self)

    def retranslateUi(self):
        _translate = QtCore.QCoreApplication.translate
        self.setWindowTitle(_translate("Relief", "Relief"))
        self.label_8.setText(_translate("Relief", "Меньше"))
        self.label_9.setText(_translate("Relief", "Больше"))
        self.label_10.setText(_translate("Relief", "Размер\n"
                                                   "ячейки, м:"))
        self.label_3.setText(_translate("Relief", "Количество\n"
                                                  "иттераций:"))
        self.file_path_button.setText(_translate("Relief", "..."))
        self.label_4.setText(_translate("Relief", "Агрессивно"))
        self.label_5.setText(_translate("Relief", "Аккуратно"))
        self.label.setText(_translate("Relief", "Быстро"))
        self.label_2.setText(_translate("Relief", "Долго"))
        self.label_6.setText(_translate("Relief", "Интенсивность\n"
                                                  "фильтрации:"))
        self.label_7.setText(_translate("Relief", "Фильтруемый\n"
                                                  "скан:"))
        self.start_button.setText(_translate("Relief", "Запуск фильтрации"))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    ui = UiRelief()
    ui.show()
    sys.exit(app.exec())
