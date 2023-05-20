import logging

from app.core.CONFIG import LOGGER
from app.core.db.start_db import engine, Tables
from app.core.utils.ImportedFileDB import ImportedFileDB
from app.core.utils.ScanParserABC import ScanParserABC
from app.core.utils.ScanTxtParser import ScanTxtParser
from app.core.utils.Scan_metrics import update_scan_metrics, update_scan_in_db_from_scan


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
