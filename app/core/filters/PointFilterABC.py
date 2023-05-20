import logging
from abc import ABC, abstractmethod
from os import remove

from sqlalchemy import delete, and_, select

from app.core.CONFIG import LOGGER, POINTS_CHUNK_COUNT
from app.core.base.Point import Point
from app.core.base.Scan import Scan
from app.core.db.start_db import Tables, engine
from app.core.utils.Scan_metrics import update_scan_in_db_from_scan, update_scan_metrics


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
                remove("temp_file.txt")
            except FileNotFoundError:
                pass
