import logging
from abc import ABC, abstractmethod

from sqlalchemy import select, desc, update, insert, and_, delete

from app.core.CONFIG import LOGGER
from app.core.db.start_db import Tables, engine


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
