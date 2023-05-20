from abc import ABC, abstractmethod

from sqlalchemy import select, and_


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
