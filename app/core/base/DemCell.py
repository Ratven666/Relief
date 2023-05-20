from sqlalchemy import insert

from app.core.base.CellABC import CellABC
from app.core.db.start_db import Tables


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
