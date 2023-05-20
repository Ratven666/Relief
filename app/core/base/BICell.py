from sqlalchemy import insert

from app.core.base.CellABC import CellABC
from app.core.db.start_db import Tables


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
