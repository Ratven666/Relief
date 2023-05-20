from sqlalchemy import select

from app.core.base.Voxel import VoxelLite
from app.core.db.start_db import engine, Tables


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
