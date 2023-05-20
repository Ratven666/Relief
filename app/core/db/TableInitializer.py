from threading import Lock

from sqlalchemy import Table, Column, Integer, Float, String, Boolean, ForeignKey


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
