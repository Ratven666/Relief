from sqlalchemy import select, desc

from app.core.base.Voxel import VoxelLite
from app.core.db.start_db import Tables, engine
from app.core.utils.VMFullBaseIterator import VMFullBaseIterator
from app.core.utils.Voxel_model_metrics import update_voxel_model_in_db_from_voxel_model


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
