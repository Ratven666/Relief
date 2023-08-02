import logging
import os
from pathlib import Path

from sqlalchemy import and_, select

from app.core.CONFIG import DATABASE_NAME, LOGGER
from app.core.base.Point import Point
from app.core.base.Scan import Scan
from app.core.db.start_db import create_db, engine, Tables
from app.core.filters.PointFilterByMaxV import PointFilterMaxV
from app.core.filters.PointFilterMedian import PointFilterMedian
from app.core.models.BIModel import BiModel
from app.core.models.VoxelModel import VoxelModel


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


if __name__ == "__main__":
    gf = GroundFilter(r"C:\Users\Mikhail Vystrchil\Desktop\Articles\Relief\src\forest_05.txt", 10, 5, 4)
    gf.filter_scan()


