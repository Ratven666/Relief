from sqlalchemy import and_, select, update
from sqlalchemy import func

from app.core.db.start_db import Tables, engine


def calk_scan_metrics(scan_id):
    """
    Рассчитывает метрики скана средствами SQL
    :param scan_id: id скана для которого будет выполняться расчет метрик
    :return: словарь с метриками скана
    """
    with engine.connect() as db_connection:
        stmt = select(func.count(Tables.points_db_table.c.id).label("len"),
                      func.min(Tables.points_db_table.c.X).label("min_X"),
                      func.max(Tables.points_db_table.c.X).label("max_X"),
                      func.min(Tables.points_db_table.c.Y).label("min_Y"),
                      func.max(Tables.points_db_table.c.Y).label("max_Y"),
                      func.min(Tables.points_db_table.c.Z).label("min_Z"),
                      func.max(Tables.points_db_table.c.Z).label("max_Z")).where(and_(
                               Tables.points_scans_db_table.c.point_id == Tables.points_db_table.c.id,
                               Tables.points_scans_db_table.c.scan_id == Tables.scans_db_table.c.id,
                               Tables.points_scans_db_table.c.is_active == 1,
                               Tables.scans_db_table.c.id == scan_id
                      ))
        scan_metrics = dict(db_connection.execute(stmt).mappings().first())
        scan_metrics["id"] = scan_id
        return scan_metrics


def update_scan_metrics(scan):
    """
    Рассчитывает значения метрик скана по точкам загруженным в БД
    средствами SQL и обновляет их в самом скане
    :param scan: скан для которого рассчитываются и в котором обновляются метрики
    :return: скан с обновленными  метриками
    """
    scan_metrics = calk_scan_metrics(scan_id=scan.id)
    scan.len = scan_metrics["len"]
    scan.min_X, scan.max_X = scan_metrics["min_X"], scan_metrics["max_X"]
    scan.min_Y, scan.max_Y = scan_metrics["min_Y"], scan_metrics["max_Y"]
    scan.min_Z, scan.max_Z = scan_metrics["min_Z"], scan_metrics["max_Z"]
    return scan


def update_scan_in_db_from_scan_metrics(scan_metrics: dict):
    """
    Обновляет значения метрик скана в БД
    :param scan_metrics: словарь с метриками скана
    :return: None
    """
    with engine.connect() as db_connection:
        stmt = update(Tables.scans_db_table) \
            .where(Tables.scans_db_table.c.id == scan_metrics["id"]) \
            .values(scan_name=scan_metrics["scan_name"],
                    len=scan_metrics["len"],
                    min_X=scan_metrics["min_X"],
                    max_X=scan_metrics["max_X"],
                    min_Y=scan_metrics["min_Y"],
                    max_Y=scan_metrics["max_Y"],
                    min_Z=scan_metrics["min_Z"],
                    max_Z=scan_metrics["max_Z"])
        db_connection.execute(stmt)
        db_connection.commit()


def update_scan_in_db_from_scan(updated_scan, db_connection=None):
    """
    Обновляет значения метрик скана в БД
    :param updated_scan: Объект скана для которого обновляются метрики
    :param db_connection: Открытое соединение с БД
    :return: None
    """
    stmt = update(Tables.scans_db_table) \
        .where(Tables.scans_db_table.c.id == updated_scan.id) \
        .values(scan_name=updated_scan.scan_name,
                len=updated_scan.len,
                min_X=updated_scan.min_X,
                max_X=updated_scan.max_X,
                min_Y=updated_scan.min_Y,
                max_Y=updated_scan.max_Y,
                min_Z=updated_scan.min_Z,
                max_Z=updated_scan.max_Z)
    if db_connection is None:
        with engine.connect() as db_connection:
            db_connection.execute(stmt)
            db_connection.commit()
    else:
        db_connection.execute(stmt)
        db_connection.commit()


def update_scan_borders(scan, point):
    """
    Проверяет положение в точки в существующих границах скана
    и меняет их при выходе точки за их пределы
    :param scan: скан
    :param point: точка
    :return: None
    """
    if scan.min_X is None:
        scan.min_X, scan.max_X = point.X, point.X
        scan.min_Y, scan.max_Y = point.Y, point.Y
        scan.min_Z, scan.max_Z = point.Z, point.Z
    if point.X < scan.min_X:
        scan.min_X = point.X
    if point.X > scan.max_X:
        scan.max_X = point.X
    if point.Y < scan.min_Y:
        scan.min_Y = point.Y
    if point.Y > scan.max_Y:
        scan.max_Y = point.Y
    if point.Z < scan.min_Z:
        scan.min_Z = point.Z
    if point.Z > scan.max_Z:
        scan.max_Z = point.Z
