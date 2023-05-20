from sqlalchemy import update

from app.core.db.start_db import Tables, engine


def update_voxel_model_in_db_from_voxel_model(updated_voxel_model, db_connection=None):
    """
    Обновляет значения метрик воксельной модели в БД
    :param updated_voxel_model: Объект воксельной модели для которой обновляются метрики
    :param db_connection: Открытое соединение с БД
    :return: None
    """
    stmt = update(Tables.voxel_models_db_table) \
        .where(Tables.voxel_models_db_table.c.id == updated_voxel_model.id) \
        .values(id=updated_voxel_model.id,
                vm_name=updated_voxel_model.vm_name,
                step=updated_voxel_model.step,
                len=updated_voxel_model.len,
                X_count=updated_voxel_model.X_count,
                Y_count=updated_voxel_model.Y_count,
                Z_count=updated_voxel_model.Z_count,
                min_X=updated_voxel_model.min_X,
                max_X=updated_voxel_model.max_X,
                min_Y=updated_voxel_model.min_Y,
                max_Y=updated_voxel_model.max_Y,
                min_Z=updated_voxel_model.min_Z,
                max_Z=updated_voxel_model.max_Z,
                base_scan_id=updated_voxel_model.base_scan_id)
    if db_connection is None:
        with engine.connect() as db_connection:
            db_connection.execute(stmt)
            db_connection.commit()
    else:
        db_connection.execute(stmt)
        db_connection.commit()
