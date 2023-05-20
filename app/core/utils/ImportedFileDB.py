from sqlalchemy import select, and_, insert

from app.core.db.start_db import Tables, engine


class ImportedFileDB:
    """
    Класс определяющий логику контроля повторной загрузки файла с данными
    """

    def __init__(self, file_name):
        self.__file_name = file_name
        self.__hash = None

    def is_file_already_imported_into_scan(self, scan):
        """
        Проверяет был ли этот файл уже загружен в скан

        :param scan: скан в который загружаются данные из файла
        :type scan: ScanDB
        :return: True / False
        """
        select_ = select(Tables.imported_files_db_table).where(
            and_(Tables.imported_files_db_table.c.file_name == self.__file_name,
                 Tables.imported_files_db_table.c.scan_id == scan.id))
        with engine.connect() as db_connection:
            imp_file = db_connection.execute(select_).first()
        if imp_file is None:
            return False
        return True

    def insert_in_db(self, scan):
        """
        Добавляет в таблицу БД imported_files данные о файле и скане в который он был загружен
        :param scan: скан в который загружаются данные из файла
        :type scan: ScanDB
        :return: None
        """
        with engine.connect() as db_connection:
            stmt = insert(Tables.imported_files_db_table).values(file_name=self.__file_name,
                                                                 scan_id=scan.id)
            db_connection.execute(stmt)
            db_connection.commit()
