from app.core.CONFIG import POINTS_CHUNK_COUNT
from app.core.utils.ScanParserABC import ScanParserABC


class ScanTxtParser(ScanParserABC):
    """
    Парсер точек из текстового txt формата
    Формат данных:
        4.2517 -14.2273 33.4113 208 195 182 -0.023815 -0.216309 0.976035
          X        Y       Z     R   G   B      nX nY nZ (не обязательны и пока игнорируются)
    """
    __supported_file_extension__ = [".txt"]

    def __init__(self, chunk_count=POINTS_CHUNK_COUNT):
        self.__chunk_count = chunk_count
        self.__last_point_id = None

    def parse(self, file_name):
        """
        Запускает процедуру парсинга файла и возвращает списки словарей с данными для загрузки в БД
        размером не превышающим POINTS_CHUNK_COUNT
        При запуске выполняется процедурка проверки расширения файла
        :param file_name: путь до файла из которго будут загружаться данные
        :return: список точек готовый к загрузке в БД
        """
        self._check_file_extension(file_name, self.__supported_file_extension__)
        self.__last_point_id = self._get_last_point_id()

        with open(file_name, "rt", encoding="utf-8") as file:
            points = []
            for line in file:
                line = line.strip().split()
                self.__last_point_id += 1
                try:
                    if len(line) == 3:
                        point = {"id": self.__last_point_id,
                                 "X": line[0], "Y": line[1], "Z": line[2],
                                 }
                    elif len(line) == 6 or len(line) == 9:
                        point = {"id": self.__last_point_id,
                                 "X": line[0], "Y": line[1], "Z": line[2],
                                 "R": line[3], "G": line[4], "B": line[5]
                                 }
                    elif len(line) == 7:
                        point = {"id": self.__last_point_id,
                                 "X": line[0], "Y": line[1], "Z": line[2],
                                 "R": line[3], "G": line[4], "B": line[5],
                                 "is_ground": line[6]
                                 }
                except IndexError:
                    self.logger.critical(f"Структура \"{file_name}\" некорректна - \"{line}\"")
                    return
                points.append(point)
                if len(points) == self.__chunk_count:
                    yield points
                    points = []
            yield points
