class Point:
    """
    Класс точки
    """
    __slots__ = ["id", "X", "Y", "Z", "R", "G", "B"]

    def __init__(self, X, Y, Z, R=0, G=0, B=0, id_=None):
        self.id = id_
        self.X, self.Y, self.Z = X, Y, Z
        self.R, self.G, self.B = R, G, B

    def __str__(self):
        return f"{self.__class__.__name__} " \
               f"[id: {self.id},\tx: {self.X} y: {self.Y} z: {self.Z},\t" \
               f"RGB: ({self.R},{self.G},{self.B})]"

    def __repr__(self):
        return f"{self.__class__.__name__} [id: {self.id}]"

    @classmethod
    def parse_point_from_db_row(cls, row: tuple):
        """
        Метод который создает и возвращает объект Point по данным читаемым из БД
        :param row: кортеж данных читаемых из БД
        :return: объект класса Point
        """
        return cls(id_=row[0], X=row[1], Y=row[2], Z=row[3], R=row[4], G=row[5], B=row[6])
