from statistics import median

from app.core.filters.PointFilterABC import PointFilterABC


class PointFilterMedian(PointFilterABC):

    def __init__(self, scan, dem_model, k_value=2.5):
        super().__init__(scan)
        self.dem_model = dem_model
        self.MSE = self.dem_model.mse_data
        self.median = self.__calk_median_mse()
        self.k_value = k_value

    def __calk_median_mse(self):
        cell_mse = [cell.mse for cell in self.dem_model if cell.mse is not None]
        return median(cell_mse)

    def _filter_logic(self, point):
        cell = self.dem_model.get_model_element_for_point(point)
        if cell is None or cell.mse is None:
            return False
        try:
            cell_z = cell.get_z_from_xy(point.X, point.Y)
        except TypeError:
            return False
        v = point.Z - cell_z
        if v <= self.median * self.k_value:
            return True
        else:
            return False
