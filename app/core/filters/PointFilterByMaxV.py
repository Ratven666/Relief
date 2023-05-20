from app.core.filters.PointFilterABC import PointFilterABC


class PointFilterMaxV(PointFilterABC):

    def __init__(self, scan, dem_model, max_v=2):
        super().__init__(scan)
        self.dem_model = dem_model
        self.max_v = max_v

    def _filter_logic(self, point):
        cell = self.dem_model.get_model_element_for_point(point)
        if cell is None or cell.mse is None:
            return False
        try:
            cell_z = cell.get_z_from_xy(point.X, point.Y)
        except TypeError:
            return False
        v = point.Z - cell_z
        if v <= self.max_v:
            return True
        else:
            return False
