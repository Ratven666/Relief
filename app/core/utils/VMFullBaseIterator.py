
class VMFullBaseIterator:
    """
    Иттератор полной воксельной модели
    """
    def __init__(self, vxl_mdl):
        self.vxl_mdl = vxl_mdl
        self.x = 0
        self.y = 0
        self.z = 0
        self.X_count, self.Y_count, self.Z_count = vxl_mdl.X_count, vxl_mdl.Y_count, vxl_mdl.Z_count

    def __iter__(self):
        return self

    def __next__(self):
        for vxl_z in range(self.z, self.Z_count):
            for vxl_y in range(self.y, self.Y_count):
                for vxl_x in range(self.x, self.X_count):
                    self.x += 1
                    return self.vxl_mdl.voxel_structure[vxl_z][vxl_y][vxl_x]
                self.y += 1
                self.x = 0
            self.z += 1
            self.y = 0
        raise StopIteration
