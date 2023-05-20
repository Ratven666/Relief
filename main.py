from PyQt6.QtWidgets import QApplication

from app.interface import UiRelief

if __name__ == "__main__":
    import sys

    app = QApplication(sys.argv)
    ui = UiRelief()
    ui.show()
    sys.exit(app.exec())
