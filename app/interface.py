from pathlib import Path

from PyQt6 import QtCore
from PyQt6.QtWidgets import QWidget, QFileDialog, QSlider, QMessageBox, QLabel, QGridLayout, QVBoxLayout, QSpacerItem, \
    QTextEdit, QToolButton, QHBoxLayout, QSpinBox, QProgressBar, QPushButton, QSizePolicy

from app.GroundFilter import GroundFilter


class UiRelief(QWidget):

    def __init__(self):
        super().__init__()
        self.setupUi()
        self.k = self.k_value_slider.value()
        self.n = self.n_counter_slider.value()
        self.step = self.grid_size_slider.value()
        self.filepath = None
        self.file_path_button.clicked.connect(self.open_file_dialog)
        self.file_path_text.textChanged.connect(self.filepath_from_text_line)
        self.k_value_slider.valueChanged.connect(self.sliders_update)
        self.n_counter_slider.valueChanged.connect(self.sliders_update)
        self.grid_size_slider.valueChanged.connect(self.sliders_update)
        self.start_button.clicked.connect(self.start_filtering)
        self.progress = 0

    def start_filtering(self):
        self.start_button.setEnabled(False)
        self.progress = 0
        self.progressBar.setProperty("value", 0)
        gf = GroundFilter(self.filepath, self.n, self.step, self.k)
        self.update_progress_bar()
        for _ in gf.filter_scan():
            self.update_progress_bar()
        dig = QMessageBox(self)
        dig.setWindowTitle("Result")
        dig.setText("Фильтрация скана завершена!")
        dig.setIcon(QMessageBox.Icon.Information)
        dig.exec()

    def update_progress_bar(self):
        self.progress += 1 / (self.n+2) * 100
        self.progressBar.setProperty("value", round(self.progress))

    def filepath_from_text_line(self):
        self.filepath = self.file_path_text.toPlainText()
        self.start_button.setEnabled(True)

    def open_file_dialog(self):
        filename, ok = QFileDialog.getOpenFileName(
            self,
            "Select a File",
            ".",
            "Scan (*.txt *.ascii)"
        )
        if filename:
            path = Path(filename)
            self.file_path_text.setText(str(filename))
            self.filepath = str(path)
            self.start_button.setEnabled(True)

    def sliders_update(self):
        self.k = self.k_value_slider.value()
        self.n = self.n_counter_slider.value()
        self.step = self.grid_size_slider.value()

    def setupUi(self):
        self.setObjectName("Relief")
        self.resize(564, 279)
        self.setMinimumSize(QtCore.QSize(494, 224))
        self.setMaximumSize(QtCore.QSize(16777215, 224))
        self.gridLayout_3 = QGridLayout(self)
        self.gridLayout_3.setObjectName("gridLayout_3")
        self.verticalLayout_4 = QVBoxLayout()
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.gridLayout_4 = QGridLayout()
        self.gridLayout_4.setObjectName("gridLayout_4")
        self.gridLayout_5 = QGridLayout()
        self.gridLayout_5.setObjectName("gridLayout_5")
        self.grid_size_slider = QSlider(parent=self)
        self.grid_size_slider.setMinimum(1)
        self.grid_size_slider.setMaximum(20)
        self.grid_size_slider.setProperty("value", 5)
        self.grid_size_slider.setOrientation(QtCore.Qt.Orientation.Horizontal)
        self.grid_size_slider.setObjectName("grid_size_slider")
        self.grid_size_slider.setTickPosition(QSlider.TickPosition.TicksAbove)
        self.grid_size_slider.setPageStep(1)

        self.gridLayout_5.addWidget(self.grid_size_slider, 1, 1, 1, 1)
        self.horizontalLayout_4 = QHBoxLayout()
        self.horizontalLayout_4.setObjectName("horizontalLayout_4")
        self.label_8 = QLabel(parent=self)
        self.label_8.setObjectName("label_8")
        self.horizontalLayout_4.addWidget(self.label_8)
        spacerItem = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding,
                                 QSizePolicy.Policy.Minimum)
        self.horizontalLayout_4.addItem(spacerItem)
        self.label_9 = QLabel(parent=self)
        self.label_9.setObjectName("label_9")
        self.horizontalLayout_4.addWidget(self.label_9)
        self.gridLayout_5.addLayout(self.horizontalLayout_4, 0, 1, 1, 1)
        self.gridLayout_4.addLayout(self.gridLayout_5, 1, 1, 1, 1)
        self.verticalLayout_5 = QVBoxLayout()
        self.verticalLayout_5.setObjectName("verticalLayout_5")
        spacerItem1 = QSpacerItem(0, 0, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout_5.addItem(spacerItem1)
        self.file_path_text = QTextEdit(parent=self)
        self.file_path_text.setMinimumSize(QtCore.QSize(0, 20))
        self.file_path_text.setMaximumSize(QtCore.QSize(16777215, 25))
        self.file_path_text.setObjectName("file_path_text")
        self.verticalLayout_5.addWidget(self.file_path_text)
        spacerItem2 = QSpacerItem(20, 10, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout_5.addItem(spacerItem2)
        self.gridLayout_4.addLayout(self.verticalLayout_5, 0, 1, 1, 1)
        self.label_10 = QLabel(parent=self)
        self.label_10.setAlignment(
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignTrailing | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.label_10.setObjectName("label_10")
        self.gridLayout_4.addWidget(self.label_10, 1, 0, 1, 1)
        self.label_3 = QLabel(parent=self)
        self.label_3.setAlignment(
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignTrailing | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.label_3.setObjectName("label_3")
        self.gridLayout_4.addWidget(self.label_3, 3, 0, 1, 1)
        self.file_path_button = QToolButton(parent=self)
        self.file_path_button.setObjectName("file_path_button")
        self.gridLayout_4.addWidget(self.file_path_button, 0, 2, 1, 1)
        self.gridLayout_2 = QGridLayout()
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.k_value_slider = QSlider(parent=self)
        self.k_value_slider.setMinimum(1)
        self.k_value_slider.setMaximum(6)
        self.k_value_slider.setProperty("value", 4)
        self.k_value_slider.setOrientation(QtCore.Qt.Orientation.Horizontal)
        self.k_value_slider.setObjectName("k_value_slider")
        self.k_value_slider.setTickPosition(QSlider.TickPosition.TicksAbove)
        self.k_value_slider.setPageStep(1)

        self.gridLayout_2.addWidget(self.k_value_slider, 1, 1, 1, 1)
        self.horizontalLayout_2 = QHBoxLayout()
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.label_4 = QLabel(parent=self)
        self.label_4.setObjectName("label_4")
        self.horizontalLayout_2.addWidget(self.label_4)
        spacerItem3 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding,
                                  QSizePolicy.Policy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem3)
        self.label_5 = QLabel(parent=self)
        self.label_5.setObjectName("label_5")
        self.horizontalLayout_2.addWidget(self.label_5)
        self.gridLayout_2.addLayout(self.horizontalLayout_2, 0, 1, 1, 1)
        self.gridLayout_4.addLayout(self.gridLayout_2, 2, 1, 1, 1)
        self.gridLayout = QGridLayout()
        self.gridLayout.setObjectName("gridLayout")
        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label = QLabel(parent=self)
        self.label.setObjectName("label")
        self.horizontalLayout.addWidget(self.label)
        spacerItem4 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding,
                                  QSizePolicy.Policy.Minimum)
        self.horizontalLayout.addItem(spacerItem4)
        self.label_2 = QLabel(parent=self)
        self.label_2.setObjectName("label_2")
        self.horizontalLayout.addWidget(self.label_2)
        self.gridLayout.addLayout(self.horizontalLayout, 0, 1, 1, 1)
        self.n_counter_slider = QSlider(parent=self)
        self.n_counter_slider.setMinimum(1)
        self.n_counter_slider.setMaximum(30)
        self.n_counter_slider.setProperty("value", 10)
        self.n_counter_slider.setSliderPosition(10)
        self.n_counter_slider.setOrientation(QtCore.Qt.Orientation.Horizontal)
        self.n_counter_slider.setObjectName("n_counter_slider")
        self.n_counter_slider.setTickPosition(QSlider.TickPosition.TicksAbove)
        self.n_counter_slider.setPageStep(1)

        self.gridLayout.addWidget(self.n_counter_slider, 1, 1, 1, 1)
        self.gridLayout_4.addLayout(self.gridLayout, 3, 1, 1, 1)
        self.label_6 = QLabel(parent=self)
        self.label_6.setAlignment(
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignTrailing | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.label_6.setObjectName("label_6")
        self.gridLayout_4.addWidget(self.label_6, 2, 0, 1, 1)
        self.label_7 = QLabel(parent=self)
        self.label_7.setMaximumSize(QtCore.QSize(16777215, 100))
        self.label_7.setAlignment(
            QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignTrailing | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.label_7.setObjectName("label_7")
        self.gridLayout_4.addWidget(self.label_7, 0, 0, 1, 1)
        self.verticalLayout = QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        spacerItem5 = QSpacerItem(20, 10, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout.addItem(spacerItem5)
        self.n_slider_box = QSpinBox(parent=self)
        self.n_slider_box.setMinimum(1)
        self.n_slider_box.setMaximum(30)
        self.n_slider_box.setProperty("value", 10)
        self.n_slider_box.setObjectName("n_slider_box")
        self.verticalLayout.addWidget(self.n_slider_box)
        self.gridLayout_4.addLayout(self.verticalLayout, 3, 2, 1, 1)
        self.verticalLayout_2 = QVBoxLayout()
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        spacerItem6 = QSpacerItem(20, 10, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout_2.addItem(spacerItem6)
        self.k_value_box = QSpinBox(parent=self)
        self.k_value_box.setMinimum(1)
        self.k_value_box.setMaximum(6)
        self.k_value_box.setProperty("value", 4)
        self.k_value_box.setObjectName("k_value_box")
        self.verticalLayout_2.addWidget(self.k_value_box)
        self.gridLayout_4.addLayout(self.verticalLayout_2, 2, 2, 1, 1)
        self.verticalLayout_3 = QVBoxLayout()
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        spacerItem7 = QSpacerItem(20, 10, QSizePolicy.Policy.Minimum,
                                  QSizePolicy.Policy.Expanding)
        self.verticalLayout_3.addItem(spacerItem7)
        self.grid_size_box = QSpinBox(parent=self)
        self.grid_size_box.setMinimum(1)
        self.grid_size_box.setMaximum(20)
        self.grid_size_box.setProperty("value", 5)
        self.grid_size_box.setObjectName("grid_size_box")
        self.verticalLayout_3.addWidget(self.grid_size_box)
        self.gridLayout_4.addLayout(self.verticalLayout_3, 1, 2, 1, 1)
        self.verticalLayout_4.addLayout(self.gridLayout_4)
        self.horizontalLayout_3 = QHBoxLayout()
        self.horizontalLayout_3.setObjectName("horizontalLayout_3")
        self.progressBar = QProgressBar(parent=self)
        self.progressBar.setProperty("value", 0)
        self.progressBar.setTextVisible(True)
        self.progressBar.setObjectName("progressBar")

        self.horizontalLayout_3.addWidget(self.progressBar)
        self.start_button = QPushButton(parent=self)
        self.start_button.setStyleSheet("background-color: rgb(170, 255, 127);")
        self.start_button.setFlat(False)
        self.start_button.setObjectName("start_button")
        self.start_button.setEnabled(False)
        self.horizontalLayout_3.addWidget(self.start_button)
        self.verticalLayout_4.addLayout(self.horizontalLayout_3)
        self.gridLayout_3.addLayout(self.verticalLayout_4, 0, 0, 1, 1)
        self.retranslateUi()
        self.n_counter_slider.valueChanged['int'].connect(self.n_slider_box.setValue)  # type: ignore
        self.n_slider_box.valueChanged['int'].connect(self.n_counter_slider.setValue)  # type: ignore
        self.k_value_slider.valueChanged['int'].connect(self.k_value_box.setValue)  # type: ignore
        self.k_value_box.valueChanged['int'].connect(self.k_value_slider.setValue)  # type: ignore
        self.grid_size_slider.valueChanged['int'].connect(self.grid_size_box.setValue)  # type: ignore
        self.grid_size_box.valueChanged['int'].connect(self.grid_size_slider.setValue)  # type: ignore
        QtCore.QMetaObject.connectSlotsByName(self)

    def retranslateUi(self):
        _translate = QtCore.QCoreApplication.translate
        self.setWindowTitle(_translate("Relief", "Relief"))
        self.label_8.setText(_translate("Relief", "Меньше"))
        self.label_9.setText(_translate("Relief", "Больше"))
        self.label_10.setText(_translate("Relief", "Размер\n"
                                                   "ячейки, м:"))
        self.label_3.setText(_translate("Relief", "Количество\n"
                                                  "иттераций:"))
        self.file_path_button.setText(_translate("Relief", "..."))
        self.label_4.setText(_translate("Relief", "Агресивно"))
        self.label_5.setText(_translate("Relief", "Аккуратно"))
        self.label.setText(_translate("Relief", "Быстро"))
        self.label_2.setText(_translate("Relief", "Долго"))
        self.label_6.setText(_translate("Relief", "Интенсивность\n"
                                                  "фильтрации:"))
        self.label_7.setText(_translate("Relief", "Фильтруемый\n"
                                                  "скан:"))
        self.start_button.setText(_translate("Relief", "Запуск фильтрации"))


# if __name__ == "__main__":
#     import sys
#
#     app = QApplication(sys.argv)
#     Form = QWidget()
#     ui = UiRelief()
#     ui.show()
#     sys.exit(app.exec())
