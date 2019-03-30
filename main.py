import sys

from PyQt5 import QtWidgets
from window import Window


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = Window()
    window.show()
    app.exec_()


if __name__ == '__main__':
    main()
