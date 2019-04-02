from PyQt5 import QtWidgets
from PyQt5.QtGui import QStandardItemModel, QStandardItem

import mainwindow
from DataBase import DataBase


class Window(QtWidgets.QMainWindow, mainwindow.Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.init_db()

    def init_db(self):
        self.db = DataBase()
        self.db.init_tables()

        tableViews = [self.tableView, self.tableView_2, self.tableView_3, self.tableView_5]
        tables = self.db.get_tables()
        for table, tableView in zip(tables, tableViews):
            self.set_table(tableView, table['rows'], table['header'])

    def init_table(self, tableView, headers_, count_rows):
        model = QStandardItemModel()
        model.clear()
        model.setHorizontalHeaderLabels(headers_)
        model.setVerticalHeaderLabels([str(i) for i in range(count_rows)])

        tableView.setModel(model)
        return model

    def set_table(self, tableView, values, header):
        model = self.init_table(tableView, header, len(values))
        for i, d in enumerate(values):
            try:
                for j, value in enumerate(d):
                    item = QStandardItem(str(value))
                    model.setItem(i, j, item)
            except:
                for j, (_, value) in enumerate(d.items()):
                    item = QStandardItem(str(value))
                    model.setItem(i, j, item)

    def query_to_db(self):
        index = self.comboBox.currentIndex()
        description = ['Количество фильмов каждого режиссера',
                       'Суммарный бюджет всех фильмов 21 века',
                       'Рейтинг режиссеров по их самым бюджетным фильмам',
                       'Фильм с самым молодым режиссером',
                       'Средний рейтинг фильмов каждого режиссера',
                       'Фильмы с актерам обладателями Оскара',
                       'Средний рейтинг фильмов каждого актера',
                       'Самый старый актер, имеющий Оскар',
                       'Список актрис в фильме с минимальным возрастным рейтингом',
                       'Список актеров, с которыми работал Кристофер Нолан',
                       'Лучшие фильмы для зрителей младше 16 лет',
                       'Актеры с одинаковым днем рождения',
                       'Последний фильм каждого актера',
                       'Фильмы с самым длинным названием 90-х годов',
                       'Разница в возрасте между самым старым и самым молодым актером-обладателем Оскара']

        self.label_6.setText(description[index])

        res = self.db.set_query(index)
        self.input_box.setText(res['sql'])

        header = [head[0] for head in res['description']]
        res = {'header': header, 'rows': res['rows']}
        self.set_table(self.tableView_4, res['rows'], res['header'])
