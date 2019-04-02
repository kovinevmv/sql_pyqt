import pymysql


class DataBase:
    def __init__(self, name='sql', user='root',
                 password='1234', host='127.0.0.1'):
        self.connection = pymysql.connect(host=host,
                                          user=user,
                                          password=password)
        self.name = name

    def execute(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            res = {'description': cursor.description, 'rows': [row for row in cursor]}
        return res

    def _create_tables(self):
        self.execute(f"CREATE DATABASE IF NOT EXISTS `{self.name}` "
                     "CHARACTER SET utf8mb4 "
                     "COLLATE utf8mb4_unicode_ci")
        self.execute("USE `sql`")

        tables = {
            'actor': """
        CREATE TABLE actor(
            actor_id int NOT NULL PRIMARY KEY,
            first_name VARCHAR (50) NOT NULL,
            last_name VARCHAR (50) NOT NULL,
            gender VARCHAR (10) NOT NULL,
            date_of_birth Date NOT NULL, 
            oscar BOOLEAN NOT NULL DEFAULT 0
        );
        """,
            'director': """
        CREATE TABLE director(
            director_id int NOT NULL PRIMARY KEY,
            first_name VARCHAR (50) NOT NULL,
            last_name VARCHAR (50) NOT NULL,
            gender VARCHAR (10) NOT NULL,
            date_of_birth Date NOT NULL
        );
        """,
            'movie': """
        CREATE TABLE movie(
            movie_id int NOT NULL PRIMARY KEY,
            title varchar (50) NOT NULL,
            release_data Date NOT NULL,
            director_id INT NOT NULL,
            score FLOAT NOT NULL,
            budget BIGINT NOT NULL, 
            rating SMALLINT NOT NULL
        );
        """,
            'movie_x_actor': """
        CREATE TABLE movie_x_actor(
            movie_id int NOT NULL,
            actor_id int NOT NULL,
            PRIMARY KEY (movie_id, actor_id)
        );
        """
        }

        for name, sql in tables.items():
            self.execute('DROP TABLE IF EXISTS {}'.format(name))
            self.execute(sql)

    def _insert_values(self):
        values = {
            'actor':
                """
                INSERT INTO actor VALUES 
                    (1,'Эдвард','Нортон','Мужской','1969-08-18',0),
                    (2,'Брэд','Питт','Мужской','1963-12-18',1),
                    (3,'Хелена','Картер','Женский','1966-05-26',0),
                    (4,'Леонардо','ДиКаприо','Мужской','1974-11-11',1),
                    (5,'Марк','Руффало','Мужской','1967-11-22',0),
                    (6,'Джозеф','Гордон-Левитт','Мужской','1981-02-17',0),
                    (7,'Эллен','Пейдж','Женский','1987-02-21',0),
                    (8,'Том','Харди','Мужской','1977-09-15',0),
                    (9,'Хью','Джекман','Мужской','1968-10-12',0),
                    (10,'Кристиан','Бэйл','Мужской','1974-01-30',1),
                    (11,'Майкл','Кейн','Мужской','1933-03-14',1),
                    (12,'Пол','Джаматти','Мужской','1967-06-06',0),
                    (13,'Джессика','Бил','Женский','1982-03-03',0),
                    (14,'Эдвард','Ферлонг','Мужской','1977-08-02',0),
                    (15,'Беверли','Д’Анджело','Женский','1951-11-15',0),
                    (16,'Арнольд','Шварценеггер','Мужской','1947-07-30',0),
                    (17,'Линда','Хэмилтон','Женский','1956-09-26',0),
                    (18,'Кейт','Уинслет','Женский','1975-10-05',1),
                    (19,'Сэм','Уортингтон','Мужской','1967-08-02',0),
                    (20,'Зои','Салдана','Женский','1978-06-19',0),
                    (21,'Морган','Фриман','Мужской','1937-06-01',1),
                    (22,'Гвинет','Пэлтроу','Женский','1972-09-27',1),
                    (23,'Джейк','Джилленхол','Мужской','1980-12-19',0),
                    (24,'Роберт','Дауни','Мужской','1965-04-04',0),
                    (25,'Чарльз','Гродин','Мужской','1935-04-21',0),
                    (26,'Бонни','Хант','Женский','1961-09-22',0),
                    (27,'Александр','Невский','Мужской','1971-07-17',0),
                    (28,'Лиззи','Каплан','Женский','1982-06-30',0),
                    (29,'Джессика','Лукас','Женский','1985-09-24',0),
                    (30,'Дмитрий','Нагиев','Мужской','1967-04-04',0);
                """,
            'director':
                """
                INSERT INTO director VALUES
                    (1, 'Дэвид', 'Финчер', 'Мужской', '1962-08-28'),
                    (2, 'Мартин', 'Скорсезе', 'Мужской', '1942-11-17'),
                    (3, 'Кристофер', 'Нолан', 'Мужской', '1970-07-30'),
                    (4, 'Нил', 'Бёргер', 'Мужской', '1964-01-01'),
                    (5, 'Тони', 'Кэй', 'Мужской', '1952-06-08'),
                    (6, 'Джеймс', 'Кэмерон', 'Мужской', '1954-08-16'),
                    (7, 'Брайан', 'Левант', 'Мужской', '1952-08-06'),
                    (8, 'Брент', 'Хафф', 'Мужской', '1961-03-11'),
                    (9, 'Мэтт', 'Ривз', 'Мужской', '1966-04-27'),
                    (10, 'Александр', 'Невзоров', 'Мужской', '1958-08-03');
                """,
            'movie':
                """
                INSERT INTO movie VALUES 
                    (1, 'Бойцовский клуб', '2000-01-13', 1, 8.648, 63000000, 18),
                    (2, 'Остров проклятых', '2010-02-18', 2, 8.492, 80000000, 16),
                    (3, 'Начало', '2010-07-22', 3, 8.665, 160000000, 12),
                    (4, 'Престиж', '2007-01-18', 3, 8.524, 40000000, 12),
                    (5, 'Иллюзионист', '2006-10-19', 4, 8.015, 16500000, 12),
                    (6, 'Американская история X', '1998-10-30', 5, 8.297, 20000000, 16),
                    (7, 'Терминатор 2: Судный день', '1991-07-01', 6, 8.305, 102000000, 18),
                    (8, 'Титаник', '1998-02-20', 6, 8.369, 200000000, 12),
                    (9, 'Аватар', '2009-12-17', 6, 7.943, 237000000, 12),
                    (10, 'Семь', '1995-09-15', 1, 8.295, 33000000, 18),
                    (11, 'Зодиак', '2007-08-02', 1, 7.337, 65000000, 18),
                    (12, 'Бетховен', '1993-09-17', 7, 7.277, 40000000, 6),
                    (13, 'Форсаж да Винчи', '2008-04-03', 8, 1.317, 1000000, 16),
                    (14, 'Монстро', '2008-01-18', 9, 6.926, 25000000, 16),
                    (15, 'Чистилище', '1998-03-23', 10, 7.763, 1000000, 16);
                """,
            'movie_x_actor':
                """
                INSERT INTO movie_x_actor VALUES 
                    (1, 1),
                    (1, 2),
                    (1, 3),
                    (2, 4),
                    (2, 5),
                    (3, 4),
                    (3, 6),
                    (3, 7),
                    (3, 8),
                    (4, 9),
                    (4, 10),
                    (4, 11),
                    (5, 1),
                    (5, 12),
                    (5, 13),
                    (6, 1),
                    (6, 14),
                    (6, 15),
                    (7, 14),
                    (7, 16),
                    (7, 17),
                    (8, 4),
                    (8, 18),
                    (9, 19),
                    (9, 20),
                    (10, 2),
                    (10, 21),
                    (10, 22),
                    (11, 5),
                    (11, 23),
                    (11, 24),
                    (12, 25),
                    (12, 26),
                    (13, 27),
                    (14, 28),
                    (14, 29),
                    (15, 30);
                """
        }

        for name, sql in values.items():
            self.execute(sql)

    def init_tables(self):
        self._create_tables()
        self._insert_values()

    def get_table(self, name):
        res = self.execute(f'SELECT * FROM {name}')
        header = [head[0] for head in res['description']]
        return {'header': header, 'rows': res['rows']}

    def get_tables(self):
        list_tables = ['actor', 'director', 'movie', 'movie_x_actor']
        return [self.get_table(table) for table in list_tables]

    def set_query(self, value):
        if value == 0:
            query = 'SELECT first_name as Имя, last_name as Фамилия, Count(movie.title) as "Количество фильмов" ' \
                    'FROM director INNER JOIN movie ON director.director_id = movie.director_id ' \
                    'GROUP BY first_name, last_name;'

        if value == 1:
            query = 'SELECT Sum(budget) AS "Суммарный бюджет" ' \
                    'FROM movie ' \
                    'WHERE Year(release_data)>2000;'

        if value == 2:
            query = 'SELECT Имя, Фамилия, Бюджет from ' \
                    '(' \
                    '   SELECT director.first_name AS Имя, director.last_name AS Фамилия, Max(movie.budget) AS Бюджет ' \
                    '   FROM director ' \
                    '   INNER JOIN movie ON director.director_id = movie.director_id  ' \
                    '   GROUP BY director.first_name, director.last_name' \
                    ') as Q ' \
                    'order by Бюджет DESC'

        if value == 3:
            query = 'SELECT title as "Название фильма", director.first_name AS Имя, director.last_name AS Фамилия ' \
                    'FROM movie ' \
                    'INNER JOIN director ON director.director_id = movie.director_id ' \
                    'where director.date_of_birth = (SELECT MAX(date_of_birth) FROM director)'

        if value == 4:
            query = 'SELECT first_name as Имя, last_name as Фамилия, AVG(score) as "Средний рейтинг" ' \
                    'FROM movie INNER JOIN director ON director.director_id = movie.director_id ' \
                    'GROUP BY director.first_name, director.last_name'

        if value == 5:
            query = 'SELECT distinct title as Название ' \
                    'FROM (actor LEFT JOIN movie_x_actor ON actor.ACTOR_ID = movie_x_actor.ACTOR_ID) ' \
                    'LEFT JOIN movie ON movie_x_actor.MOVIE_ID = movie.MOVIE_ID ' \
                    'where oscar=True'

        if value == 6:
            query = 'SELECT actor.first_name as Имя, actor.last_name as Фамилия, AVG(movie.score) AS "Средний рейтинг" ' \
                    'FROM (actor LEFT JOIN movie_x_actor ON actor.actor_id = movie_x_actor.actor_id) ' \
                    'LEFT JOIN movie ON movie_x_actor.movie_id = movie.movie_id ' \
                    'GROUP BY actor.first_name, actor.last_name'

        if value == 7:
            query = 'SELECT actor.first_name as Имя, actor.last_name as Фамилия, actor.date_of_birth AS "Дата рождения" ' \
                    'FROM actor ' \
                    'WHERE OSCAR=True AND date_of_birth = (SELECT MIN(date_of_birth) FROM actor)'

        if value == 8:
            query = 'SELECT FIRST_NAME as Имя, LAST_NAME as Фамилия ' \
                    'FROM (actor LEFT JOIN movie_x_actor ON actor.ACTOR_ID = movie_x_actor.ACTOR_ID) ' \
                    'LEFT JOIN movie ON movie_x_actor.MOVIE_ID = movie.MOVIE_ID ' \
                    'WHERE GENDER="Женский" AND RATING = (SELECT MIN(RATING) FROM movie)'

        if value == 9:
            query = 'SELECT actor.FIRST_NAME as Имя, actor.LAST_NAME as Фамилия, TITLE as Название ' \
                    'FROM ((actor LEFT JOIN movie_x_actor ON actor.ACTOR_ID = movie_x_actor.ACTOR_ID) ' \
                    'LEFT JOIN movie ON movie_x_actor.MOVIE_ID = movie.MOVIE_ID) ' \
                    'LEFT JOIN director ON director.DIRECTOR_ID = movie.DIRECTOR_ID ' \
                    'WHERE director.FIRST_NAME="Кристофер" AND director.LAST_NAME="Нолан"'

        if value == 10:
            query = 'SELECT TITLE as Название, SCORE as Рейтинг, RATING as Возраст ' \
                    'FROM movie ' \
                    'WHERE RATING < 16 ORDER BY SCORE DESC limit 5'

        if value == 11:
            query = 'SELECT FIRST_NAME as Имя, LAST_NAME as Фамилия, date_of_birth AS "Дата рождения" FROM actor, ' \
                    '(' \
                    '   SELECT Month(date_of_birth) AS ex1, Day(date_of_birth) AS ex2, Count(*) AS ex3 ' \
                    '   FROM actor ' \
                    '   GROUP BY Month(date_of_birth), Day(date_of_birth) ' \
                    '   HAVING (Count(*)>1) ' \
                    ') as Q ' \
                    'WHERE Month(date_of_birth) = ex1 AND Day(date_of_birth) = ex2'

        if value == 12:
            query = 'SELECT Q.FIRST_NAME as Имя, Q.LAST_NAME as Фамилия, EX1 as "Дата выхода фильма",' \
                    ' TITLE as Название FROM movie, ' \
                    '(' \
                    '   SELECT FIRST_NAME, LAST_NAME, MAX(RELEASE_DATA) AS EX1 ' \
                    '   FROM (actor LEFT JOIN movie_x_actor ON actor.ACTOR_ID = movie_x_actor.ACTOR_ID)' \
                    '   LEFT JOIN movie ON movie_x_actor.MOVIE_ID = movie.MOVIE_ID' \
                    '   GROUP BY FIRST_NAME, LAST_NAME' \
                    ') AS Q ' \
                    'WHERE Q.EX1 = movie.RELEASE_DATA'

        if value == 13:
            query = 'SELECT TITLE as Название, CHAR_LENGTH(TITLE) as "Длина названия" ' \
                    'FROM movie ' \
                    'WHERE YEAR(RELEASE_DATA) BETWEEN 1990 AND 2000 ' \
                    'ORDER BY CHAR_LENGTH(TITLE) DESC limit 3'

        if value == 14:
            query = 'SELECT (MAX(YEAR(date_of_birth)) - MIN(YEAR(date_of_birth)) ) as "Разница в возрасте" ' \
                    'FROM actor ' \
                    'WHERE OSCAR = TRUE AND GENDER = "Мужской"'

        res = {'sql': query}
        res.update(self.execute(query))
        return res








