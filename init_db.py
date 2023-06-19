from dotenv import load_dotenv
import psycopg2
import os
import pandas as pd


def read_data(path):
    general_info = pd.read_excel(path, sheet_name='Общая информация', header=0)
    publications = pd.read_excel(path, sheet_name='Публикации', header=0)
    projects = pd.read_excel(path, sheet_name='Проекты', header=0)
    events = pd.read_excel(path, sheet_name='Мероприятия', header=0)

    return {
        'general_info': general_info,
        'publications': publications,
        'projects': projects,
        'events': events
    }


def create_table_general_info(conn, data):
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM Общая_информация;")
        tmp = cur.fetchone()
        print("Table General Info was initiated !!!")
        if tmp:
            print(tmp)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE Общая_информация 
                        (
                            id serial PRIMARY KEY,
                            ИСУ integer,
                            ФИО varchar,
                            ОБУЧЕНИЕ varchar,
                            ДОЛЖНОСТИ varchar,
                            ПОЛНОМОЧИЯ varchar
                        );''')
        for _, row in data.iterrows():
            cur.execute('''INSERT INTO Общая_информация
                            ( ИСУ, ФИО, ОБУЧЕНИЕ, ДОЛЖНОСТИ, ПОЛНОМОЧИЯ )
                            VALUES 
                            ( %s, %s, %s, %s, %s )''',
                            (row['ИСУ'], row['ФИО'], row['ОБУЧЕНИЕ'], row['ДОЛЖНОСТИ'], row['ПОЛНОМОЧИЯ']))

    conn.commit()
    cur.close()


def create_table_publications(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM Публикации;")
        tmp = cur.fetchone()
        print("Table Publications was initiated !!!")

        if tmp:
            print(tmp)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE Публикации 
                        (
                            id serial PRIMARY KEY,
                            ИСУ integer,
                            ТИП_ПУБЛИКАЦИИ varchar,
                            ВЫХОДНЫЕ_ДАННЫЕ varchar,
                            ГОД integer,
                            ИНДЕКСИРОВАНИЕ_В_БД varchar
                        );''')
        for _, row in data.iterrows():
            cur.execute('''INSERT INTO Публикации
                            ( ИСУ, ТИП_ПУБЛИКАЦИИ, ВЫХОДНЫЕ_ДАННЫЕ, ГОД, ИНДЕКСИРОВАНИЕ_В_БД )
                            VALUES 
                            ( %s, %s, %s, %s, %s )''',
                            (row['ИСУ'], row['ТИП_ПУБЛИКАЦИИ'], row['ВЫХОДНЫЕ_ДАННЫЕ'], row['ГОД'], row['ИНДЕКСИРОВАНИЕ_В_БД']))

    conn.commit()
    cur.close()


def create_table_projects(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM Проекты;")
        tmp = cur.fetchone()

        print("Table Projects was initiated !!!")
        if tmp:
            print(tmp)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE Проекты 
                        (
                            id serial PRIMARY KEY,
                            ИСУ integer,
                            НОМЕР_ТЕМЫ integer,
                            ТИП_ПРОЕКТА varchar,
                            НАИМЕНОВАНИЕ varchar,
                            ПОДРАЗДЕЛЕНИЕ varchar,
                            НАЧАЛО varchar,
                            ОКОНЧАНИЕ varchar,
                            КЛЮЧЕВЫЕ_СЛОВА varchar,
                            РЕГИСТРАЦИОННАЯ_КАРТА varchar,
                            РОЛЬ varchar,
                            ЗАКАЗЧИК varchar
                        );''')
        for _, row in data.iterrows():
            cur.execute('''INSERT INTO Проекты
                            ( ИСУ, НОМЕР_ТЕМЫ, ТИП_ПРОЕКТА, НАИМЕНОВАНИЕ, ПОДРАЗДЕЛЕНИЕ, НАЧАЛО, ОКОНЧАНИЕ, КЛЮЧЕВЫЕ_СЛОВА, РЕГИСТРАЦИОННАЯ_КАРТА, РОЛЬ, ЗАКАЗЧИК )
                            VALUES 
                            ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )''',
                            (row['ИСУ'], row['НОМЕР_ТЕМЫ'], row['ТИП_ПРОЕКТА'], row['НАИМЕНОВАНИЕ'], row['ПОДРАЗДЕЛЕНИЕ'], row['НАЧАЛО'], row['ОКОНЧАНИЕ'], row['КЛЮЧЕВЫЕ_СЛОВА'], row['РЕГИСТРАЦИОННАЯ_КАРТА'], row['РОЛЬ'], row['ЗАКАЗЧИК']))

    conn.commit()
    cur.close()


def create_table_events(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM Мероприятия;")
        tmp = cur.fetchone()

        print("Table Events was initiated !!!")
        if tmp:
            print(tmp)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE Мероприятия 
                        (
                            id serial PRIMARY KEY,
                            ИСУ integer,
                            НАИМЕНОВАНИЕ varchar,
                            СРОКИ varchar,
                            ГОД integer,
                            ТИП varchar,
                            РАНГ varchar,
                            РОЛИ varchar
                        );''')
        for _, row in data.iterrows():
            cur.execute('''INSERT INTO Мероприятия
                            ( ИСУ, НАИМЕНОВАНИЕ, СРОКИ, ГОД, ТИП, РАНГ, РОЛИ )
                            VALUES 
                            ( %s, %s, %s, %s, %s, %s, %s )''',
                            (row['ИСУ'], row['НАИМЕНОВАНИЕ'], row['СРОКИ'], row['ГОД'], row['ТИП'], row['РАНГ'], row['РОЛИ']))

    conn.commit()
    cur.close()


if __name__ == "__main__":
    data = read_data('Для хакатона.xlsx')

    load_dotenv()
    DB_URI = os.environ.get("DB_URI")
    print(DB_URI)
    conn = psycopg2.connect(DB_URI)

    create_table_general_info(conn, data['general_info'])
    create_table_publications(conn, data['publications'])
    create_table_projects(conn, data['projects'])
    create_table_events(conn, data['events'])
    # print(data)