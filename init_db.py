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
        cur.execute("SELECT * FROM general_infos;")
        tmp = cur.fetchone()
        print("Table General Info was initiated !!!")
        if tmp:
            print(tmp)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE general_infos 
                        (
                            id serial PRIMARY KEY,
                            isu integer,
                            full_name varchar,
                            education varchar,
                            positions varchar,
                            powers varchar
                        );''')
        for _, row in data.iterrows():
            cur.execute('''INSERT INTO general_infos
                            ( isu, full_name, education, positions, powers )
                            VALUES 
                            ( %s, %s, %s, %s, %s )''',
                            (row['isu'], row['full_name'], row['education'], row['positions'], row['powers']))

    conn.commit()
    cur.close()


def create_table_publications(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM publications;")
        tmp = cur.fetchone()
        print("Table Publications was initiated !!!")

        if tmp:
            print(tmp)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE publications 
                        (
                            id serial PRIMARY KEY,
                            isu integer,
                            type varchar,
                            output varchar,
                            year integer,
                            index_to_db varchar
                        );''')
        for _, row in data.iterrows():
            cur.execute('''INSERT INTO publications
                            ( isu, type, output, year, index_to_db )
                            VALUES 
                            ( %s, %s, %s, %s, %s )''',
                            (row['isu'], row['type'], row['output'], row['year'], row['index_to_db']))

    conn.commit()
    cur.close()


def create_table_projects(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM projects;")
        tmp = cur.fetchone()

        print("Table Projects was initiated !!!")
        if tmp:
            print(tmp)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE projects 
                        (
                            id serial PRIMARY KEY,
                            isu integer,
                            number_topic integer,
                            type varchar,
                            name varchar,
                            subdivision varchar,
                            start varchar,
                            end varchar,
                            keywords varchar,
                            registration_card varchar,
                            role varchar,
                            customer varchar
                        );''')
        for _, row in data.iterrows():
            cur.execute('''INSERT INTO projects
                            ( isu, number_topic, type, name, subdivision, start, end, keywords, registration_card, role, customer )
                            VALUES 
                            ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )''',
                            (row['isu'], row['number_topic'], row['type'], row['name'], row['subdivision'], row['start'], row['end'], row['keywords'], row['registration_card'], row['role'], row['customer']))

    conn.commit()
    cur.close()


def create_table_events(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM events;")
        tmp = cur.fetchone()

        print("Table Events was initiated !!!")
        if tmp:
            print(tmp)
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE events 
                        (
                            id serial PRIMARY KEY,
                            isu integer,
                            name varchar,
                            timing varchar,
                            year integer,
                            type varchar,
                            rank varchar,
                            role varchar
                        );''')
        for _, row in data.iterrows():
            cur.execute('''INSERT INTO events
                            ( isu, name, timing, year, type, rank, role )
                            VALUES 
                            ( %s, %s, %s, %s, %s, %s, %s )''',
                            (row['isu'], row['name'], row['timing'], row['year'], row['type'], row['rank'], row['role']))

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