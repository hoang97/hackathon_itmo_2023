from dotenv import load_dotenv
import psycopg2
import os
import pandas as pd
import time, datetime
from dateutil.relativedelta import relativedelta

months = {
    'января': 1,
    'февраля': 2,
    'марта': 3,
    'апреля': 4,
    'мая': 5,
    'июня': 6,
    'июля': 7,
    'августа': 8,
    'сентября': 9,
    'октября': 10,
    'ноября': 11,
    'декабря': 12
}

def str_to_unix(str):
    return time.mktime(datetime.datetime.strptime(str, f"%d %m %Y").timetuple())

def time_to_unix(dt):
    return time.mktime(dt.timetuple())

def add_1_year(unix_time):
    tmp = datetime.datetime.fromtimestamp(unix_time)
    tmp = tmp + relativedelta(years=1)
    return time.mktime(tmp.timetuple())

def normalize(s:str):
    for i in months.keys():
        s = s.replace(i, str(months[i]))
    return s

def read_data(path):
    general_info = pd.read_excel(path, sheet_name='Общая информация', header=0)
    user_publications = pd.read_excel(path, sheet_name='Публикации', header=0)
    user_projects = pd.read_excel(path, sheet_name='Проекты', header=0)
    user_events = pd.read_excel(path, sheet_name='Мероприятия', header=0)

    publications = user_publications.drop(columns=['isu']).drop_duplicates().reset_index(drop=True)

    projects = user_projects.drop(columns=['isu']).drop_duplicates().reset_index(drop=True)
    projects['start'] = projects['start'].apply(time_to_unix)
    projects['end'] = projects['end'].apply(time_to_unix) 

    events = user_events.drop(columns=['isu']).drop_duplicates().reset_index(drop=True)
    start = []
    end = []
    for _, row in events.iterrows():
        tmp = normalize(row['timing'].lower())
        tmp = tmp.split(' - ')
        
        start.append(tmp[0] + " " + str(row['year']))
        if len(tmp) > 1:
            end.append(tmp[1] + " " + str(row['year']))
        else:
            end.append(tmp[0] + " " + str(row['year']))
            
    start = pd.Series(start).apply(str_to_unix).apply(add_1_year)
    end = pd.Series(end).apply(str_to_unix).apply(add_1_year)
    events['start'] = start
    events['end'] = end 
    events.drop(columns=['timing', 'year'], inplace=True)

    return {
        'general_info': general_info,
        'user_publications': user_publications,
        'user_projects': user_projects,
        'user_events': user_events,
        'publications': publications,
        'projects': projects,
        'events': events,
    }


def create_table_general_info(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM general_infos;")
        tmp = cur.fetchone()
        if tmp:
            print(tmp)
        else:
            for _, row in data.iterrows():
                cur.execute('''INSERT INTO general_infos
                                ( isu, full_name, education, positions, powers )
                                VALUES 
                                ( %s, %s, %s, %s, %s )''',
                                (row['isu'], row['full_name'], row['education'], row['positions'], row['powers']))
        print("Table General Info was successfully initiated !!!")
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
        print("Table General Info was not created !!!")
        
    conn.commit()
    cur.close()


def create_table_publications(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM publications;")
        tmp = cur.fetchone()
        if tmp:
            print(tmp)
        else:
            for _, row in data.iterrows():
                cur.execute('''INSERT INTO publications
                                ( type, output, year, index_to_db )
                                VALUES 
                                ( %s, %s, %s, %s )''',
                                (row['type'], row['output'], int(row['year']), row['index_to_db']))
        print("Table Publications was successfully initiated !!!")
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE publications 
                        (
                            id serial PRIMARY KEY,
                            type varchar,
                            output varchar,
                            year integer,
                            index_to_db varchar
                        );''')
        print("Table Publications was not created !!!")
        
    conn.commit()
    cur.close()


def create_table_events(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM events;")
        tmp = cur.fetchone()
        if tmp:
            print(tmp)
        else:
            for _, row in data.iterrows():
                cur.execute('''INSERT INTO events
                                ( name, start_time, end_time, type )
                                VALUES 
                                ( %s, %s, %s, %s )''',
                                (row['name'], row['start'], row['end'], row['type']))
        print("Table Events was successfully initiated !!!")
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE events 
                        (
                            id serial PRIMARY KEY,
                            name varchar,
                            start_time integer,
                            end_time integer,
                            type varchar
                        );''')
        print("Table Events was not created !!!")
        
    conn.commit()
    cur.close()


def create_table_projects(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM projects;")
        tmp = cur.fetchone()
        if tmp:
            print(tmp)
        else:
            for _, row in data.iterrows():
                cur.execute('''INSERT INTO projects
                                ( number_topic, type, name, subdivision, start_time, end_time, keywords, registration_card, customer )
                                VALUES 
                                ( %s, %s, %s, %s, %s, %s, %s, %s, %s )''',
                                (row['number_topic'], row['type'], row['name'], row['subdivision'], row['start'], row['end'], row['keywords'], row['registration_card'], row['customer']))
        print("Table Projects was successfully initiated !!!")
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE projects 
                        (
                            id serial PRIMARY KEY,
                            number_topic integer,
                            type varchar,
                            name varchar,
                            subdivision varchar,
                            start_time integer,
                            end_time integer,
                            keywords varchar,
                            registration_card varchar,
                            customer varchar
                        );''')
        print("Table Projects was not created !!!")
        
    conn.commit()
    cur.close()


def create_table_user_publications(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM user_publications;")
        tmp = cur.fetchone()
        if tmp:
            print(tmp)
        else:
            for _, row in data.iterrows():
                cur.execute(f'''SELECT id FROM publications WHERE publications.output=%s''', (row['output'],))
                pub_id = cur.fetchone()[0]
                cur.execute('''INSERT INTO user_publications
                                ( isu, publication)
                                VALUES 
                                ( %s, %s )''',
                                (row['isu'], pub_id))
        print("Table user_publications was successfully initiated !!!")
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE user_publications 
                        (
                            id serial PRIMARY KEY,
                            isu integer,
                            publication integer
                        );''')
        print("Table user_publications was not created !!!")
        
    conn.commit()
    cur.close()


def create_table_user_events(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM user_events;")
        tmp = cur.fetchone()
        if tmp:
            print(tmp)
        else:
            for _, row in data.iterrows():
                cur.execute('''SELECT id FROM events WHERE events.name=%s''', (row['name'],))
                event_id = cur.fetchone()[0]
                cur.execute('''INSERT INTO user_events
                                ( isu, event, rank, role )
                                VALUES 
                                ( %s, %s, %s, %s )''',
                                (row['isu'], event_id, row['rank'], row['role']))
        print("Table user_events was successfully initiated !!!")
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE user_events 
                        (
                            id serial PRIMARY KEY,
                            isu integer,
                            event integer,
                            rank varchar,
                            role varchar
                        );''')
        print("Table user_events was not created !!!")
        
    conn.commit()
    cur.close()


def create_table_user_projects(conn, data):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM user_projects;")
        tmp = cur.fetchone()
        if tmp:
            print(tmp)
        else:
            for _, row in data.iterrows():
                cur.execute('''SELECT id FROM projects WHERE projects.number_topic=%s''', (row['number_topic'],))
                project_id = cur.fetchone()[0]
                cur.execute('''INSERT INTO user_projects
                                ( isu, project, role )
                                VALUES 
                                ( %s, %s, %s )''',
                                (row['isu'], project_id, row['role']))
        print("Table user_projects was successfully initiated !!!")
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.execute('''CREATE TABLE user_projects 
                        (
                            id serial PRIMARY KEY,
                            isu integer,
                            project integer,
                            role varchar
                        );''')
        print("Table user_projects was not created !!!")
        
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
    create_table_events(conn, data['events'])
    create_table_projects(conn, data['projects'])
    create_table_user_publications(conn, data['user_publications'])
    create_table_user_events(conn, data['user_events'])
    create_table_user_projects(conn, data['user_projects'])
