from contextlib import contextmanager
import sqlite3
from dotenv import load_dotenv
import psycopg2
import os
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from datetime import datetime


@contextmanager
def conn_context_sqlite(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@contextmanager
def conn_context_pg(dsl: dict):
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    yield conn
    conn.close()


def connect_to_db(func):

    def wrap():
        load_dotenv()
        dsl = {'dbname': os.environ.get('POSTGRES_DB'),
               'user': os.environ.get('POSTGRES_USER'),
               'password': os.environ.get('POSTGRES_PASSWORD'),
               'host': os.environ.get('POSTGRES_HOST'),
               'port': os.environ.get('POSTGRES_PORT')}
        with conn_context_sqlite(os.environ.get('PATH_TO_SQLITE')) as conn,\
             conn_context_pg(dsl) as pg_conn:
            result = func(conn, pg_conn)
        return result
    return wrap


@connect_to_db
def test_table_genre(connection: sqlite3.Connection,
                     pg_conn: _connection,):
    result_sqlite = connection.execute("select count(*) c from genre")\
                        .fetchone()
    cursor = pg_conn.cursor()
    cursor.execute("select count(*) c from content.genre")
    result_pg = cursor.fetchone()
    assert result_sqlite['c'] == result_pg['c']
    cursor.execute("select * from content.genre")
    result_pg = cursor.fetchall()
    format_date = '%Y-%m-%d %H:%M:%S.%f%z'
    for row in result_pg:
        result_sqlite = connection.execute("""select
                                                id,
                                                name,
                                                description,
                                                created_at||':00' created,
                                                updated_at||':00' modified
         from genre where id=(?)""", [row['id']]).fetchone()
        assert row['id'] == result_sqlite['id']
        assert row['name'] == result_sqlite['name']
        assert row['description'] == result_sqlite['description']
        assert row['created'] == datetime.strptime(result_sqlite['created'],
                                                   format_date)
        assert row['modified'] == datetime.strptime(result_sqlite['modified'],
                                                    format_date)


@connect_to_db
def test_table_person(connection: sqlite3.Connection,
                      pg_conn: _connection,):
    result_sqlite = connection.execute("select count(*) c from person")\
                        .fetchone()
    cursor = pg_conn.cursor()
    cursor.execute("select count(*) c from content.person")
    result_pg = cursor.fetchone()
    assert result_sqlite['c'] == result_pg['c']
    cursor.execute("select * from content.person")
    result_pg = cursor.fetchall()
    format_date = '%Y-%m-%d %H:%M:%S.%f%z'
    for row in result_pg:
        result_sqlite = connection.execute("""select
                                                id,
                                                full_name,
                                                created_at||':00' created,
                                                updated_at||':00' modified
         from person where id=(?)""", [row['id']]).fetchone()
        assert row['id'] == result_sqlite['id']
        assert row['full_name'] == result_sqlite['full_name']
        assert row['created'] == datetime.strptime(result_sqlite['created'],
                                                   format_date)
        assert row['modified'] == datetime.strptime(result_sqlite['modified'],
                                                    format_date)


@connect_to_db
def test_table_film_work(connection: sqlite3.Connection,
                         pg_conn: _connection,):
    result_sqlite = connection.execute("select count(*) c from film_work")\
                        .fetchone()
    cursor = pg_conn.cursor()
    cursor.execute("select count(*) c from content.film_work")
    result_pg = cursor.fetchone()
    assert result_sqlite['c'] == result_pg['c']
    cursor.execute("select * from content.film_work")
    result_pg = cursor.fetchall()
    format_date = '%Y-%m-%d %H:%M:%S.%f%z'
    for row in result_pg:
        result_sqlite = connection.execute("""
            select
                id,
                title,
                description,
                creation_date||':00' creation_date,
                rating,
                type,
                '' certificate,
                file_path,
                created_at||':00' created,
                updated_at||':00' modified
            from film_work where id=(?)""", [row['id']]).fetchone()
        assert row['id'] == result_sqlite['id']
        assert row['title'] == result_sqlite['title']
        assert row['description'] == result_sqlite['description']
        assert row['rating'] == result_sqlite['rating']
        assert row['certificate'] == result_sqlite['certificate']
        assert row['file_path'] == result_sqlite['file_path']
        assert row['created'] == datetime.strptime(result_sqlite['created'],
                                                   format_date)
        assert row['modified'] == datetime.strptime(result_sqlite['modified'],
                                                    format_date)
        if result_sqlite['creation_date'] is None:
            assert row['creation_date'] == result_sqlite['creation_date']
        else:
            assert row['creation_date'] == datetime.strptime(
                                        result_sqlite['creation_date'],
                                        format_date)


@connect_to_db
def test_table_person_film_work(connection: sqlite3.Connection,
                                pg_conn: _connection,):
    result_sqlite = connection.execute("""
        select count(*) c
        from person_film_work
        """).fetchone()
    cursor = pg_conn.cursor()
    cursor.execute("select count(*) c from content.person_film_work")
    result_pg = cursor.fetchone()
    assert result_sqlite['c'] == result_pg['c']
    cursor.execute("select * from content.person_film_work")
    result_pg = cursor.fetchall()
    format_date = '%Y-%m-%d %H:%M:%S.%f%z'
    for row in result_pg:
        result_sqlite = connection.execute("""select
                                                id,
                                                person_id,
                                                film_work_id,
                                                role,
                                                created_at||':00' created
         from person_film_work where id=(?)""", [row['id']]).fetchone()
        assert row['id'] == result_sqlite['id']
        assert row['person_id'] == result_sqlite['person_id']
        assert row['film_work_id'] == result_sqlite['film_work_id']
        assert row['role'] == result_sqlite['role']
        assert row['created'] == datetime.strptime(result_sqlite['created'],
                                                   format_date)


@connect_to_db
def test_table_genre_film_work(connection: sqlite3.Connection,
                               pg_conn: _connection,):
    result_sqlite = connection.execute("""
        select count(*) c
        from genre_film_work
        """).fetchone()
    cursor = pg_conn.cursor()
    cursor.execute("select count(*) c from content.genre_film_work")
    result_pg = cursor.fetchone()
    assert result_sqlite['c'] == result_pg['c']
    cursor.execute("select * from content.genre_film_work")
    result_pg = cursor.fetchall()
    format_date = '%Y-%m-%d %H:%M:%S.%f%z'
    for row in result_pg:
        result_sqlite = connection.execute("""select
                                                id,
                                                genre_id,
                                                film_work_id,
                                                created_at||':00' created
         from genre_film_work where id=(?)""", [row['id']]).fetchone()
        assert row['id'] == result_sqlite['id']
        assert row['genre_id'] == result_sqlite['genre_id']
        assert row['film_work_id'] == result_sqlite['film_work_id']
        assert row['created'] == datetime.strptime(result_sqlite['created'],
                                                   format_date)
