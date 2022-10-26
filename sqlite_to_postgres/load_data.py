from contextlib import contextmanager
import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork
from dotenv import load_dotenv
import os

from save_to_pg import save_filmwork_to_pg, save_genre_filmwork_to_pg,\
     save_genre_to_pg, save_person_filmwork_to_pg, save_person_to_pg


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


def load_from_sqlite(connection: sqlite3.Connection,
                     pg_conn: _connection,
                     n: int):
    """Основной метод загрузки данных из SQLite в Postgres"""
    query = """select
                id,
                title,
                description,
                creation_date,
                file_path,
                rating,
                type,
                '' certificate,
                created_at created,
                updated_at modified
            from film_work fw"""
    cursor = connection.execute(query)
    while table_data := cursor.fetchmany(n):
        for row in table_data:
            filmwork = Filmwork(**row)
            save_filmwork_to_pg(pg_conn, filmwork)

    query = """select
                id,
                full_name,
                created_at created,
                updated_at modified
                from person"""
    cursor = connection.execute(query)
    while table_data := cursor.fetchmany(n):
        for row in table_data:
            person = Person(**row)
            save_person_to_pg(pg_conn, person)

    query = """select
                id,
                name,
                description,
                created_at created,
                updated_at modified
                from genre"""
    cursor = connection.execute(query)
    while table_data := cursor.fetchmany(n):
        for row in table_data:
            genre = Genre(**row)
            save_genre_to_pg(pg_conn, genre)

    query = """select
                id,
                genre_id,
                film_work_id,
                created_at created
                from genre_film_work"""
    cursor = connection.execute(query)
    while table_data := cursor.fetchmany(n):
        for row in table_data:
            genre_film_work = GenreFilmwork(**row)
            save_genre_filmwork_to_pg(pg_conn, genre_film_work)

    query = """select
                id,
                person_id,
                film_work_id,
                role,
                created_at created
                from person_film_work"""
    cursor = connection.execute(query)
    while table_data := cursor.fetchmany(n):
        for row in table_data:
            person_film_work = PersonFilmwork(**row)
            save_person_filmwork_to_pg(pg_conn, person_film_work)


if __name__ == '__main__':
    load_dotenv()
    dsl = {'dbname': os.environ.get('POSTGRES_DB'),
           'user': os.environ.get('POSTGRES_USER'),
           'password': os.environ.get('POSTGRES_PASSWORD'),
           'host': os.environ.get('POSTGRES_HOST'),
           'port': os.environ.get('POSTGRES_PORT')}
    with conn_context_sqlite(os.environ.get('PATH_TO_SQLITE')) as sqlite_conn,\
            conn_context_pg(dsl) as pg_conn:
        sqlite_conn.row_factory = sqlite3.Row
        pg_conn.autocommit = False
        load_from_sqlite(sqlite_conn, pg_conn, 5)
        pg_conn.commit()
