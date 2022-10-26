from dataclasses import asdict
from psycopg2.extensions import connection as _connection
from models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


def save_filmwork_to_pg(pg_conn: _connection, filmwork: Filmwork):
    cursor = pg_conn.cursor()
    sql = """
        INSERT INTO content.film_work (id,
                                       title,
                                       description,
                                       creation_date,
                                       rating,
                                       type,
                                       certificate,
                                       file_path,
                                       created,
                                       modified)
            VALUES (%(id)s,
                    %(title)s,
                    %(description)s,
                    %(creation_date)s,
                    %(rating)s,
                    %(type)s,
                    %(certificate)s,
                    %(file_path)s,
                    %(created)s,
                    %(modified)s)
            ON CONFLICT (id) DO NOTHING;
    """
    cursor.execute(sql, asdict(filmwork))


def save_genre_to_pg(pg_conn: _connection, genre: Genre):
    cursor = pg_conn.cursor()
    sql = """
        INSERT INTO content.genre (id,
                                       name,
                                       description,
                                       created,
                                       modified)
            VALUES (%(id)s,
                    %(name)s,
                    %(description)s,
                    %(created)s,
                    %(modified)s)
            ON CONFLICT (id) DO NOTHING;
    """
    cursor.execute(sql, asdict(genre))


def save_person_to_pg(pg_conn: _connection, person: Person):
    cursor = pg_conn.cursor()
    sql = """
        INSERT INTO content.person (id,
                                       full_name,
                                       created,
                                       modified)
            VALUES (%(id)s,
                    %(full_name)s,
                    %(created)s,
                    %(modified)s)
            ON CONFLICT (id) DO NOTHING;
    """
    cursor.execute(sql, asdict(person))


def save_genre_filmwork_to_pg(pg_conn: _connection,
                              genre_filmwork: GenreFilmwork):
    cursor = pg_conn.cursor()
    sql = """
        INSERT INTO content.genre_film_work (id,
                                       genre_id,
                                       film_work_id,
                                       created)
            VALUES (%(id)s,
                    %(genre_id)s,
                    %(film_work_id)s,
                    %(created)s)
            ON CONFLICT (id) DO NOTHING;
    """
    cursor.execute(sql, asdict(genre_filmwork))


def save_person_filmwork_to_pg(pg_conn: _connection,
                               person_filmwork: PersonFilmwork):
    cursor = pg_conn.cursor()
    sql = """
        INSERT INTO content.person_film_work (id,
                                       person_id,
                                       film_work_id,
                                       role,
                                       created)
            VALUES (%(id)s,
                    %(person_id)s,
                    %(film_work_id)s,
                    %(role)s,
                    %(created)s)
            ON CONFLICT (id) DO NOTHING;
    """
    cursor.execute(sql, asdict(person_filmwork))
