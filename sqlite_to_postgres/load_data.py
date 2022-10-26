from contextlib import contextmanager
import sqlite3
import os
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from settings import dsl, table_is_dataclass
from query_for_sqlite import query_for_sqlite
from dataclasses import asdict, dataclass, fields
import logging

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


def save_dataclass_to_pg(pg_conn: _connection,
                         dclass: dataclass,
                         table: str,
                         schema: str = 'public'):
    cursor = pg_conn.cursor()
    dataclass_fields = [field.name for field in fields(dclass)]
    values_fields = ', '.join(f'%({field})s' for field in dataclass_fields)
    insert_fields = ', '.join(dataclass_fields)
    sql = f"""
        INSERT INTO {schema}.{table} ({insert_fields})
            VALUES ({values_fields})
            ON CONFLICT (id) DO NOTHING;
    """
    cursor.execute(sql, asdict(dclass))


def load_from_sqlite(connection: sqlite3.Connection,
                     pg_conn: _connection,
                     batch_size: int):
    """Основной метод загрузки данных из SQLite в Postgres"""
    for table, dclass in table_is_dataclass.items():
        try:
            cursor = connection.execute(
                query_for_sqlite[table]
            )
        except Exception as e:
            logging.error(e)
            continue
        while table_data := cursor.fetchmany(batch_size):
            for row in table_data:
                dobject = dclass(**row)
                try:
                    save_dataclass_to_pg(pg_conn, dobject, table, 'content')
                except Exception as e:
                    logging.error(e)


if __name__ == '__main__':
    logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')
    with conn_context_sqlite(os.environ.get('PATH_TO_SQLITE')) as sqlite_conn,\
            conn_context_pg(dsl) as pg_conn:
        sqlite_conn.row_factory = sqlite3.Row
        pg_conn.autocommit = False
        try:
            load_from_sqlite(sqlite_conn, pg_conn, 5)
        except Exception as e:
            logging.error(e)
        finally:
            pg_conn.commit()
