import os
from dotenv import load_dotenv
from models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


load_dotenv()

dsl = {'dbname': os.environ.get('POSTGRES_DB'),
       'user': os.environ.get('POSTGRES_USER'),
       'password': os.environ.get('POSTGRES_PASSWORD'),
       'host': os.environ.get('POSTGRES_HOST'),
       'port': os.environ.get('POSTGRES_PORT')}

table_is_dataclass = {
       'film_work': Filmwork,
       'genre': Genre,
       'person': Person,
       'person_film_work': PersonFilmwork,
       'genre_film_work': GenreFilmwork,
}