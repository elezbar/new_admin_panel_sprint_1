import uuid
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Filmwork:
    id: uuid.UUID
    title: str
    description: str
    creation_date: datetime
    rating: float
    type: str
    certificate: str
    file_path: str
    created: datetime
    modified: datetime


@dataclass
class Genre:
    id: uuid.UUID
    name: str
    description: str
    created: datetime
    modified: datetime


@dataclass
class Person:
    id: uuid.UUID
    full_name: str
    created: datetime
    modified: datetime


@dataclass
class GenreFilmwork:
    id: uuid.UUID
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    created: datetime


@dataclass
class PersonFilmwork:
    id: uuid.UUID
    person_id: uuid.UUID
    film_work_id: uuid.UUID
    role: str
    created: datetime
