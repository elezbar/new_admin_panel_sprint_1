# Запросы с унификацией данных из sqlite в postgres

query_for_sqlite = {
    'film_work': """select
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
                from film_work fw""",
    'person': """select
                id,
                full_name,
                created_at created,
                updated_at modified
                from person""",
    'genre': """select
                id,
                name,
                description,
                created_at created,
                updated_at modified
                from genre""",
    'genre_film_work': """select
                id,
                genre_id,
                film_work_id,
                created_at created
                from genre_film_work""",
    'person_film_work': """select
                id,
                person_id,
                film_work_id,
                role,
                created_at created
                from person_film_work"""
}
