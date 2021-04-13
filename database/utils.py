import psycopg2
from decouple import config


# noinspection DuplicatedCode
def insert_row(query, data):
    DB_DATABASE = config('DB_DATABASE')
    DB_HOST = config('DB_HOST')
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    conn = psycopg2.connect(database=DB_DATABASE,
                            host=DB_HOST,
                            port=5432, user=DB_USER, password=DB_PASSWORD)
    try:
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(query, data)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
