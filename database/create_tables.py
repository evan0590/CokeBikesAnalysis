import psycopg2

sql_static_bike = "CREATE TABLE static_bike_data (scheme_id smallint, scheme_short_name varchar(10), " \
                  "station_id smallint PRIMARY KEY, docks_count smallint, name varchar(75), " \
                  "latitude numeric, longitude numeric);"

sql_live_bike = "CREATE TABLE live_bike_data(station_id smallint," \
                "bikes_available smallint, docks_available smallint, " \
                "status smallint, date_status varchar (25), dt integer);"

sql_live_weather = "CREATE TABLE live_weather_data(longitude numeric, latitude numeric, number smallint, " \
                   "weather_main varchar(20), weather_description varchar(100), weather_icon varchar(5), " \
                   "main_temp smallint, main_feels_like smallint, main_temp_min smallint, main_temp_max smallint," \
                   "main_pressure smallint, main_humidity smallint, visibility smallint, wind_speed numeric," \
                   "wind_deg numeric, clouds_all smallint, rain numeric, dt integer, sys_sunrise integer," \
                   "sys_sunset integer, timezone smallint, name varchar(15), date_string varchar (25));"


def create_tables(sql_query):
    DB_HOST = config('DB_HOST')
    DB_DBNAME = config('DB_DATABASE')
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    connection = psycopg2.connect("host='{}' port='{}' dbname='{}' user='{}' password='{}'".format
                                  (DB_HOST, 5432, DB_DBNAME, DB_USER, DB_PASSWORD))
    try:
        # create a new cursor
        cur = connection.cursor()
        # execute the INSERT statement
        cur.execute(sql_query)
        # commit the changes to the database
        connection.commit()
        # close communication with the database
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


create_tables(sql_static_bike)
create_tables(sql_live_weather)
create_tables(sql_live_bike)
