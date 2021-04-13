import psycopg2
import multiprocessing
import pandas as pd
import numpy as np
import pandas.io.sql as sqlio
from decouple import config


def query_db(table_name="live_bike_data", attribute="dt"):
    """Return table sorted by a given attribute from a PostgreSQL database.

    :param: str table_name: A table name.
    :param: str attribute: An attribute by which to sort the table data.
    :raise: Exception: If query to the database has been unsuccessful.
    :return: A dataframe of the relevant table from the database.
    :rtype: pandas.core.frame.DataFrame
    """
    DB_HOST = config('DB_HOST')
    DB_DBNAME = config('DB_DATABASE')
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    connection = psycopg2.connect("host='{}' port='{}' dbname='{}' user='{}' password='{}'".format
                                  (DB_HOST, 5432, DB_DBNAME, DB_USER, DB_PASSWORD))
    try:
        sql = "SELECT * FROM " + table_name + " ORDER BY " + attribute + " ASC;"
        data = sqlio.read_sql_query(sql, connection)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
    return data


def count_change(index_arr, bikes_arr):
    """Count change in a list.

    :param: index_arr list: A list of dataframe indexes.
    :param: bike_arr list: A list of bike available corresponding to the given indexes.
    :return: A dictionary with index values as key and change counts as value.
    :rtype: dict
    """
    count, count_dict = 0, {}
    if len(index_arr) == 0:
        return count_dict
    if len(bikes_arr) > 1:
        for i in range(len(bikes_arr) - 1):
            count_dict[index_arr[i]] = count
            count += abs(bikes_arr[i] - bikes_arr[i + 1])
        count_dict[index_arr[-1]] = count
    else:
        count_dict[index_arr[-1]] = count
    return count_dict


def calculate_available_bikes_citywide(df, city_id, date):
    """Calculate all available bikes in each city per each moment recorded in the table.

    :param: df pandas.core.frame.DataFrame: A dataframe of bike scheme information.
    :param: city_id int: A numerical representation of each city.
    :param: date str: A dd-mm-yy date string.
    :raise: Exception if df does not contain the required attributes.
    :return: A dictionary with index values as key and city bike counts per unix moment as value.
    :rtype: dict
    """
    if pd.Series(["city_id", "dd_mm_yy", "dt", "bikes_available"]).isin(df.columns).all() is False:
        raise Exception("Required attributes not in df")
    index_list = df.loc[(df["city_id"] == city_id)
                        & (df["dd_mm_yy"] == date)].index.to_list()
    sum_ba_dict = {}    # a dictionary w/ key: dt and value: total bike available counts per city
    for dt in df['dt'].loc[(df["city_id"] == city_id)
                           & (df["dd_mm_yy"] == date)].unique():
        sum_ba_dict[dt] = sum(df['bikes_available'].loc[(df["dt"] == dt)
                                                        & (df["city_id"] == city_id)].to_list())
    total_ba_dict = {}    # a dictionary w/ key: index and value: total bikes available per city per dt
    for index in index_list:
        total_ba_dict[index] = sum_ba_dict[df.loc[[index]]["dt"].values[0]]
    return total_ba_dict


def track_station_usage_by_date(df, station_id, date):
    """Track bike usage for a each station across a given day.

    :param: df pandas.core.frame.DataFrame: A dataframe of bike scheme information.
    :param: station_id int: An integer representing individual bike stations
    :param: date str: A dd-mm-yy date string.
    :raise: Exception if df does not contain the required attributes.
    :return: A dictionary with index values as key and station usage counts as value.
    :rtype: dict
    """
    if pd.Series(["station_id", "dd_mm_yy", "bikes_available"]).isin(df.columns).all() is False:
        raise Exception("Required attributes not in df")
    index_list = df.loc[(df["station_id"] == station_id)
                        & (df["dd_mm_yy"] == date)].index.to_list()
    bikes_available_list = df.loc[(df["station_id"] == station_id)
                                  & (df["dd_mm_yy"] == date)]["bikes_available"].to_list()
    count_dict = count_change(index_list, bikes_available_list)    # calling count_change
    return count_dict


def track_city_usage_by_date(df, city_id, date):
    """Track bike usage for a each city across a given day.

    :param: df pandas.core.frame.DataFrame: A dataframe of bike scheme information.
    :param: city_id int: A numerical representation of each city.
    :param: date str: A dd-mm-yy date string.
    :raise: Exception if df does not contain the required attributes.
    :return: A dictionary with index values as key and station usage counts as value.
    :rtype: dict
    """
    if pd.Series(["city_id", "dd_mm_yy", "dt", "bikes_available"]).isin(df.columns).all() is False:
        raise Exception("Required attributes not in df")
    index_list = df.loc[(df["city_id"] == city_id)
                        & (df["dd_mm_yy"] == date)].index.to_list()
    sum_ba_dict = {}    # a dictionary w/ key: dt and value: total bike available counts per city
    for dt in df["dt"].loc[(df["city_id"] == city_id)
                           & (df["dd_mm_yy"] == date)].unique():
        sum_ba_dict[dt] = sum(df["bikes_available"].loc[(df["dt"] == dt)
                                                        & (df["city_id"] == city_id)].to_list())
    sum_ba_list = []    # sum bikes available list for total at each index
    for index in index_list:
        sum_ba_list.append(sum_ba_dict[df.loc[[index]]["dt"].values[0]])
    count_dict = count_change(index_list, sum_ba_list)    # calling count_change
    return count_dict


def pool_process(func, data, pool_size=8):
    """A function for running a job that uses a pool of processes.

    :param: func function: A function that can take a tuple as argument.
    :param: data list: A list of arguments on which func will be mapped.
    :param: pool_size int: The number of processes in the pool
    :return: A list of results return from func
    :rtype: list
    """
    pool = multiprocessing.Pool(processes=pool_size)
    output = pool.starmap(func, data)
    pool.close()
    pool.join()
    return output


def df_compile(df_1, df_2):
    """Merge two dataframes based on shared attributes,
    then generate additional time series based boolean-style attributes/

    :param: df_1 pandas.core.frame.DataFrame: A dataframe of bike scheme information.
    :param: df_2 pandas.core.frame.DataFrame: A dataframe of weather information.
    :raise: Exception if df_1 or df_2 do not contain the required attributes.
    :return: A new, merged dataframe, with additional columns amended.
    :rtype: pandas.core.frame.DataFrame
    """
    if pd.Series(["dt", "city_id"]).isin(df_1.columns).all() is False \
            or pd.Series(["dt", "city_id", "date_string"]).isin(df_2.columns).all() is False:
        raise Exception("Required attributes not in df")
    df = pd.merge_asof(df_1, df_2, on="dt", by="city_id")    # asof merge, similar to a left-join - match on nearest key
    df["date_string"] = pd.to_datetime(df["dt"], unit='s')
    # Create four attributes, each representing a stage of the day.
    df["morning"] = np.where((df["date_string"].dt.time > pd.to_datetime("05:00:00").time())
                             & (df["date_string"].dt.time < pd.to_datetime("12:00:00").time()), 1, 0)
    df["afternoon"] = np.where((df["date_string"].dt.time > pd.to_datetime("12:01:00").time())
                               & (df["date_string"].dt.time < pd.to_datetime("16:59:00").time()), 1, 0)
    df["evening"] = np.where((df["date_string"].dt.time > pd.to_datetime("17:00:00").time())
                             & (df["date_string"].dt.time < pd.to_datetime("20:00:00").time()), 1, 0)
    df["night"] = np.where((df["date_string"].dt.time > pd.to_datetime("20:01:00").time())
                           & (df["date_string"].dt.time < pd.to_datetime("23:59:59").time()), 1, 0)
    # Generate new attributes: tod, weekday, mnth, workingday, season
    df["tod"] = df["date_string"].dt.hour    # time of day
    df["weekday"] = df["date_string"].dt.dayofweek
    df["mnth"] = df["date_string"].dt.month    # month
    df["workingday"] = np.where((df["weekday"] >= 0) & (df["weekday"] <= 5), 1, 0)
    df["season"] = df["date_string"].dt.month % 12 // 3 + 1
    return df
