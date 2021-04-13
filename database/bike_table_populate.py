import requests
import json
import time
import datetime
from decouple import config
from utils import insert_row


def live_bike_scraper():
    down_times = ["1", "2", "3", "4", "5"]
    now = datetime.datetime.now()
    current_time_int = int(now.strftime("%H")) + 1
    current_time = str(current_time_int)
    for i in down_times:
        if current_time == i:
            return None
    BIKE_API_KEY = config('BIKE_API_KEY')
    payload = {'key': BIKE_API_KEY, 'schemeId': '-1'}
    r = requests.post("https://data.bikeshare.ie/dataapi/resources/station/data/list", data=payload)
    data = r.text
    large_parsed = json.loads(data)
    parsed = large_parsed['data']
    for i in range(len(parsed)):
        station_id = parsed[i]['stationId']
        bikes_available = parsed[i]['bikesAvailable']
        docks_available = parsed[i]['docksAvailable']
        status = parsed[i]['status']
        date_status = parsed[i]['dateStatus']
        dt = int(time.mktime(datetime.datetime.strptime(date_status, "%d-%m-%Y %H:%M:%S").timetuple()))

        sql = "INSERT INTO live_bike_data(station_id, bikes_available, docks_available, status, date_status, " \
              "dt) VALUES(%s,%s,%s,%s,%s,%s)"

        data = (station_id, bikes_available, docks_available, status, date_status, dt)

        insert_row(sql, data)


live_bike_scraper()