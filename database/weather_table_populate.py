import requests
import json
import datetime
from decouple import config
from utils import insert_row


limerick_id = "2962943"
cork_id = "2965140"
galway_id = "2964180"
city_list = [limerick_id, cork_id, galway_id]


def weather_scraper(city_id):
    down_times = ["1", "2", "3", "4", "5"]
    now = datetime.datetime.now()
    current_time_int = int(now.strftime("%H"))
    current_time = str(current_time_int)
    for time in down_times:
        if current_time == time:
            return None
    WEATHER_URL_2 = "https://api.openweathermap.org/data/2.5/weather"
    WEATHER_API_KEY = config('WEATHER_API_KEY')
    response = requests.get(WEATHER_URL_2, params={"id": city_id, "appid": WEATHER_API_KEY})
    # parse the data
    data = response.text
    parsed = json.loads(data)
    kelvin = 273.15
    longitude = parsed["coord"]["lon"]
    latitude = parsed["coord"]["lat"]
    number = parsed["weather"][0]["id"]
    weather_main = parsed["weather"][0]["main"]
    weather_description = parsed["weather"][0]["description"]
    weather_icon = parsed["weather"][0]["icon"]
    main_temp = round(int(parsed["main"]["temp"]) - kelvin)
    main_feels_like = round(int(parsed["main"]["feels_like"]) - kelvin)
    main_temp_min = round(int(parsed["main"]["temp_min"]) - kelvin)
    main_temp_max = round(int(parsed["main"]["temp_max"]) - kelvin)
    main_pressure = parsed["main"]["pressure"]
    main_humidity = parsed["main"]["humidity"]
    visibility = parsed["visibility"]
    wind_speed = parsed["wind"]["speed"]
    wind_deg = parsed["wind"]["deg"]
    clouds_all = parsed["clouds"]["all"]
    try:
        rain = parsed["rain"]["1h"]
    except:
        rain = 0
    dt = parsed["dt"]
    sys_sunrise = parsed["sys"]["sunset"]
    sys_sunset = parsed["sys"]["sunset"]
    timezone = parsed["timezone"]
    name = parsed["name"]
    date_string = datetime.datetime.fromtimestamp(dt)

    sql = "INSERT INTO live_weather_data(longitude, latitude, number, weather_main, weather_description, " \
          "weather_icon, main_temp, " \
          "main_feels_like, main_temp_min, main_temp_max, main_pressure, main_humidity, visibility, wind_speed," \
          "wind_deg, clouds_all, rain, dt, sys_sunrise, sys_sunset, timezone, name, date_string)" \
          "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    data = (longitude, latitude, number, weather_main, weather_description, weather_icon, main_temp, main_feels_like,
            main_temp_min,
            main_temp_max, main_pressure, main_humidity, visibility, wind_speed, wind_deg, clouds_all, rain, dt,
            sys_sunrise, sys_sunset, timezone, name, date_string)

    insert_row(sql, data)


for i in city_list:
    weather_scraper(i)
