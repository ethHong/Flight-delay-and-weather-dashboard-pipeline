from meteostat import Hourly, Daily
from meteostat import Stations
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import os

# add confiruration for hourly and daily
print("🔃Loading flight data to get target airport and date range...")
airplane_data = pd.read_csv("airline_delay_cancellation_data.csv")

start_ts = airplane_data["FL_DATE"].min()
end_ts = airplane_data["FL_DATE"].max()
end_ts_dt = pd.to_datetime(end_ts)
# Add 3 days
end_ts_plus_3 = end_ts_dt + pd.Timedelta(days=3)
# Convert back to string
end_ts = end_ts_plus_3.strftime("%Y-%m-%d")


start_dt = datetime.strptime(start_ts, "%Y-%m-%d")
end_dt = datetime.strptime(end_ts, "%Y-%m-%d")

airport_id = airplane_data["ORIGIN"].unique()

"""
print("Loading airport ids from txt file...")
with open("airport_ids.txt", "r") as f:
    airport_id = [line.strip() for line in f.readlines()]
"""

# code_map = pd.read_csv("iata-icao.csv")

errors = []

stations = Stations()
stations = stations.fetch()

code_map = pd.read_csv("airports.csv")[["code", "icao"]]
code_map = dict(zip(code_map["code"], code_map["icao"]))


def get_weather_data(iata, start, end):

    icao = code_map[iata]

    stations = Stations()
    stations = stations.fetch()

    select_station = stations[stations["icao"] == icao]
    id = select_station.index.values[0]

    data = Hourly(id, start, end)
    data = data.fetch()
    # make weather_data directory if not exists

    return data


# Get weather data for all stations
error = []
df = pd.DataFrame()

for airport in tqdm(airport_id):

    print(f"Getting data for {airport}")

    try:
        data = get_weather_data(airport, start_dt, end_dt)
        data["airport"] = airport
        df = pd.concat([df, data])
        print(f"✅Successcully fetched {airport}! and concatenated to df")
    except:
        error.append(airport)
        print(f"❌Error fetching {airport}!")

df.to_csv(f"merged_weather.csv")

# export error as comma seperated string
error_str = ",\n".join(error)
with open("error.txt", "w") as f:
    f.write(error_str)
    print("Error file created!")
