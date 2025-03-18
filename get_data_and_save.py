import kagglehub
import os
import pandas as pd
import argparse
from tqdm.auto import tqdm
from geopy.geocoders import Nominatim
import gc
import requests
from bs4 import BeautifulSoup


# Use geocoder package to code lat / long to city, state, country

tqdm.pandas()

parser = argparse.ArgumentParser()
parser.add_argument("--start_year", type=int, default=2018)

args = parser.parse_args()

print("🚀Downloading Flight Data from Kaggle")
# Download latest version
path = kagglehub.dataset_download(
    "yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018"
)
start_year = args.start_year
print("Path to dataset files:", path)
files = os.listdir(path)

# Only from start_year
get_files = [file for file in files if int(file.split(".")[0]) >= start_year]

dfs = []
# load and concat all files in files

for file in tqdm(get_files):
    print(f"🔃Loading file {file}")
    chunk_size = 50000  # Adjust based on memory
    for chunk in pd.read_csv(os.path.join(path, file), chunksize=chunk_size):
        dfs.append(chunk)

df = pd.concat(dfs, ignore_index=True)
df["FL_DATE"] = pd.to_datetime(df["FL_DATE"])
################################################################################################
# Subset data for airport whitelist

print("🔄Filtering data for airport whitelist")
airport_filter = pd.read_csv("airport_whitelist.csv")["ORIGIN"].unique()
df = df[df["ORIGIN"].isin(airport_filter)]

################################################################################################
# Load airport info masterfile

# Get carrier codes
# if codes_to_carrier.csv not exists, make it
if os.path.exists("codes_to_carrier.csv"):
    print("🔄Loading carrier codes from codes_to_carrier.csv")
    codes_to_carrier = pd.read_csv("codes_to_carrier.csv")
else:  # Collect data from wikipedia
    print("🚀Downloading carrier codes from wikipedia")
    carrier_codes_url = (
        "https://en.wikipedia.org/wiki/List_of_airlines_of_the_United_States"
    )
    response = requests.get(carrier_codes_url)
    soup = BeautifulSoup(response.content, "html.parser")

    airline_tables = soup.findAll("table", class_="wikitable sortable")
    codes_to_carrier = {}

    for table in airline_tables:
        airlines = table.findAll("tr")[1:]
        for i in airlines:
            iata, icao, name = (i.findAll("td")[j].text.strip() for j in range(2, 5))
            codes_to_carrier[iata] = name

    # 2 missing carriers manually
    codes_to_carrier["VX"] = "Virgin America"
    codes_to_carrier["EV"] = "ExpressJet Airlines"

    codes_to_carrier = pd.DataFrame(
        codes_to_carrier.items(), columns=["Code", "Carrier"]
    )

    codes_to_carrier.to_csv("codes_to_carrier.csv", index=False)


code_to_carrier = dict(zip(codes_to_carrier["Code"], codes_to_carrier["Carrier"]))


def get_carrier_name(code):
    return code_to_carrier.get(code, None)


tqdm.pandas()
# Get carrier name
print("🔄Getting carrier name for each airline code")
df["OP_CARRIER_NAME"] = df["OP_CARRIER"].progress_map(get_carrier_name)


print("🚀Downloading airports.csv")
# Try loading airports.csv if file exist, else, download from github and process for city, states
if os.path.exists("airports.csv"):
    airport_info = pd.read_csv("airports.csv")
else:
    url = "https://raw.githubusercontent.com/lxndrblz/Airports/main/airports.csv"
    airport_info = pd.read_csv(url)
    # airport_info.to_csv("airports.csv", index=False)
    airport_info = airport_info[airport_info["country"] == "US"]
    # Filter airports
    airport_info = airport_info[airport_info["code"].isin(airport_filter)]

    # Update city, state, country based on coordinate - airport.csv have omitted cite/staet/country
    print("🔄Updating city, state, country based on coordinate, if they are null")

    geolocator = Nominatim(user_agent="my_app", timeout=3)

    def get_city_state_country(lat, lon):
        location = geolocator.reverse((lat, lon), language="en")
        if location:
            address = location.raw.get("address", {})
            return (
                address.get("city", None),
                address.get("state", None),
                address.get("country", None),
                address.get("town", None),
            )
        return None, None, None, None

    missing_mask = (
        airport_info["city"].isna()
        | airport_info["state"].isna()
        | airport_info["country"].isna()
    )
    missing_rows = airport_info[missing_mask]

    for idx, row in tqdm(
        missing_rows.iterrows(), total=len(missing_rows), desc="Fetching locations"
    ):
        city, state, country, town = get_city_state_country(
            row["latitude"], row["longitude"]
        )
        # Update only if values are missing
        if pd.isna(row["city"]) and city:
            airport_info.at[idx, "city"] = city
            # If 'city' variable is None, use town
        elif pd.isna(row["city"]) and not city:
            airport_info.at[idx, "city"] = town

        if pd.isna(row["state"]) and state:
            airport_info.at[idx, "state"] = state
        if pd.isna(row["country"]) and country:
            airport_info.at[idx, "country"] = country

    airport_info.to_csv("airports.csv", index=False)


################################################################################################
# Additional featrure processing
tqdm.pandas()
airport_dict = {
    code: {
        "time_zone": time_zone,
        "city": city,
        "state": state,
        "latitude": latitude,
        "longitude": longitude,
        "country": country,
        "airport_name": name,
    }
    for code, time_zone, city, state, latitude, longitude, country, name in zip(
        airport_info["code"],
        airport_info["time_zone"],
        airport_info["city"],
        airport_info["state"],
        airport_info["latitude"],
        airport_info["longitude"],
        airport_info["country"],
        airport_info["name"],
    )
}

features = [
    "time_zone",
    "city",
    "state",
    "country",
    "longitude",
    "latitude",
    "airport_name",
]

for feature in tqdm(features):
    print(f"⏱️Processing {feature} for ORIGIN...")
    df[f"{feature.upper()}_ORIGIN"] = df["ORIGIN"].map(
        lambda x: airport_dict.get(x, {}).get(feature, None)
    )

    print(f"⏱️Processing {feature} for DESTINATION...")
    df[f"{feature.upper()}_DEST"] = df["DEST"].map(
        lambda x: airport_dict.get(x, {}).get(feature, None)
    )

print("✅Completed preprocessing acquired data!")
print("📊Sample data:", "\n", df.head(10))

print("💾Saving merged file")
df.to_csv("airline_delay_cancellation_data.csv")

# Export station_ids to txt
print("💾Exporting airport ids to txt file")
airpot_id = df["ORIGIN"].unique()
with open("airport_ids.txt", "w") as f:
    for airport in airpot_id:
        f.write(airport + "\n")
    print("✈️Airport ids exported to airport_ids.txt")

# Free up RAM after saving
del df
gc.collect()
