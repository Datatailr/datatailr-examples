import json
import time
from datetime import datetime, timedelta, timezone

from datatailr import Blob, task

BLOB_PREFIX = "weather_analytics"

CITIES = [
    # North America (40 cities)
    {"city": "New York", "lat": 40.71, "lon": -74.01, "continent": "North America"},
    {"city": "Los Angeles", "lat": 34.05, "lon": -118.24, "continent": "North America"},
    {"city": "Chicago", "lat": 41.88, "lon": -87.63, "continent": "North America"},
    {"city": "Houston", "lat": 29.76, "lon": -95.37, "continent": "North America"},
    {"city": "Phoenix", "lat": 33.45, "lon": -112.07, "continent": "North America"},
    {"city": "Toronto", "lat": 43.65, "lon": -79.38, "continent": "North America"},
    {"city": "Mexico City", "lat": 19.43, "lon": -99.13, "continent": "North America"},
    {"city": "Montreal", "lat": 45.50, "lon": -73.57, "continent": "North America"},
    {"city": "Vancouver", "lat": 49.28, "lon": -123.12, "continent": "North America"},
    {"city": "Miami", "lat": 25.76, "lon": -80.19, "continent": "North America"},
    {"city": "San Francisco", "lat": 37.77, "lon": -122.42, "continent": "North America"},
    {"city": "Seattle", "lat": 47.61, "lon": -122.33, "continent": "North America"},
    {"city": "Denver", "lat": 39.74, "lon": -104.99, "continent": "North America"},
    {"city": "Atlanta", "lat": 33.75, "lon": -84.39, "continent": "North America"},
    {"city": "Boston", "lat": 42.36, "lon": -71.06, "continent": "North America"},
    {"city": "Dallas", "lat": 32.78, "lon": -96.80, "continent": "North America"},
    {"city": "Washington DC", "lat": 38.91, "lon": -77.04, "continent": "North America"},
    {"city": "Guatemala City", "lat": 14.63, "lon": -90.51, "continent": "North America"},
    {"city": "Havana", "lat": 23.11, "lon": -82.37, "continent": "North America"},
    {"city": "San Jose CR", "lat": 9.93, "lon": -84.08, "continent": "North America"},
    {"city": "Minneapolis", "lat": 44.98, "lon": -93.27, "continent": "North America"},
    {"city": "Detroit", "lat": 42.33, "lon": -83.05, "continent": "North America"},
    {"city": "Calgary", "lat": 51.05, "lon": -114.07, "continent": "North America"},
    {"city": "Ottawa", "lat": 45.42, "lon": -75.70, "continent": "North America"},
    {"city": "Portland", "lat": 45.52, "lon": -122.68, "continent": "North America"},
    {"city": "Las Vegas", "lat": 36.17, "lon": -115.14, "continent": "North America"},
    {"city": "Guadalajara", "lat": 20.67, "lon": -103.35, "continent": "North America"},
    {"city": "Monterrey", "lat": 25.67, "lon": -100.31, "continent": "North America"},
    {"city": "Panama City", "lat": 8.98, "lon": -79.52, "continent": "North America"},
    {"city": "San Juan", "lat": 18.47, "lon": -66.11, "continent": "North America"},
    {"city": "Kingston", "lat": 18.00, "lon": -76.79, "continent": "North America"},
    {"city": "Edmonton", "lat": 53.55, "lon": -113.49, "continent": "North America"},
    {"city": "Winnipeg", "lat": 49.90, "lon": -97.14, "continent": "North America"},
    {"city": "Salt Lake City", "lat": 40.76, "lon": -111.89, "continent": "North America"},
    {"city": "Nashville", "lat": 36.16, "lon": -86.78, "continent": "North America"},
    {"city": "Charlotte", "lat": 35.23, "lon": -80.84, "continent": "North America"},
    {"city": "Anchorage", "lat": 61.22, "lon": -149.90, "continent": "North America"},
    {"city": "Honolulu", "lat": 21.31, "lon": -157.86, "continent": "North America"},
    {"city": "Santo Domingo", "lat": 18.47, "lon": -69.90, "continent": "North America"},
    {"city": "Tegucigalpa", "lat": 14.07, "lon": -87.19, "continent": "North America"},
    # Europe (40 cities)
    {"city": "London", "lat": 51.51, "lon": -0.13, "continent": "Europe"},
    {"city": "Paris", "lat": 48.86, "lon": 2.35, "continent": "Europe"},
    {"city": "Berlin", "lat": 52.52, "lon": 13.41, "continent": "Europe"},
    {"city": "Madrid", "lat": 40.42, "lon": -3.70, "continent": "Europe"},
    {"city": "Rome", "lat": 41.90, "lon": 12.50, "continent": "Europe"},
    {"city": "Amsterdam", "lat": 52.37, "lon": 4.90, "continent": "Europe"},
    {"city": "Vienna", "lat": 48.21, "lon": 16.37, "continent": "Europe"},
    {"city": "Prague", "lat": 50.08, "lon": 14.44, "continent": "Europe"},
    {"city": "Stockholm", "lat": 59.33, "lon": 18.07, "continent": "Europe"},
    {"city": "Warsaw", "lat": 52.23, "lon": 21.01, "continent": "Europe"},
    {"city": "Lisbon", "lat": 38.72, "lon": -9.14, "continent": "Europe"},
    {"city": "Dublin", "lat": 53.35, "lon": -6.26, "continent": "Europe"},
    {"city": "Brussels", "lat": 50.85, "lon": 4.35, "continent": "Europe"},
    {"city": "Copenhagen", "lat": 55.68, "lon": 12.57, "continent": "Europe"},
    {"city": "Helsinki", "lat": 60.17, "lon": 24.94, "continent": "Europe"},
    {"city": "Oslo", "lat": 59.91, "lon": 10.75, "continent": "Europe"},
    {"city": "Zurich", "lat": 47.38, "lon": 8.54, "continent": "Europe"},
    {"city": "Athens", "lat": 37.98, "lon": 23.73, "continent": "Europe"},
    {"city": "Budapest", "lat": 47.50, "lon": 19.04, "continent": "Europe"},
    {"city": "Bucharest", "lat": 44.43, "lon": 26.10, "continent": "Europe"},
    {"city": "Barcelona", "lat": 41.39, "lon": 2.17, "continent": "Europe"},
    {"city": "Munich", "lat": 48.14, "lon": 11.58, "continent": "Europe"},
    {"city": "Milan", "lat": 45.46, "lon": 9.19, "continent": "Europe"},
    {"city": "Istanbul", "lat": 41.01, "lon": 28.98, "continent": "Europe"},
    {"city": "Moscow", "lat": 55.76, "lon": 37.62, "continent": "Europe"},
    {"city": "Saint Petersburg", "lat": 59.93, "lon": 30.32, "continent": "Europe"},
    {"city": "Kyiv", "lat": 50.45, "lon": 30.52, "continent": "Europe"},
    {"city": "Belgrade", "lat": 44.79, "lon": 20.47, "continent": "Europe"},
    {"city": "Zagreb", "lat": 45.81, "lon": 15.98, "continent": "Europe"},
    {"city": "Riga", "lat": 56.95, "lon": 24.11, "continent": "Europe"},
    {"city": "Tallinn", "lat": 59.44, "lon": 24.75, "continent": "Europe"},
    {"city": "Vilnius", "lat": 54.69, "lon": 25.28, "continent": "Europe"},
    {"city": "Edinburgh", "lat": 55.95, "lon": -3.19, "continent": "Europe"},
    {"city": "Reykjavik", "lat": 64.15, "lon": -21.94, "continent": "Europe"},
    {"city": "Marseille", "lat": 43.30, "lon": 5.37, "continent": "Europe"},
    {"city": "Hamburg", "lat": 53.55, "lon": 9.99, "continent": "Europe"},
    {"city": "Porto", "lat": 41.15, "lon": -8.61, "continent": "Europe"},
    {"city": "Krakow", "lat": 50.06, "lon": 19.94, "continent": "Europe"},
    {"city": "Sofia", "lat": 42.70, "lon": 23.32, "continent": "Europe"},
    {"city": "Minsk", "lat": 53.90, "lon": 27.57, "continent": "Europe"},
    # Asia (40 cities)
    {"city": "Tokyo", "lat": 35.68, "lon": 139.69, "continent": "Asia"},
    {"city": "Beijing", "lat": 39.91, "lon": 116.40, "continent": "Asia"},
    {"city": "Shanghai", "lat": 31.23, "lon": 121.47, "continent": "Asia"},
    {"city": "Mumbai", "lat": 19.08, "lon": 72.88, "continent": "Asia"},
    {"city": "Delhi", "lat": 28.61, "lon": 77.21, "continent": "Asia"},
    {"city": "Bangkok", "lat": 13.76, "lon": 100.50, "continent": "Asia"},
    {"city": "Singapore", "lat": 1.35, "lon": 103.82, "continent": "Asia"},
    {"city": "Seoul", "lat": 37.57, "lon": 126.98, "continent": "Asia"},
    {"city": "Jakarta", "lat": -6.21, "lon": 106.85, "continent": "Asia"},
    {"city": "Manila", "lat": 14.60, "lon": 120.98, "continent": "Asia"},
    {"city": "Hong Kong", "lat": 22.32, "lon": 114.17, "continent": "Asia"},
    {"city": "Taipei", "lat": 25.03, "lon": 121.57, "continent": "Asia"},
    {"city": "Kuala Lumpur", "lat": 3.14, "lon": 101.69, "continent": "Asia"},
    {"city": "Hanoi", "lat": 21.03, "lon": 105.85, "continent": "Asia"},
    {"city": "Osaka", "lat": 34.69, "lon": 135.50, "continent": "Asia"},
    {"city": "Dhaka", "lat": 23.81, "lon": 90.41, "continent": "Asia"},
    {"city": "Karachi", "lat": 24.86, "lon": 67.01, "continent": "Asia"},
    {"city": "Tehran", "lat": 35.69, "lon": 51.39, "continent": "Asia"},
    {"city": "Riyadh", "lat": 24.71, "lon": 46.68, "continent": "Asia"},
    {"city": "Dubai", "lat": 25.20, "lon": 55.27, "continent": "Asia"},
    {"city": "Doha", "lat": 25.29, "lon": 51.53, "continent": "Asia"},
    {"city": "Colombo", "lat": 6.93, "lon": 79.84, "continent": "Asia"},
    {"city": "Kathmandu", "lat": 27.72, "lon": 85.32, "continent": "Asia"},
    {"city": "Yangon", "lat": 16.87, "lon": 96.20, "continent": "Asia"},
    {"city": "Phnom Penh", "lat": 11.56, "lon": 104.92, "continent": "Asia"},
    {"city": "Ulaanbaatar", "lat": 47.89, "lon": 106.91, "continent": "Asia"},
    {"city": "Almaty", "lat": 43.24, "lon": 76.95, "continent": "Asia"},
    {"city": "Tashkent", "lat": 41.30, "lon": 69.28, "continent": "Asia"},
    {"city": "Tbilisi", "lat": 41.72, "lon": 44.79, "continent": "Asia"},
    {"city": "Baku", "lat": 40.41, "lon": 49.87, "continent": "Asia"},
    {"city": "Islamabad", "lat": 33.69, "lon": 73.04, "continent": "Asia"},
    {"city": "Bangalore", "lat": 12.97, "lon": 77.59, "continent": "Asia"},
    {"city": "Chennai", "lat": 13.08, "lon": 80.27, "continent": "Asia"},
    {"city": "Kolkata", "lat": 22.57, "lon": 88.36, "continent": "Asia"},
    {"city": "Ho Chi Minh City", "lat": 10.82, "lon": 106.63, "continent": "Asia"},
    {"city": "Chengdu", "lat": 30.57, "lon": 104.07, "continent": "Asia"},
    {"city": "Guangzhou", "lat": 23.13, "lon": 113.26, "continent": "Asia"},
    {"city": "Shenzhen", "lat": 22.54, "lon": 114.06, "continent": "Asia"},
    {"city": "Nagoya", "lat": 35.18, "lon": 136.91, "continent": "Asia"},
    {"city": "Sapporo", "lat": 43.06, "lon": 141.35, "continent": "Asia"},
    # South America (30 cities)
    {"city": "Sao Paulo", "lat": -23.55, "lon": -46.63, "continent": "South America"},
    {"city": "Buenos Aires", "lat": -34.60, "lon": -58.38, "continent": "South America"},
    {"city": "Rio de Janeiro", "lat": -22.91, "lon": -43.17, "continent": "South America"},
    {"city": "Lima", "lat": -12.05, "lon": -77.04, "continent": "South America"},
    {"city": "Bogota", "lat": 4.71, "lon": -74.07, "continent": "South America"},
    {"city": "Santiago", "lat": -33.45, "lon": -70.67, "continent": "South America"},
    {"city": "Caracas", "lat": 10.49, "lon": -66.88, "continent": "South America"},
    {"city": "Quito", "lat": -0.18, "lon": -78.47, "continent": "South America"},
    {"city": "Montevideo", "lat": -34.88, "lon": -56.16, "continent": "South America"},
    {"city": "La Paz", "lat": -16.50, "lon": -68.15, "continent": "South America"},
    {"city": "Asuncion", "lat": -25.26, "lon": -57.58, "continent": "South America"},
    {"city": "Medellin", "lat": 6.25, "lon": -75.56, "continent": "South America"},
    {"city": "Cali", "lat": 3.44, "lon": -76.52, "continent": "South America"},
    {"city": "Brasilia", "lat": -15.79, "lon": -47.88, "continent": "South America"},
    {"city": "Salvador", "lat": -12.97, "lon": -38.51, "continent": "South America"},
    {"city": "Fortaleza", "lat": -3.72, "lon": -38.53, "continent": "South America"},
    {"city": "Belo Horizonte", "lat": -19.92, "lon": -43.94, "continent": "South America"},
    {"city": "Curitiba", "lat": -25.43, "lon": -49.27, "continent": "South America"},
    {"city": "Recife", "lat": -8.05, "lon": -34.87, "continent": "South America"},
    {"city": "Guayaquil", "lat": -2.17, "lon": -79.92, "continent": "South America"},
    {"city": "Cordoba", "lat": -31.42, "lon": -64.18, "continent": "South America"},
    {"city": "Rosario", "lat": -32.95, "lon": -60.65, "continent": "South America"},
    {"city": "Valparaiso", "lat": -33.05, "lon": -71.62, "continent": "South America"},
    {"city": "Manaus", "lat": -3.12, "lon": -60.02, "continent": "South America"},
    {"city": "Porto Alegre", "lat": -30.03, "lon": -51.23, "continent": "South America"},
    {"city": "Santa Cruz", "lat": -17.78, "lon": -63.18, "continent": "South America"},
    {"city": "Barranquilla", "lat": 10.96, "lon": -74.78, "continent": "South America"},
    {"city": "Sucre", "lat": -19.04, "lon": -65.26, "continent": "South America"},
    {"city": "Maracaibo", "lat": 10.63, "lon": -71.64, "continent": "South America"},
    {"city": "Arequipa", "lat": -16.41, "lon": -71.54, "continent": "South America"},
    # Africa (30 cities)
    {"city": "Cairo", "lat": 30.04, "lon": 31.24, "continent": "Africa"},
    {"city": "Lagos", "lat": 6.52, "lon": 3.38, "continent": "Africa"},
    {"city": "Nairobi", "lat": -1.29, "lon": 36.82, "continent": "Africa"},
    {"city": "Johannesburg", "lat": -26.20, "lon": 28.04, "continent": "Africa"},
    {"city": "Cape Town", "lat": -33.93, "lon": 18.42, "continent": "Africa"},
    {"city": "Casablanca", "lat": 33.57, "lon": -7.59, "continent": "Africa"},
    {"city": "Addis Ababa", "lat": 9.02, "lon": 38.75, "continent": "Africa"},
    {"city": "Accra", "lat": 5.56, "lon": -0.19, "continent": "Africa"},
    {"city": "Dar es Salaam", "lat": -6.79, "lon": 39.28, "continent": "Africa"},
    {"city": "Kinshasa", "lat": -4.44, "lon": 15.27, "continent": "Africa"},
    {"city": "Luanda", "lat": -8.84, "lon": 13.23, "continent": "Africa"},
    {"city": "Algiers", "lat": 36.74, "lon": 3.08, "continent": "Africa"},
    {"city": "Tunis", "lat": 36.81, "lon": 10.17, "continent": "Africa"},
    {"city": "Dakar", "lat": 14.72, "lon": -17.47, "continent": "Africa"},
    {"city": "Abuja", "lat": 9.08, "lon": 7.49, "continent": "Africa"},
    {"city": "Khartoum", "lat": 15.55, "lon": 32.53, "continent": "Africa"},
    {"city": "Maputo", "lat": -25.97, "lon": 32.57, "continent": "Africa"},
    {"city": "Kampala", "lat": 0.35, "lon": 32.58, "continent": "Africa"},
    {"city": "Harare", "lat": -17.83, "lon": 31.05, "continent": "Africa"},
    {"city": "Lusaka", "lat": -15.39, "lon": 28.32, "continent": "Africa"},
    {"city": "Antananarivo", "lat": -18.88, "lon": 47.51, "continent": "Africa"},
    {"city": "Douala", "lat": 4.05, "lon": 9.77, "continent": "Africa"},
    {"city": "Bamako", "lat": 12.64, "lon": -8.00, "continent": "Africa"},
    {"city": "Conakry", "lat": 9.64, "lon": -13.58, "continent": "Africa"},
    {"city": "Lome", "lat": 6.17, "lon": 1.23, "continent": "Africa"},
    {"city": "Abidjan", "lat": 5.36, "lon": -4.01, "continent": "Africa"},
    {"city": "Tripoli", "lat": 32.89, "lon": 13.18, "continent": "Africa"},
    {"city": "Mogadishu", "lat": 2.05, "lon": 45.32, "continent": "Africa"},
    {"city": "Windhoek", "lat": -22.56, "lon": 17.08, "continent": "Africa"},
    {"city": "Gaborone", "lat": -24.65, "lon": 25.91, "continent": "Africa"},
    # Oceania (20 cities)
    {"city": "Sydney", "lat": -33.87, "lon": 151.21, "continent": "Oceania"},
    {"city": "Melbourne", "lat": -37.81, "lon": 144.96, "continent": "Oceania"},
    {"city": "Brisbane", "lat": -27.47, "lon": 153.03, "continent": "Oceania"},
    {"city": "Perth", "lat": -31.95, "lon": 115.86, "continent": "Oceania"},
    {"city": "Auckland", "lat": -36.85, "lon": 174.76, "continent": "Oceania"},
    {"city": "Wellington", "lat": -41.29, "lon": 174.78, "continent": "Oceania"},
    {"city": "Adelaide", "lat": -34.93, "lon": 138.60, "continent": "Oceania"},
    {"city": "Canberra", "lat": -35.28, "lon": 149.13, "continent": "Oceania"},
    {"city": "Christchurch", "lat": -43.53, "lon": 172.64, "continent": "Oceania"},
    {"city": "Hobart", "lat": -42.88, "lon": 147.33, "continent": "Oceania"},
    {"city": "Darwin", "lat": -12.46, "lon": 130.84, "continent": "Oceania"},
    {"city": "Suva", "lat": -18.14, "lon": 178.44, "continent": "Oceania"},
    {"city": "Port Moresby", "lat": -9.44, "lon": 147.18, "continent": "Oceania"},
    {"city": "Noumea", "lat": -22.28, "lon": 166.46, "continent": "Oceania"},
    {"city": "Apia", "lat": -13.83, "lon": -171.76, "continent": "Oceania"},
    {"city": "Nuku'alofa", "lat": -21.14, "lon": -175.20, "continent": "Oceania"},
    {"city": "Cairns", "lat": -16.92, "lon": 145.77, "continent": "Oceania"},
    {"city": "Gold Coast", "lat": -28.02, "lon": 153.43, "continent": "Oceania"},
    {"city": "Hamilton NZ", "lat": -37.79, "lon": 175.28, "continent": "Oceania"},
    {"city": "Townsville", "lat": -19.26, "lon": 146.82, "continent": "Oceania"},
]

DEFAULT_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "pressure_msl",
    "cloud_cover",
    "dew_point_2m",
    "apparent_temperature",
]

VARIABLE_BOUNDS = {
    "temperature_2m": (-90, 60),
    "relative_humidity_2m": (0, 100),
    "precipitation": (0, 500),
    "wind_speed_10m": (0, 200),
    "pressure_msl": (870, 1084),
    "cloud_cover": (0, 100),
    "dew_point_2m": (-90, 60),
    "apparent_temperature": (-100, 70),
}


def _save_parquet_to_blob(df, blob_path: str):
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    Blob().put(blob_path, buf.getvalue())


@task()
def ingest_stations():
    import pandas as pd
    df = pd.DataFrame(CITIES)
    df["station_id"] = range(len(df))
    df = df[["station_id", "city", "lat", "lon", "continent"]]
    _save_parquet_to_blob(df, f"{BLOB_PREFIX}/stations.parquet")
    return df


@task(memory="4g", cpu=2)
def ingest_weather(stations, days_back: int = 30, variables: list = None):
    import pandas as pd
    import requests

    if variables is None:
        variables = DEFAULT_VARIABLES

    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    variables_str = ",".join(variables)

    all_frames = []

    # Open-Meteo supports up to 100 locations per batch via comma-separated coords
    batch_size = 50
    station_records = stations.to_dict("records")
    for batch_start in range(0, len(station_records), batch_size):
        batch = station_records[batch_start:batch_start + batch_size]
        lats = ",".join(str(s["lat"]) for s in batch)
        lons = ",".join(str(s["lon"]) for s in batch)

        url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={lats}&longitude={lons}"
            f"&start_date={start_str}&end_date={end_str}"
            f"&hourly={variables_str}"
            f"&timezone=UTC"
        )

        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=120)
                resp.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)

        data = resp.json()

        results = data if isinstance(data, list) else [data]
        for i, result in enumerate(results):
            station = batch[i]
            hourly = result.get("hourly", {})
            if not hourly or "time" not in hourly:
                continue

            rows = {"time": hourly["time"]}
            for var in variables:
                rows[var] = hourly.get(var, [None] * len(hourly["time"]))

            df = pd.DataFrame(rows)
            df["station_id"] = station["station_id"]
            df["city"] = station["city"]
            df["lat"] = station["lat"]
            df["lon"] = station["lon"]
            df["continent"] = station["continent"]
            all_frames.append(df)

        time.sleep(0.5)

    result_df = pd.concat(all_frames, ignore_index=True)
    result_df["time"] = pd.to_datetime(result_df["time"])
    _save_parquet_to_blob(result_df, f"{BLOB_PREFIX}/raw_weather.parquet")
    return result_df


@task(memory="4g")
def clean_and_validate(raw_data):
    import numpy as np
    import pandas as pd

    df = raw_data.copy()
    initial_rows = len(df)

    df["quality_flags"] = ""

    for var, (low, high) in VARIABLE_BOUNDS.items():
        if var not in df.columns:
            continue
        mask = df[var].notna() & ((df[var] < low) | (df[var] > high))
        df.loc[mask, "quality_flags"] = df.loc[mask, "quality_flags"] + f"{var}_out_of_range;"
        df.loc[mask, var] = np.nan

    for var in VARIABLE_BOUNDS:
        if var not in df.columns:
            continue
        df[var] = df.groupby("station_id")[var].transform(
            lambda s: s.interpolate(method="linear", limit=6)
        )

    if "temperature_2m" in df.columns and "relative_humidity_2m" in df.columns:
        t = df["temperature_2m"]
        rh = df["relative_humidity_2m"]
        mask = t.notna() & rh.notna() & (t >= 27)
        hi = np.where(
            mask,
            -8.785 + 1.611 * t + 2.339 * rh - 0.146 * t * rh
            - 0.013 * t**2 - 0.016 * rh**2
            + 0.002 * t**2 * rh + 0.001 * t * rh**2
            - 0.000004 * t**2 * rh**2,
            np.nan,
        )
        df["heat_index"] = hi

    if "temperature_2m" in df.columns and "wind_speed_10m" in df.columns:
        t = df["temperature_2m"]
        w = df["wind_speed_10m"]
        mask = t.notna() & w.notna() & (t <= 10) & (w > 4.8)
        wc = np.where(
            mask,
            13.12 + 0.6215 * t - 11.37 * w**0.16 + 0.3965 * t * w**0.16,
            np.nan,
        )
        df["wind_chill"] = wc

    df["hour"] = df["time"].dt.hour
    df["day_of_week"] = df["time"].dt.dayofweek
    df["date"] = df["time"].dt.date.astype(str)

    df["data_completeness"] = df[list(VARIABLE_BOUNDS.keys())].notna().mean(axis=1)

    _save_parquet_to_blob(df, f"{BLOB_PREFIX}/clean_weather.parquet")
    return df


@task(memory="4g")
def compute_statistics(clean_data):
    import numpy as np
    import pandas as pd

    df = clean_data
    numeric_vars = [v for v in DEFAULT_VARIABLES if v in df.columns]

    city_daily = df.groupby(["station_id", "city", "continent", "lat", "lon", "date"])[numeric_vars].agg(
        ["mean", "min", "max", "std"]
    )
    city_daily.columns = ["_".join(col) for col in city_daily.columns]
    city_daily = city_daily.reset_index()

    city_stats = df.groupby(["station_id", "city", "continent", "lat", "lon"])[numeric_vars].agg(
        ["mean", "min", "max", "std", "median"]
    )
    city_stats.columns = ["_".join(col) for col in city_stats.columns]
    city_stats = city_stats.reset_index()

    # Temperature trend (linear regression slope in degrees C per day)
    if "temperature_2m" in df.columns:
        def calc_trend(group):
            g = group.dropna(subset=["temperature_2m"]).copy()
            if len(g) < 24:
                return np.nan
            g["hours"] = (g["time"] - g["time"].min()).dt.total_seconds() / 3600
            x = g["hours"].values
            y = g["temperature_2m"].values
            if np.std(x) == 0:
                return 0.0
            slope = np.polyfit(x, y, 1)[0]
            return slope * 24  # per day

        trends = df.groupby(["station_id", "city"]).apply(calc_trend).reset_index()
        trends.columns = ["station_id", "city", "temp_trend_per_day"]
        city_stats = city_stats.merge(trends, on=["station_id", "city"], how="left")

    if "precipitation" in df.columns:
        precip_totals = df.groupby(["station_id", "city"])["precipitation"].sum().reset_index()
        precip_totals.columns = ["station_id", "city", "total_precipitation"]
        city_stats = city_stats.merge(precip_totals, on=["station_id", "city"], how="left")

    continent_stats = df.groupby("continent")[numeric_vars].agg(
        ["mean", "min", "max", "std"]
    )
    continent_stats.columns = ["_".join(col) for col in continent_stats.columns]
    continent_stats = continent_stats.reset_index()

    _save_parquet_to_blob(city_stats, f"{BLOB_PREFIX}/statistics.parquet")
    _save_parquet_to_blob(city_daily, f"{BLOB_PREFIX}/daily_statistics.parquet")
    _save_parquet_to_blob(continent_stats, f"{BLOB_PREFIX}/continent_statistics.parquet")
    return city_stats


@task(memory="4g")
def detect_anomalies(clean_data):
    import pandas as pd

    df = clean_data
    numeric_vars = [v for v in DEFAULT_VARIABLES if v in df.columns]
    anomalies = []

    for station_id, group in df.groupby("station_id"):
        city = group["city"].iloc[0]
        continent = group["continent"].iloc[0]
        lat = group["lat"].iloc[0]
        lon = group["lon"].iloc[0]

        for var in numeric_vars:
            series = group[var].dropna()
            if len(series) < 48:
                continue
            mean = series.mean()
            std = series.std()
            if std == 0:
                continue

            z_scores = (group[var] - mean) / std
            for severity, threshold in [("severe", 3.5), ("moderate", 3.0), ("mild", 2.5)]:
                mask = z_scores.abs() > threshold
                flagged = group[mask]
                if severity == "moderate":
                    mask = mask & (z_scores.abs() <= 3.5)
                    flagged = group[mask]
                elif severity == "mild":
                    mask = mask & (z_scores.abs() <= 3.0)
                    flagged = group[mask]

                for _, row in flagged.iterrows():
                    anomalies.append({
                        "station_id": station_id,
                        "city": city,
                        "continent": continent,
                        "lat": lat,
                        "lon": lon,
                        "time": row["time"],
                        "variable": var,
                        "value": row[var],
                        "mean": mean,
                        "std": std,
                        "z_score": (row[var] - mean) / std,
                        "severity": severity,
                    })

    anomaly_df = pd.DataFrame(anomalies) if anomalies else pd.DataFrame(
        columns=["station_id", "city", "continent", "lat", "lon", "time",
                 "variable", "value", "mean", "std", "z_score", "severity"]
    )

    anomaly_df = anomaly_df.drop_duplicates(subset=["station_id", "time", "variable"])
    _save_parquet_to_blob(anomaly_df, f"{BLOB_PREFIX}/anomalies.parquet")
    return anomaly_df


@task()
def aggregate_and_store(statistics, anomalies) -> dict:
    anomaly_summary = {}
    if not anomalies.empty and "severity" in anomalies.columns:
        for sev in ["mild", "moderate", "severe"]:
            anomaly_summary[sev] = int((anomalies["severity"] == sev).sum())
        anomaly_summary["total"] = int(len(anomalies))

        if "variable" in anomalies.columns:
            anomaly_summary["by_variable"] = anomalies["variable"].value_counts().to_dict()
        if "continent" in anomalies.columns:
            anomaly_summary["by_continent"] = anomalies["continent"].value_counts().to_dict()

    metadata = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_stations": int(statistics["station_id"].nunique()) if "station_id" in statistics.columns else 0,
        "total_records_in_stats": int(len(statistics)),
        "total_anomalies": int(len(anomalies)),
        "anomaly_summary": anomaly_summary,
        "continents": sorted(statistics["continent"].unique().tolist()) if "continent" in statistics.columns else [],
        "status": "completed",
    }

    Blob().put(f"{BLOB_PREFIX}/run_metadata.json", json.dumps(metadata, indent=2))
    return metadata
