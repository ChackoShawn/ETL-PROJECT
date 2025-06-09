import requests
import pandas as pd
import sqlite3
from datetime import datetime, timezone
import os
import time

# ------------------------------
# Configuration
# ------------------------------
API_KEY = "b38a2c08-1c1f-48d5-9a2a-8473a45eae96"  # IQAir API Key

CITIES = {
    "New York": (40.7128, -74.0060),
    "London": (51.5074, -0.1278),
    "Tokyo": (35.6895, 139.6917),
    "Paris": (48.8566, 2.3522),
    "Beijing": (39.9042, 116.4074),
    "Sydney": (-33.8688, 151.2093),
    "Moscow": (55.7558, 37.6173),
    "São Paulo": (-23.5505, -46.6333),
    "Mexico City": (19.4326, -99.1332),
    "Cairo": (30.0444, 31.2357),
    "Mumbai": (19.0760, 72.8777),
    "Istanbul": (41.0082, 28.9784),
    "Toronto": (43.6532, -79.3832),
    "Seoul": (37.5665, 126.9780),
    "Bangkok": (13.7563, 100.5018),
    "Dubai": (25.276987, 55.296249),
    "Singapore": (1.3521, 103.8198),
    "Johannesburg": (-26.2041, 28.0473),
    "Berlin": (52.5200, 13.4050),
    "Buenos Aires": (-34.6037, -58.3816)
}

BASE_URL = "https://api.airvisual.com/v2/nearest_city"
DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)
CSV_PATH = os.path.join(DATA_DIR, "global_air_quality_data.csv")
DB_PATH = os.path.join(DATA_DIR, "global_air_quality.db")

# ------------------------------
# Step 1: Fetch Air Quality Data
# ------------------------------
def fetch_air_quality_data():
    records = []

    for city, (lat, lon) in CITIES.items():
        params = {
            "lat": lat,
            "lon": lon,
            "key": API_KEY
        }
        try:
            response = requests.get(BASE_URL, params=params)
            data = response.json()

            if data["status"] == "success":
                pollution = data["data"]["current"]["pollution"]
                weather = data["data"]["current"]["weather"]

                record = {
                    "city": city,
                    "aqi_us": pollution["aqius"],
                    "main_pollutant_us": pollution["mainus"],
                    "temperature": weather["tp"],
                    "humidity": weather["hu"],
                    "pressure": weather["pr"],
                    "wind_speed": weather["ws"],
                    "wind_direction": weather["wd"],
                    "timestamp": pollution["ts"],
                    "datetime": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                }
                records.append(record)
            else:
                print(f"[ERROR] API error for {city}: {data.get('message', data.get('data'))}")

        except Exception as e:
            print(f"[EXCEPTION] Error fetching data for {city}: {str(e)}")

        time.sleep(1)  # ⏳ Prevent rate limiting

    return pd.DataFrame(records)

# ------------------------------
# Step 2: Store Data
# ------------------------------
def store_data(df):
    df.to_csv(CSV_PATH, index=False)
    print(f"✅ Data saved to CSV: {CSV_PATH}")

    conn = sqlite3.connect(DB_PATH)
    df.to_sql("air_quality_data", conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ Data saved to SQLite DB: {DB_PATH}")

# ------------------------------
# Main Pipeline Execution
# ------------------------------
def run_etl():
    print("🚀 Starting Global IQAir ETL pipeline...")
    df = fetch_air_quality_data()

    if df.empty:
        print("⚠️ No data fetched. Skipping storage.")
    else:
        store_data(df)
    print("✅ ETL pipeline completed.")

# ------------------------------
# Trigger when script runs
# ------------------------------
if __name__ == "__main__":
    run_etl()
