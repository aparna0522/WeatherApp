# Standard library imports
import os
import time
from datetime import datetime

# Third-party libraries
import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from pprint import pprint
from typing import List, Dict, Optional


def fetch_raw_weather_data_for_city(
    city: str, api_key: str, retries: int = 3, backoff_factor: float = 0.3
) -> Optional[Dict]:
    """
    Fetches raw weather data for a given city using the OpenWeatherMap API.
    Retries the request on failure up to 'retries' times with exponential backoff.
    Returns the JSON response as a dictionary, or None if the request fails.
    """
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": "metric"}

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                return data
            elif response.status_code == 404:
                print(f"City '{city}' not found. Skipping.")
                return None
            elif response.status_code == 401:
                print("Invalid API key. Please check your .env file.")
                return None
            else:
                print(
                    f"Unexpected status code {response.status_code} for city '{city}'. Response: {response.text}"
                )
                return None
        except requests.RequestException as e:
            print("Threw an Execption ", str(e))
        except requests.Timeout:
            print(f"Timeout when requesting city {city}, retrying...")
        except requests.ConnectionError:
            print(f"Connection error when requesting city {city}")
        except requests.RequestException as e:
            print(f"Request failed for city {city}: {e}")

        time.sleep(backoff_factor * (2**attempt))
    print(f"Failed to fetch data for city '{city}' after {retries} attempts.")
    return None


def extract_required_fields(data: List[Dict], city_idx: int) -> Optional[Dict]:
    """
    Extracts essential weather fields from the raw API response for a specific city index.
    Returns a dictionary with selected fields, or None if required data is missing.
    """
    try:
        item = data[city_idx]
        city = item.get("name")
        country = item.get("sys", {}).get("country")
        temp_celsius = item.get("main", {}).get("temp")
        feels_like_celsius = item.get("main", {}).get("feels_like")
        weather_main = item.get("weather", [{}])[0].get("main")
        weather_desc = item.get("weather", [{}])[0].get("description")
        humidity = item.get("main", {}).get("humidity")
        wind_speed = item.get("wind", {}).get("speed")
        timestamp_utc = item.get("dt")
        timestamp_str = datetime.utcfromtimestamp(timestamp_utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        if city and country and temp_celsius is not None:
            return {
                "city": city,
                "country": country,
                "temp_celsius": temp_celsius,
                "feels_like_celsius": feels_like_celsius,
                "weather_main": weather_main,
                "weather_desc": weather_desc,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "timestamp_utc": timestamp_str,
            }
        else:
            print(
                f"Missing essential data for index {city_idx}. Skipping printing that is why"
            )
    except Exception as e:
        print("Couldn't extract the required fields - ", str(e))


def create_weather_dataframe(data: List[Dict]) -> pd.DataFrame:
    """
    Converts a list of weather data dictionaries into a pandas DataFrame.
    Returns the resulting DataFrame.
    """
    return pd.DataFrame(data)


def plot_temps(df):
    """
    Plots a bar chart of temperatures by city using matplotlib.
    """
    plt.figure(figsize=(8, 4))
    plt.bar(df["city"], df["temp_celsius"], color="skyblue")
    plt.title("Temperatures by City")
    plt.ylabel("°C")
    plt.tight_layout()
    plt.show()


def get_weather_data(cities: List[str]) -> List[Dict]:
    """
    Fetches weather data for a list of cities.
    Loads the API key from environment variables and returns a list of raw API responses.
    """
    load_dotenv()
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("API key not found, please add api key in your .env file")
        return []

    results = []
    for city in cities:
        data = fetch_raw_weather_data_for_city(city, api_key)
        if data:
            results.append(data)

    return results


def main():
    """
    Main function to orchestrate fetching, cleaning, displaying, saving, and plotting weather data for a list of cities.
    """
    cities = ["Seattle", "Mumbai", "Los Angeles"]
    weather_data_list = get_weather_data(cities)

    for data in weather_data_list:
        print(json.dumps(data, indent=5))

    print(
        "==============================================================================================================================="
    )
    cleaned_data = []
    for index, data in enumerate(weather_data_list):
        row = extract_required_fields(weather_data_list, index)
        if row:
            pprint(json.dumps(data, indent=3))
            cleaned_data.append(row)

    print(
        "==============================================================================================================================="
    )
    print("Cleaned Data:")
    pprint(cleaned_data)

    print(
        "==============================================================================================================================="
    )
    df = create_weather_dataframe(cleaned_data)
    print(df.to_string(index=False))

    print(
        "==============================================================================================================================="
    )
    df.sort_values(by="temp_celsius", ascending=True, inplace=True)
    print(df.to_string(index=False))

    print(
        "==============================================================================================================================="
    )
    df.to_csv("weather_data.csv", index=False)
    if os.path.exists("weather_data.csv"):
        print("CSV saved!")

    print(
        "==============================================================================================================================="
    )
    df.to_json("weather_data.json", orient="records", indent=2)
    if os.path.exists("weather_data.json"):
        print("JSON saved!")

    print(
        "==============================================================================================================================="
    )
    print("Showing the plot")
    plot_temps(df)


if __name__ == "__main__":
    main()
