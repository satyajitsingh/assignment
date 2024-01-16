import requests
import pandas as pd

# API URL
url = "https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31"
csv_file = 'daily_weather_data.csv'
paraquet_file= 'output.paraquet'
# Fetching data from the API
def retrieve_and_aggregate_data():
    try:
        response = requests.get(url)
        data = response.json()
    except TimeoutError:
        print("The request timed out ")

# Extracting hourly data
    try:
        hourly_data = data['hourly']
        time = hourly_data['time']
        temperatures = hourly_data['temperature_2m']
        rain = hourly_data['rain']
        showers = hourly_data['showers']
        visibility = hourly_data['visibility']
    except Exception:
        print("An error ocuured while extracting the data")

# Creating a DataFrame
    try:
        df = pd.DataFrame({
            'time': pd.to_datetime(time),
            'temperature': temperatures,
            'rain': rain,
            'showers': showers,
            'visibility': visibility
        })
    except Exception:
        print("An error ocuured while transforming the data")

# Setting time as the index
    df.set_index('time', inplace=True)
    daily_df = agggregate_data(df)
    
    
# Convert and safe data to praquet
    save_file(daily_df)
    print(f"Data saved to {paraquet_file}")

def agggregate_data(df):
# Resampling to daily data and aggregating
    try:
        daily_df = df.resample('D').agg({
            'temperature': 'mean',
            'rain': 'sum',
            'showers': 'sum',
            'visibility': 'mean'
        })
        return daily_df
    except Exception:
        print("An error ocuured during aggregation")

# Resetting index to include date in the CSV
    daily_df.reset_index(inplace=True)

# Saving to CSV file
def save_file(daily_df): 
    try:
        daily_df.to_csv(csv_file, index=False)
        daily_df.to_parquet(paraquet_file)
    except Exception:
        print("Exception occured during saving data")

if __name__ == "__main__":
    retrieve_and_aggregate_data()