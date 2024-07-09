import os
import requests
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime
import json
import time

# Fetch credentials from environment variables
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'weather-rawdata'
REGION = 'us-east-1'

# OpenWeatherMap API details
API_KEY = os.getenv('OPENWEATHER_API_KEY')
CITY_ID = '5131638'  # City ID for Potsdam, NY

def fetch_weather_data(city_id, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def upload_to_s3(data, bucket_name, file_name, region, aws_access_key, aws_secret_key):
    s3 = boto3.client('s3', 
                      region_name=region, 
                      aws_access_key_id=aws_access_key, 
                      aws_secret_access_key=aws_secret_key)
    try:
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=data)
        print("Upload Successful")
    except NoCredentialsError:
        print("Credentials not available")

def main():
    while True:
        try:
            # Fetch weather data
            weather_data = fetch_weather_data(CITY_ID, API_KEY)
            print("Weather Data Fetched Successfully")

            # Prepare data for upload
            timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
            file_name = f"weather_data_{timestamp}.json"
            data = json.dumps(weather_data)

            # Upload data to S3
            upload_to_s3(data, BUCKET_NAME, file_name, REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY)
        except requests.RequestException as e:
            print(f"Error fetching weather data: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

        # Sleep for 60 seconds
        time.sleep(30)

if __name__ == "__main__":
    main()
