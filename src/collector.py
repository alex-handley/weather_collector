import io
from os import environ
import time
import boto3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# SpotWX model codes
MODELS = {
    "NAM": "nam_awphys",
}

LOCATIONS = {
    "sky_pilot": {
        "lat": 49.63297,
        "lon": -123.08596,
        "tz": "America%2FVancouver"
    },
}

# Setup headless Chrome for Lambda container environment
chrome_driver_path = "/usr/bin/chromedriver"
chrome_binary_path = "/usr/bin/google-chrome-stable"

options = Options()
options.binary_location = chrome_binary_path
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--single-process")
options.add_argument("window-size=1920,1080")
options.add_argument("--remote-debugging-port=9222")  # very important
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/124.0.0.0 Safari/537.36")


# Initialize driver only when needed to avoid global initialization issues in Lambda
driver = None

def enable_extra_columns(driver):
    wait = WebDriverWait(driver, 10)

    columns_to_toggle = [
        {"name": "PTYPE", "xpath": '//*[@id="example_wrapper"]/div/button[3]/span'},
        {"name": "RQP", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[13]'},
        {"name": "SQP", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[14]'},
        {"name": "FQP", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[15]'},
        {"name": "IQP", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[16]'},
        {"name": "WS925", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[17]'},
        {"name": "WD925", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[18]'},
        {"name": "TMP850", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[19]'},
        {"name": "WS850", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[20]'},
        {"name": "WD850", "xpath": '//*[@id="example_wrapper"]/div/div[2]/div/button[21]'},
    ]

    for column in columns_to_toggle:
        print(f"Enabling column: {column['name']}")
        try:
            toggle_button = wait.until(EC.element_to_be_clickable((By.XPATH, column['xpath'])))
            toggle_button.click()
            time.sleep(1)
        except TimeoutException:
            print(f"Could not find button {column['name']}.")
        except Exception as e:
            print(f"Unexpected error: {e}")

def scrape_spotwx_table(url, model_name):
    print(f"Loading {model_name} forecast...")
    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
    except TimeoutException:
        print(f"Table not found for {model_name}.")
        return pd.DataFrame()

    enable_extra_columns(driver)
    time.sleep(3)

    tables = driver.find_elements(By.TAG_NAME, "table")
    print(f"Found {len(tables)} tables for {model_name}.")

    if not tables:
        print(f"No tables found for {model_name}.")
        return pd.DataFrame()

    table = tables[0]
    try:
        rows = table.find_elements(By.TAG_NAME, "tr")
        table_data = []
        headers = [th.text for th in rows[0].find_elements(By.TAG_NAME, "th")]

        for row in rows[1:]:
            cols = [td.text for td in row.find_elements(By.TAG_NAME, "td")]
            if cols:
                table_data.append(cols)

        df = pd.DataFrame(table_data, columns=headers if headers else None)
        print(f"Parsed table for {model_name} with {len(df)} rows.")
        print(df.head())
    except Exception as e:
        print(f"Error parsing table for {model_name}: {e}")

    return df

def persist_forecast_data(df, model_name, location):
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)

    s3 = boto3.client('s3')
    partition = f"date={datetime.today().strftime('%Y-%m-%d')}"
    prefix = f"forecasts/location={location}/model={model_name}/{partition}/data.parquet"

    s3.put_object(
        Bucket=environ.get("BUCKET"),
        Key=prefix,
        Body=buffer.getvalue()
    )

    print(f"Persisted {model_name} forecast data for {location} to S3 at {prefix}")

def lambda_handler(event, context):
    global driver

    try:
        print("Initializing Chrome WebDriver...")
        service = Service(executable_path=chrome_driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        print(driver.capabilities['browserVersion'])

        for location, loc_data in LOCATIONS.items():
            for model_name, model_code in MODELS.items():
                url = f"https://spotwx.com/products/grib_index.php?model={model_code}&lat={loc_data['lat']}&lon={loc_data['lon']}&tz={loc_data['tz']}&display=table"
                print(f"Scraping {model_name} from {url}...")

                df = scrape_spotwx_table(url, model_name)
                persist_forecast_data(df, model_name, location)

        return {"statusCode": 200, "body": "Data collection completed successfully"}

    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}

    finally:
        if driver:
            print("Closing Chrome WebDriver...")
            driver.quit()
