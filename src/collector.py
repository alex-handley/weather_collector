import io
import json
import os
import asyncio
import time
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)
import random
import time

MODELS = os.environ.get("MODELS", {})
LOCATIONS = os.environ.get("LOCATIONS", {})
FORECASTS_URL = os.environ.get("FORECASTS_URL", "")

# Define the minimum and maximum sleep durations in seconds
MIN_SLEEP_TIME = 3.0
MAX_SLEEP_TIME = 10.0


BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--no-zygote",
    "--disable-software-rasterizer",
    "--single-process",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


async def enable_extra_columns(page):
    # same XPaths as Selenium version
    xpaths = [
        '//*[@id="example_wrapper"]/div/button[3]/span',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[13]',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[14]',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[15]',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[16]',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[17]',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[18]',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[19]',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[20]',
        '//*[@id="example_wrapper"]/div/div[2]/div/button[21]',
    ]
    for xp in xpaths:
        try:
            btn = page.locator(f"xpath={xp}")
            await btn.wait_for(state="visible", timeout=5000)
            await btn.click()
            await page.wait_for_timeout(250)  # brief pause for table redraw
        except PlaywrightTimeoutError:
            print(f"Could not find toggle button {xp}")
        except Exception as e:
            print(f"Unexpected error clicking {xp}: {e}")


async def scrape_spotwx_table(page, url, model_name) -> pd.DataFrame:
    print(f"Loading {model_name} forecast...")
    try:
        await page.goto(url, timeout=20000, wait_until="domcontentloaded")
    except PlaywrightTimeoutError:
        print(f"Timeout navigating to {url}")
        return pd.DataFrame()

    # Wait for any table to appear
    table_locator = page.locator("table").first
    try:
        await table_locator.wait_for(state="visible", timeout=10000)
    except PlaywrightTimeoutError:
        print(f"Table not found for {model_name}")
        return pd.DataFrame()

    # Enable requested columns
    await enable_extra_columns(page)

    # Regrab table after UI changes
    table_locator = page.locator("table").first

    # Extract header and rows in-page for speed
    try:
        data = await page.evaluate(
            """
            () => {
              const tbl = document.querySelector('table');
              if (!tbl) return {headers: [], rows: []};
              const headers = Array.from(tbl.querySelectorAll('thead th, tr th')).map(th => th.innerText.trim());
              const rows = Array.from(tbl.querySelectorAll('tbody tr')).map(tr =>
                Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
              );
              // Fallback if no thead
              if (headers.length === 0) {
                const first = tbl.querySelector('tr');
                if (first) {
                  headers.push(...Array.from(first.querySelectorAll('th')).map(th => th.innerText.trim()));
                }
              }
              return {headers, rows};
            }
            """
        )
    except Exception as e:
        print(f"Error extracting table via JS for {model_name}: {e}")
        return pd.DataFrame()

    headers = data.get("headers") or None

    headers = [h.lower().strip() for h in headers] if headers else None
    if headers:
        headers[0] = "forecast_time"

    rows = data.get("rows") or []
    if not rows:
        print(f"No rows parsed for {model_name}")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(rows, columns=headers if headers else None)
        print(f"Parsed table for {model_name} with {len(df)} rows.")
        return df
    except Exception as e:
        print(f"Error building DataFrame for {model_name}: {e}")
        return pd.DataFrame()


def persist_forecast_data(df: pd.DataFrame, model_name: str, location: str):
    if df is None or df.empty:
        print(f"No data to persist for {model_name} / {location}")
        return

    collected_time = datetime.now(timezone.utc)
    df["collected_time"] = collected_time
    df["forecast_time"] = pd.to_datetime(df["forecast_time"], utc=True).dt.tz_localize(
        None
    )

    numeric_cols = ["tmp", "dpt", "apcp", "slp", "rqp", "sqp", "fqp", "iqp", "tmp850"]
    int_cols = ["rh", "ws", "wd", "wg", "cloud", "ws925", "wd925", "ws850"]

    for col in numeric_cols:
        try:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
        except KeyError:
            print(f"Column {col} not found in DataFrame for {model_name} / {location}")

    for col in int_cols:
        try:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
        except KeyError:
            print(f"Column {col} not found in DataFrame for {model_name} / {location}")

    table = pa.Table.from_pandas(df, schema=None, preserve_index=False)
    buffer = io.BytesIO()

    pq.write_table(
        table,
        buffer,
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
        use_deprecated_int96_timestamps=False,
    )

    s3 = boto3.client("s3")
    date = f"date={datetime.today().strftime('%Y-%m-%d')}"
    key = f"raw_forecasts/location={location}/model={model_name.lower()}/{date}/{collected_time.strftime('%Y-%m-%d_%H-%M-%SZ')}.parquet"
    s3.put_object(Bucket=os.environ.get("BUCKET"), Key=key, Body=buffer.getvalue())

    print(f"Persisted {model_name} forecast data for {location} to S3 at {key}")


async def run_job():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=BROWSER_ARGS)
        context = await browser.new_context(
            user_agent=USER_AGENT, viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        for location, loc_data in json.loads(LOCATIONS).items():
            for model_name, model_code in json.loads(MODELS).items():
                time.sleep(random.uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME))

                url = (
                    f"{FORECASTS_URL}?model={model_code}"
                    f"&lat={loc_data['lat']}&lon={loc_data['lon']}&tz={loc_data['tz']}&display=table"
                )
                print(f"Scraping {model_name} from {url}...")
                df = await scrape_spotwx_table(page, url, model_name)
                persist_forecast_data(df, model_name, location)

        await context.close()
        await browser.close()


def lambda_handler(event, context):
    start = time.time()
    try:
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
        asyncio.run(run_job())
        took = round(time.time() - start, 2)
        return {
            "statusCode": 200,
            "body": f"Data collection completed successfully in {took}s",
        }
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
