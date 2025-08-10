import asyncio
import json
import io
import types
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from datetime import datetime, timezone

import src.collector as mod


# ----------------------------- Fixtures --------------------------------------


@pytest.fixture(autouse=True)
def env(monkeypatch):
    # Minimal but realistic env payloads
    locations = {
        "sky_pilot": {"lat": 49.63, "lon": -123.09, "tz": "America%2FVancouver"},
    }
    models = {
        "NAM": "nam",
        "ICON": "icon",
    }
    monkeypatch.setenv("LOCATIONS", json.dumps(locations))
    monkeypatch.setenv("MODELS", json.dumps(models))
    monkeypatch.setenv("FORECASTS_URL", "https://example.com/spotwx")
    monkeypatch.setenv("BUCKET", "test-bucket")

    # Reload module env vars (only needed if values were imported at import-time)
    mod.LOCATIONS = json.dumps(locations)
    mod.MODELS = json.dumps(models)
    mod.FORECASTS_URL = "https://example.com/spotwx"

    return


class DummyLocator:
    def __init__(self, xp, visible=True, should_timeout=False):
        self.xp = xp
        self.visible = visible
        self.should_timeout = should_timeout
        self.clicked = False

    @property
    def first(self):
        # Playwright returns a Locator; for our dummy, it's just itself
        return self

    async def wait_for(self, state="visible", timeout=5000):
        if self.should_timeout:
            raise mod.PlaywrightTimeoutError("timeout locating " + self.xp)

    async def click(self):
        self.clicked = True


class DummyPage:
    def __init__(self):
        self._locators = {}
        self._table_visible = True
        self._headers = ["Time", "tmp", "rh"]
        self._rows = [
            ["2025-08-08 12:00Z", "15.2", "70"],
            ["2025-08-08 15:00Z", "17.1", "60"],
        ]

    def locator(self, sel):
        # Return a cached locator or create one
        should_timeout = sel.startswith("xpath=") and "button[21]" in sel
        loc = self._locators.get(sel) or DummyLocator(
            sel, should_timeout=should_timeout
        )
        self._locators[sel] = loc
        return loc

    async def wait_for_timeout(self, ms):
        return

    async def goto(self, url, timeout=20000, wait_until="domcontentloaded"):
        return

    @property
    def first(self):
        return self

    async def evaluate(self, script):
        if not self._table_visible:
            return {"headers": [], "rows": []}
        return {"headers": self._headers, "rows": self._rows}


class DummyContext:
    def __init__(self, page):
        self._page = page

    async def new_page(self):
        return self._page

    async def close(self):
        return


class DummyBrowser:
    def __init__(self, page):
        self._page = page

    async def new_context(self, user_agent=None, viewport=None):
        return DummyContext(self._page)

    async def close(self):
        return


class DummyPlaywright:
    def __init__(self, page):
        self.chromium = types.SimpleNamespace(
            launch=lambda headless=True, args=None: DummyBrowser(page)
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def fake_playwright(monkeypatch):
    page = DummyPage()

    class DummyPlaywright:
        def __init__(self, page):
            async def _launch(headless=True, args=None):
                return DummyBrowser(page)

            self.chromium = types.SimpleNamespace(launch=_launch)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def _apw():
        return DummyPlaywright(page)

    monkeypatch.setattr(mod, "async_playwright", _apw)
    return page

    # IMPORTANT: this must be a normal function returning an async context manager
    def _apw():
        return DummyPlaywright(page)

    monkeypatch.setattr(mod, "async_playwright", _apw)
    return page


@pytest.fixture
def fake_s3(monkeypatch):
    """
    Patch boto3.client('s3') to a fake that records put_object calls.
    """
    calls = []

    class FakeS3:
        def put_object(self, Bucket, Key, Body):
            calls.append({"Bucket": Bucket, "Key": Key, "Body": Body})

    def fake_client(name):
        assert name == "s3"
        return FakeS3()

    monkeypatch.setattr(mod, "boto3", types.SimpleNamespace(client=fake_client))
    return calls


# ---------------------------- Tests: enable_extra_columns ---------------------


@pytest.mark.asyncio
async def test_enable_extra_columns_clicks_some(monkeypatch):
    page = DummyPage()
    # Make one of the xpaths time out (button[21] handled in DummyPage.locator)
    await mod.enable_extra_columns(page)

    # Ensure at least one click happened and timeouts were tolerated
    clicked = [loc for loc in page._locators.values() if loc.clicked]
    assert len(clicked) >= 1  # clicked several buttons
    # And at least one should have been set to timeout in DummyPage
    timeouts = [loc for loc in page._locators.values() if loc.should_timeout]
    assert len(timeouts) >= 1


# ---------------------------- Tests: scrape_spotwx_table ----------------------


@pytest.mark.asyncio
async def test_scrape_spotwx_table_happy_path(monkeypatch):
    page = DummyPage()
    df = await mod.scrape_spotwx_table(page, "https://example.com/x", "NAM")
    assert not df.empty
    # Header normalization
    assert "forecast_time" in df.columns
    assert "tmp" in df.columns
    assert len(df) == 2


@pytest.mark.asyncio
async def test_scrape_spotwx_table_no_table(monkeypatch):
    page = DummyPage()
    # Force no rows back from evaluate
    page._table_visible = False
    df = await mod.scrape_spotwx_table(page, "https://example.com/x", "NAM")
    assert df.empty


# -------------------------- Tests: persist_forecast_data ----------------------


def test_persist_forecast_data_writes_parquet(fake_s3, monkeypatch):
    # Build a minimal DataFrame with expected columns
    df = pd.DataFrame(
        {
            "forecast_time": ["2025-08-08 12:00Z", "2025-08-08 15:00Z"],
            "tmp": ["15.2", "17.1"],
            "rh": ["70", "60"],
        }
    )

    # Freeze time to a known collected_time for deterministic key
    fixed_dt = datetime(2025, 8, 8, 22, 16, 24, tzinfo=timezone.utc)
    monkeypatch.setattr(
        mod,
        "datetime",
        types.SimpleNamespace(
            now=lambda tz=None: fixed_dt,
            today=lambda: fixed_dt,
            strptime=datetime.strptime,
            timezone=timezone,
        ),
    )

    mod.persist_forecast_data(df.copy(), "NAM", "sky_pilot")

    assert len(fake_s3) == 1
    call = fake_s3[0]
    assert call["Bucket"] == "test-bucket"
    # Key shape: raw_forecasts/location=.../model=.../date=YYYY-MM-DD/YYYY-MM-DD_HH-MM-SSZ.parquet
    assert call["Key"].startswith(
        "raw_forecasts/location=sky_pilot/model=nam/date=2025-08-08/"
    )
    assert call["Key"].endswith(".parquet")

    # Validate the parquet buffer is readable and schema reasonable
    buf = io.BytesIO(call["Body"])
    table = pq.read_table(buf)
    cols = table.column_names
    for expected in ["forecast_time", "tmp", "rh", "collected_time"]:
        assert expected in cols

    # forecast_time should be tz-naive (stored as timestamp[ms] w/o tz)
    assert pa.types.is_timestamp(table.schema.field("forecast_time").type)


def test_persist_forecast_data_handles_empty_df(fake_s3):
    mod.persist_forecast_data(pd.DataFrame(), "NAM", "sky_pilot")
    assert fake_s3 == []


# ------------------------------- Tests: run_job --------------------------------


@pytest.mark.asyncio
async def test_run_job_invokes_scrape_and_persist(monkeypatch, fake_playwright):
    # Track calls to persist_forecast_data
    calls = []

    def fake_persist(df, model_name, location):
        calls.append((model_name, location, len(df)))

    # Return fixed DF from scraper
    async def fake_scrape(page, url, model_name):
        return pd.DataFrame(
            {"forecast_time": ["2025-08-08 12:00Z"], "tmp": ["10.0"], "rh": ["50"]}
        )

    monkeypatch.setattr(mod, "persist_forecast_data", fake_persist)
    monkeypatch.setattr(mod, "scrape_spotwx_table", fake_scrape)

    # Speed up sleeps
    monkeypatch.setattr(mod, "MIN_SLEEP_TIME", 0)
    monkeypatch.setattr(mod, "MAX_SLEEP_TIME", 0)

    await mod.run_job()

    # We have 1 location x 2 models = 2 persist calls
    assert len(calls) == 2
    assert {c[0] for c in calls} == {"NAM", "ICON"}
    assert {c[1] for c in calls} == {"sky_pilot"}
    assert all(c[2] == 1 for c in calls)  # 1-row DF each


# ---------------------------- Tests: lambda_handler ----------------------------


def test_lambda_handler_success(monkeypatch):
    async def noop():
        return

    monkeypatch.setattr(mod, "run_job", noop)
    resp = mod.lambda_handler({}, {})
    assert resp["statusCode"] == 200
    assert "Data collection completed successfully" in resp["body"]


def test_lambda_handler_failure(monkeypatch):
    async def boom():
        raise RuntimeError("nope")

    monkeypatch.setattr(mod, "run_job", boom)
    resp = mod.lambda_handler({}, {})
    assert resp["statusCode"] == 500
    assert "Error:" in resp["body"]
