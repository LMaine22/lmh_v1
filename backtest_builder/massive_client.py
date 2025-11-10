from __future__ import annotations
import time, requests
from typing import Dict, Any
from .config import MASSIVE_API_BASE, MASSIVE_API_KEY, DEFAULT_TIMEOUT, MAX_RETRIES, PAGE_LIMIT

class MassiveClient:
    def __init__(self, api_key: str | None = None, base_url: str | None = None):
        self.api_key = api_key or MASSIVE_API_KEY
        self.base = (base_url or MASSIVE_API_BASE).rstrip("/")
        if not self.api_key:
            raise ValueError("Missing MASSIVE_API_KEY")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "User-Agent": "backtest_builder/1.0"
        }

    def _get(self, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        url = f"{self.base}/{path.lstrip('/')}"
        for attempt in range(1, MAX_RETRIES + 1):
            r = requests.get(url, headers=self._headers(), params=params, timeout=DEFAULT_TIMEOUT)
            if r.status_code in (200, 206):
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * attempt)
                continue
            try:
                payload = r.json()
            except Exception:
                payload = r.text
            raise RuntimeError(f"GET {url} failed {r.status_code}: {payload}")
        raise RuntimeError(f"GET {url} exhausted retries")

    def paginate(self, path: str, params: Dict[str, Any] | None = None, page_key_candidates=("next_url","next","nextPage")):
        params = dict(params or {})
        params.setdefault("limit", PAGE_LIMIT)
        data = self._get(path, params)
        yield data
        while True:
            next_url = None
            for k in page_key_candidates:
                if isinstance(data, dict) and k in data and data[k]:
                    next_url = data[k]
                    break
            if not next_url:
                break
            for attempt in range(1, MAX_RETRIES + 1):
                r = requests.get(next_url, headers=self._headers(), timeout=DEFAULT_TIMEOUT)
                if r.status_code in (200, 206):
                    data = r.json()
                    yield data
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(1.5 * attempt)
                    continue
                raise RuntimeError(f"GET {next_url} failed {r.status_code}: {r.text}")

    def list_contracts_as_of(self, underlying_ticker: str, as_of: str, limit: int = PAGE_LIMIT):
        path = "reference/options/contracts"
        params = {"underlying_ticker": underlying_ticker, "as_of": as_of, "limit": limit}
        for page in self.paginate(path, params=params):
            yield page

    def list_option_quotes(self, option_symbol: str, date: str, limit: int = PAGE_LIMIT):
        path = f"quotes/{option_symbol}"
        params = {"date": date, "limit": limit}
        for page in self.paginate(path, params=params):
            yield page

    def list_underlier_quotes(self, ticker: str, date: str, limit: int = PAGE_LIMIT):
        path = f"quotes/{ticker}"
        params = {"date": date, "limit": limit}
        for page in self.paginate(path, params=params):
            yield page

    def get_contract_details(self, option_symbol: str):
        """Fetch contract details including OI for a specific option."""
        path = f"reference/options/contracts/{option_symbol}"
        return self._get(path)
    
    def get_open_interest_bulk(self, underlying_ticker: str, date: str, limit: int = PAGE_LIMIT):
        """Fetch open interest for contracts on a specific date."""
        # Try the snapshot endpoint for OI (may have OI even if not full Greeks historically)
        path = f"snapshot/options/{underlying_ticker}"
        params = {"date": date, "limit": limit}
        for page in self.paginate(path, params=params):
            yield page
