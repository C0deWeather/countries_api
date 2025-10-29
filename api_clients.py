import requests
from flask import abort
from typing import List, Dict, Optional


def fetch_countries_data() -> List[Dict]:
    """
    Fetch country data from the external API.
    """
    url = (
        "https://restcountries.com/v2/all?"
        "fields=name,capital,region,population,flag,currencies"
    )

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            abort(503, description="Invalid JSON response from restcountries API")
    except requests.RequestException as e:
        print(f"Error fetching countries data: {e}")
        abort(503, description="Could not fetch data from restcountries API")

def fetch_exchange_rates(currency_code: str) -> Optional[float]:
    """
    Fetch exchange rates from the external API.
    """
    url = "https://open.er-api.com/v6/latest/USD"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("rates", {}).get(currency_code)
    except requests.RequestException as e:
        print(f"Error fetching exchange rates: {e}")
        abort(503, description="Could not fetch data from exchange rates API")