import requests
from flask import abort


def fetch_countries_data():
    """
    Fetch country data from the external API.
    """
    url = "https://restcountries.com/v2/all?" + \
        "fields=name,capital,region,population,flag,currencies"

    try:
        response = requests.get(url, timeout=(3, 10))
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        abort(
            503,
            description=f"Could not fetch data from restcountries API"
        )

def fetch_exchange_rates(currency_code):
    """
    Fetch exchange rates from the external API.
    """
    url = f"https://open.er-api.com/v6/latest/USD"
    try:
        response = requests.get(url, timeout=(3, 10))
        response.raise_for_status()
        return response.json().get("rates", {}).get(currency_code)
    except requests.RequestException as e:
        abort(
            503,
            description=f"Could not fetch data from exchange rates API"
        )
    