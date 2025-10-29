#!/usr/bin/env python3
from api_clients import fetch_countries_data, fetch_exchange_rates
from datetime import datetime
from flask import Flask, jsonify, abort, request
import os
from pydantic import BaseModel, Field, ValidationError
from storage import DBStorage
from utils import compute_est_gdp


app = Flask(__name__)
storage = DBStorage()

# do not sort JSON keys
app.json.sort_keys = False

class Arguments(BaseModel):
    """Model for query parameters for /countries endpoint."""

    currency_code: str | None = Field(
        default=None,
        description="Filter countries by currency code"
    )
    region: str | None = Field(
        default=None,
        description="Filter countries by region"
    )
    sort: str | None = Field(
        default=None,
        description="Sort countries by estimated GDP. "
                    "Accepted values: 'gdp_asc', 'gdp_desc'"
    )

@app.post('/countries/refresh')
def refresh():
    """
    This endpoint fetches the latest country data from the external API
    and updates the local database accordingly.
    """
    storage.reload()
    raw_data = fetch_countries_data()
    currency_code = None
    exchange_rate = None
    estimated_gdp = None

    # Fetch existing country records from db
    records = storage.get_all_countries()
    print("records is a ", type(records))
    
    if not records:
        # No existing records, perform initial population
        for entry in raw_data:
            if entry.get('currencies'):
                currency_code = entry['currencies'][0].get('code')
                exchange_rate = fetch_exchange_rates(currency_code)
                if exchange_rate:
                    estimated_gdp = compute_est_gdp(
                        entry.get('population'),
                        exchange_rate
                    )
                else:
                    estimated_gdp = None
                record = {
                    "name": entry.get('name'),
                    "region": entry.get('region'),
                    "population": entry.get('population'),
                    "currency_code": currency_code,
                    "exchange_rate": exchange_rate,
                    "estimated_gdp": estimated_gdp,
                    "flag_url": entry.get('flag'),
                    "last_refreshed_at": datetime.utcnow().isoformat() + "Z"
                }
                records.append(record)
        storage.populate_countries(records)

    else:
        # Existing records found, perform update logic
        parameters = []
        for entry in raw_data:
            if entry.get('currencies'):
                currency_code = entry['currencies'][0].get('code')
                exchange_rate = fetch_exchange_rates(currency_code)
                if exchange_rate:
                    estimated_gdp = compute_est_gdp(
                        entry.get('population'),
                        exchange_rate)
                else:
                    estimated_gdp = None
                params = {
                    "name": entry.get('name'),
                    "population": entry.get('population'),
                    "currency_code": currency_code,
                    "exchange_rate": exchange_rate,
                    "estimated_gdp": estimated_gdp,
                    "flag_url": entry.get('flag'),
                    "last_refreshed_at": datetime.utcnow().isoformat() + "Z"
                }
                parameters.append(params)
            if parameters:
                storage.update_countries(parameters)
            
    return jsonify(message="Database updated"), 201

@app.get('/countries')
def get_countries():
    """
    This endpoint retrieves country data from the local database,
    applying optional filters and sorting based on query parameters.
    """
    storage.reload()
    try:
        args = Arguments(
            currency_code=request.args.get('currency_code'),
            region=request.args.get('region'),
            sort=request.args.get('sort')
        )
    except ValidationError as e:
        abort(400, description="Invalid query parameter values or types")
    
    response = storage.query_by_filter(args.dict())
    return jsonify(response), 200

@app.get('/countries/<name>')
def get_country(name):
    """
    This endpoint retrieves a specific country's data by its name.
    """
    storage.reload()
    response = storage.fetch_country(name)
    if not response:
        abort(404, description="Country not found")
    
    return jsonify(response), 200

@app.delete('/countries/<name>')
def delete_country(name):
    """This endpoint deletes a specific country's record by its name."""
    storage.reload()

    record = fetch_country(name)
    if not record:
        abort(404, description='String not found')

    storage.delete_country(name)
    return '', 204

@app.get('/status')
def get_status():
    """Return the total number of country records in the database
    and last refreshed timestamp."""
    storage.reload()
    query = """
        SELECT COUNT(*) as total_records,
               MAX(last_refreshed_at) as last_refreshed_at
        FROM countries;
    """
    row = storage.fetchone(query)
    response = {
        "total_records": row["total_records"],
        "last_refreshed_at": row["last_refreshed_at"]
    }
    return jsonify(response), 200

def error_response(code, error, e):
    response = {}
    response["error"] = error
    response["details"] = e.description
    return jsonify(response), code

@app.errorhandler(400)
def handle_400_error(e):
    return error_response(400, "Validation failed", e)

@app.errorhandler(404)
def handle_404_error(e):
    return error_response(404, "Country not found", e)

@app.errorhandler(500)
def handle_500_error(e):
    return error_response(500, "Internal server error", e)

@app.errorhandler(503)
def handle_503_error(e):        
    return error_response(503, "External data source unavailable", e)

@app.teardown_appcontext
def close_db_connection(exception=None):
    storage.close()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
