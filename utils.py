import random

def compute_est_gdp(population: int, exchange_rate: float) -> float:
    """Compute estimated GDP based on population
    and a random value."""

    if population is None or exchange_rate is None:
        return None

    base_gdp = population * random.uniform(1000, 2000)

    # express gdp in USD
    est_gdp = base_gdp / exchange_rate
    return round(est_gdp, 2)
