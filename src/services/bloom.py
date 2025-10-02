from datetime import date, timedelta
from typing import Dict, Any


async def detect_bloom(latitude: float, longitude: float) -> Dict[str, Any]:
    """Return dummy bloom detection data for a given lat/lon.

    This is a placeholder. Replace with real logic that queries
    imagery indices (e.g., EBI/NDVI) and finds the peak date.
    """
    return {
        "latitude": latitude,
        "longitude": longitude,
        "date_of_max_ebi": date.today() - timedelta(days=14),
        "ebi_value": 0.76,
        "image_url": "https://example.com/tiles/ebi/mock.png",
    }


async def predict_bloom(latitude: float, longitude: float) -> Dict[str, Any]:
    """Return dummy bloom prediction data for a given lat/lon.

    This is a placeholder. Replace with real logic that uses
    historical phenology and a predictive model.
    """
    start = date.today() + timedelta(days=10)
    peak = date.today() + timedelta(days=14)
    return {
        "latitude": latitude,
        "longitude": longitude,
        "predicted_bloom_start": start,
        "predicted_bloom_peak": peak,
        "confidence": 0.82,
    }

