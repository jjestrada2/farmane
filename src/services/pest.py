from typing import Dict, Any


async def detect_pest(latitude: float, longitude: float) -> Dict[str, Any]:
    """Return dummy pest detection data for a given lat/lon.

    This is a placeholder. Replace with real logic that performs
    pest inference (e.g., model/classifier) and returns detection
    confidence and a preview image URL.
    """
    return {
        "detected_pest": True,
        "confidence_score": 0.87,
        "image_url": "https://example.com/tiles/pest/mock.png",
    }

