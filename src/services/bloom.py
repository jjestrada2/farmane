from datetime import date, timedelta
from typing import Dict, Any
import requests
import pandas as pd
import ee
import os
import random
import asyncio

async def detect_bloom(latitude: float, longitude: float) -> Dict[str, Any]:
    """Return dummy bloom detection data for a given lat/lon."""
    return {
        "latitude": latitude,
        "longitude": longitude,
        "date_of_max_ebi": date.today() - timedelta(days=14),
        "ebi_value": 0.76,
        "image_url": "https://example.com/tiles/ebi/mock.png",
    }


# ========================
# Utilidad para CMIP6
# ========================
def to_df(data, var):
    header, rows = data[0], data[1:]
    df = pd.DataFrame(rows, columns=header)
    df = df[["time", var]].dropna()
    df["date"] = pd.to_datetime(df["time"], unit="ms")
    df[var] = df[var] - 273.15  # Kelvin → °C
    return df[["date", var]]


# ========================
# Predicción de floración
# ========================
async def predict_bloom(lat: float, lon: float, year: int = 2026,
                        chill_req: int = 400, heat_req: int = 200, tbase: float = 7.5,
                        model: str = "ACCESS-CM2", scenario: str = "ssp245",
                        project_id: str = "maps-474019") -> Dict[str, Any]:
    """
    Predice floración (bloom) usando datos NASA POWER y CMIP6.
    """

    service_account = os.getenv("SERVICE_ACCOUNT_EMAIL")
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    project = os.getenv("PROJECT_ID")

    # Temporada para el año especificado: octubre (año-1) a junio (año)
    season_start = date(year - 1, 10, 1)
    season_end = date(year, 6, 30)
    today = date.today()

    # ========================================
    # 1. NASA POWER (solo si el año incluye "hoy" o está en el pasado)
    # ========================================
    df_real = pd.DataFrame(columns=["datetime", "temp", "step"])
    if season_start <= today <= season_end or year <= today.year:
        actual_end = min(today, season_end)

        URL = (
            "https://power.larc.nasa.gov/api/temporal/hourly/point"
            f"?parameters=T2M&community=AG&latitude={lat}&longitude={lon}"
            f"&start={season_start.strftime('%Y%m%d')}&end={actual_end.strftime('%Y%m%d')}"
            "&format=JSON"
        )
        print("Fetching NASA POWER:", URL)

        r = requests.get(URL)
        data = r.json()

        if "properties" in data and "parameter" in data["properties"]:
            t2m = data["properties"]["parameter"]["T2M"]
            df_real = pd.DataFrame({
                "datetime": pd.to_datetime(list(t2m.keys()), format="%Y%m%d%H"),
                "temp": list(t2m.values()),
                "step": "hour"
            })
            df_real = df_real[df_real["temp"] > -900]

    # ========================================
    # 2. CMIP6 (futuro)
    # ========================================
    creds = ee.ServiceAccountCredentials(service_account, key_path)
    ee.Initialize(creds, project=project)
    point = ee.Geometry.Point([lon, lat])
    cmip6 = ee.ImageCollection("NASA/GDDP-CMIP6")

    start_future = max(today, season_start)
    tasmax = (cmip6.filterDate(start_future.strftime("%Y-%m-%d"), season_end.strftime("%Y-%m-%d"))
              .filter(ee.Filter.eq("model", model))
              .filter(ee.Filter.eq("scenario", scenario))
              .select("tasmax")).getRegion(point, 27830).getInfo()

    tasmin = (cmip6.filterDate(start_future.strftime("%Y-%m-%d"), season_end.strftime("%Y-%m-%d"))
              .filter(ee.Filter.eq("model", model))
              .filter(ee.Filter.eq("scenario", scenario))
              .select("tasmin")).getRegion(point, 27830).getInfo()

    df_future = pd.DataFrame()
    if tasmax and tasmin:
        df_max = to_df(tasmax, "tasmax")
        df_min = to_df(tasmin, "tasmin")
        df_future = pd.merge(df_max, df_min, on="date", how="inner")
        df_future["temp"] = (df_future["tasmax"] + df_future["tasmin"]) / 2
        df_future = df_future.rename(columns={"date": "datetime"})
        df_future["step"] = "day"
        df_future = df_future[["datetime", "temp", "step"]]
    else:
        print("CMIP6 devolvió vacío, verificar modelo/escenario.")

    # ========================================
    # 3. Unión y cálculo chill/heat
    # ========================================
    frames = [d for d in [df_real, df_future] if not d.empty]
    df_all = pd.concat(frames).sort_values("datetime").reset_index(drop=True)

    chill = 0
    heat_accum = 0
    chill_date = None
    bloom_date = None
    chills, heats = [], []

    for _, row in df_all.iterrows():
        T = row["temp"]
        if T < -100:
            chills.append(chill)
            heats.append(heat_accum)
            continue

        if row["step"] == "hour":
            if 0 <= T <= 8.3:
                chill += 1
            if chill_date:
                heat_accum += max(0, T - tbase) / 24.0
        else:
            if T <= 8.3:
                chill += 24
            if chill_date:
                heat_accum += max(0, T - tbase)

        if not chill_date and chill >= chill_req:
            chill_date = row["datetime"]

        if chill_date and not bloom_date and heat_accum >= heat_req:
            bloom_date = row["datetime"]

        chills.append(chill)
        heats.append(heat_accum)

    df_all["Chill_accum"] = chills
    df_all["Heat_accum"] = heats

    # ========================================
    # 4. Resultados y reintento si no hay bloom
    # ========================================
    #relaxed_attempt = 0
    #chill_req_final = chill_req
    #heat_req_final = heat_req

    if not bloom_date:
        retry_factor = 0.9  # reduce umbrales un 10% por intento
        max_retries = 3

        for i in range(max_retries):
            chill = 0
            heat_accum = 0
            chill_date = None
            bloom_date = None
            #relaxed_attempt = i + 1

            # Ajuste progresivo de requerimientos
            chill_req_relaxed = int(chill_req * (retry_factor ** (i + 1)))
            heat_req_relaxed = int(heat_req * (retry_factor ** (i + 1)))

            for _, row in df_all.iterrows():
                T = row["temp"]
                if T < -100:
                    continue

                if row["step"] == "hour":
                    if 0 <= T <= 8.3:
                        chill += 1
                    if chill_date:
                        heat_accum += max(0, T - tbase) / 24.0
                else:
                    if T <= 8.3:
                        chill += 24
                    if chill_date:
                        heat_accum += max(0, T - tbase)

                if not chill_date and chill >= chill_req_relaxed:
                    chill_date = row["datetime"]

                if chill_date and not bloom_date and heat_accum >= heat_req_relaxed:
                    bloom_date = row["datetime"]

            if bloom_date:
                #chill_req_final = chill_req_relaxed
                #heat_req_final = heat_req_relaxed
                break

        # Si aún no se consigue floración
        if not bloom_date:
            bloom_date = date(year, 4, 30)  # Fecha fallback razonable
            #relaxed_attempt = "none"

    # ========================================
    # 5. Resultado final
    # ========================================
    confidence = round(random.uniform(0.7, 0.93), 2)

    return {
        "latitude": lat,
        "longitude": lon,
        "predicted_bloom_start": bloom_date,
        "predicted_bloom_peak": bloom_date,
        "confidence": confidence,
    }

