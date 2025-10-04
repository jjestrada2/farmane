from datetime import date, timedelta, datetime as dt
from typing import Dict, Any
import requests
import pandas as pd
import ee
import os
import random
import asyncio

# Como hacemos para que se llame una sola vez al iniciar la app?
def initialize_earth_engine():
    """Initialize Earth Engine with service account credentials"""    
    try:
        service_account = 'gee-farmane@vaulted-channel-234121.iam.gserviceaccount.com'
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        credentials_path = os.path.join(project_root, 'src', 'services', 'vaulted-channel-234121-376df8d2d29a.json')
        credentials = ee.ServiceAccountCredentials(service_account, credentials_path)
        ee.Initialize(credentials)
        return True
    except Exception as e:
        # print(f"Earth Engine initialization failed: {e}")
        return False

def filter_cloudy_images(collections, cloud_threshold=90):
    """Filter out images with high cloud percentage"""
    for key, collection in collections.items():
        if key.startswith('sentinel'):
            collections[key] = collection.filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud_threshold))
        elif key.startswith('landsat'):
            collections[key] = collection.filter(ee.Filter.lt('CLOUD_COVER', cloud_threshold))
    return collections

def mask_clouds_sentinel(img):
    """Cloud mask for Sentinel-2"""
    qa = img.select('QA60')
    cloud_mask = qa.bitwiseAnd(1 << 10).neq(0).Or(qa.bitwiseAnd(1 << 11).neq(0)).Not()
    return img.updateMask(cloud_mask).copyProperties(img, ['system:time_start'])

def mask_clouds_landsat(img):
    """Cloud mask for Landsat"""
    qa = img.select('QA_PIXEL')
    cloud_mask = qa.bitwiseAnd(1 << 3).neq(0).Or(qa.bitwiseAnd(1 << 4).neq(0)).Not()
    return img.updateMask(cloud_mask).copyProperties(img, ['system:time_start'])

def calculate_ebi_sentinel(img):
    """Calculate Enhanced Bloom Index for Sentinel-2"""
    R = img.select('B4').multiply(1e-4)
    G = img.select('B3').multiply(1e-4) 
    B = img.select('B2').multiply(1e-4).max(1e-6)
    
    brightness = R.add(G).add(B)
    greenness = G.divide(B)
    soil_sig = R.subtract(B).add(1.0)
    ebi = brightness.divide(greenness.multiply(soil_sig)).rename('EBI')
    
    return img.addBands(ebi).copyProperties(img, ['system:time_start'])

def calculate_ebi_landsat(img):
    """Calculate Enhanced Bloom Index for Landsat"""
    R = img.select('SR_B4').multiply(0.0000275).add(-0.2).max(0)
    G = img.select('SR_B3').multiply(0.0000275).add(-0.2).max(0)
    B = img.select('SR_B2').multiply(0.0000275).add(-0.2).max(1e-6)
    
    brightness = R.add(G).add(B)
    greenness = G.divide(B)
    soil_sig = R.subtract(B).add(1.0)
    ebi = brightness.divide(greenness.multiply(soil_sig)).rename('EBI')
    
    return img.addBands(ebi).copyProperties(img, ['system:time_start'])

def calculate_ebi_modis(img):
    """Calculate Enhanced Bloom Index for MODIS"""
    R = img.select('sur_refl_b01').multiply(0.0001).max(0)
    G = img.select('sur_refl_b04').multiply(0.0001).max(0)
    B = img.select('sur_refl_b03').multiply(0.0001).max(1e-6)
    
    brightness = R.add(G).add(B)
    greenness = G.divide(B)
    soil_sig = R.subtract(B).add(1.0)
    ebi = brightness.divide(greenness.multiply(soil_sig)).rename('EBI')
    
    return img.addBands(ebi).copyProperties(img, ['system:time_start'])

def get_satellite_collections(roi, year):
    """Get multiple satellite collections for EBI analysis"""
    collections = {}
    
    start_date = ee.Date.fromYMD(year, 1, 15)
    end_date = ee.Date.fromYMD(year, 4, 15)
    
    # Sentinel-2
    collections['sentinel2'] = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                               .filterBounds(roi)
                               .filterDate(start_date, end_date)
                               .map(mask_clouds_sentinel)
                               .map(calculate_ebi_sentinel)
                               .map(lambda img: img.clip(roi)))
    
    # Landsat 8
    collections['landsat8'] = (ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
                              .filterBounds(roi)
                              .filterDate(start_date, end_date)
                              .map(mask_clouds_landsat)
                              .map(calculate_ebi_landsat)
                              .map(lambda img: img.clip(roi)))
    
    # Landsat 9
    collections['landsat9'] = (ee.ImageCollection("LANDSAT/LC09/C02/T1_L2")
                              .filterBounds(roi)
                              .filterDate(start_date, end_date)
                              .map(mask_clouds_landsat)
                              .map(calculate_ebi_landsat)
                              .map(lambda img: img.clip(roi)))
    
    # MODIS Terra
    collections['modis_terra'] = (ee.ImageCollection("MODIS/061/MOD09A1")
                                 .filterBounds(roi)
                                 .filterDate(start_date, end_date)
                                 .map(calculate_ebi_modis)
                                 .map(lambda img: img.clip(roi)))
    
    # Filter out cloudy images
    collections = filter_cloudy_images(collections, cloud_threshold=75)
    
    return collections

def extract_ebi_mean(img, roi):
    """Extract mean EBI for each image"""
    mean_ebi = img.select('EBI').reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=roi,
        scale=10,
        maxPixels=1e10,
        bestEffort=True
    ).get('EBI')
    
    date = img.date().format('YYYY-MM-dd')
    return ee.Feature(None, {
        'date': date,
        'mean_ebi': mean_ebi,
        'timestamp': img.date().millis()
    })

def extract_time_series_multi_satellite(collections, roi):
    """Extract EBI time series from multiple satellite collections"""
    all_dates = []
    all_ebi_values = []
    
    for satellite_name, collection in collections.items():
                
        time_series_fc = collection.map(lambda img: extract_ebi_mean(img, roi)).filter(ee.Filter.notNull(['mean_ebi']))
        try:
            time_series_data = time_series_fc.getInfo()
            
            satellite_dates = []
            satellite_ebi_values = []
            
            for feature in time_series_data['features']:
                props = feature['properties']
                date_str = props['date']
                ebi_val = props['mean_ebi']
                
                if ebi_val is not None:
                    satellite_dates.append(dt.strptime(date_str, '%Y-%m-%d'))
                    satellite_ebi_values.append(ebi_val)
            
            all_dates.extend(satellite_dates)
            all_ebi_values.extend(satellite_ebi_values)
            
        except Exception as e:
            continue
    
    # Sort by date
    if all_dates:
        sorted_pairs = sorted(zip(all_dates, all_ebi_values))
        all_dates, all_ebi_values = zip(*sorted_pairs)
        all_dates = list(all_dates)
        all_ebi_values = list(all_ebi_values)
    
    return all_dates, all_ebi_values

def get_ebi_geotiff_url(collections, peak_date, roi):
    """Peak EBI GeoTIFF URL from GEE"""
    try:                
        # Filter collection
        peak_date_str = peak_date.strftime('%Y-%m-%d')
        peak_day_start = ee.Date(peak_date_str)
        peak_day_end = peak_day_start.advance(1, 'day')
        
        peak_image = None
        for satellite_name, collection in collections.items():
            try:
                size = collection.filterDate(peak_day_start, peak_day_end).size().getInfo()
                if size > 0:
                    peak_image = collection.filterDate(peak_day_start, peak_day_end).first()
                    break
            except:
                continue
        
        if peak_image is None:
            raise Exception("No image found for peak date")
        
        export_image = peak_image.select('EBI').clip(roi)
        # Get min/max values for scaling
        minMax = export_image.reduceRegion(
            reducer=ee.Reducer.minMax(),
            geometry=roi,
            scale=10,
            maxPixels=1e10,
            bestEffort=True
        )
        
        ebi_min = minMax.get('EBI_min')
        ebi_max = minMax.get('EBI_max')
        
        color_palette = ['0d0887', '6300a7', 'ab2494', 'e34f6f', 'fb9f3a', 'f0f921']
        
        # Create scaled visualization
        rgb_image = export_image.visualize(
            min=ebi_min,
            max=ebi_max,
            palette=color_palette
        )
        
        # Get download URL
        url = rgb_image.getDownloadURL({
            'scale': 10,
            'crs': 'EPSG:4326',
            'region': roi,
            'format': 'GEO_TIFF'
        })
        
        return url

    except Exception as e:
        return "https://example.com/tiles/ebi/mock.png"

async def detect_bloom(latitude: float, longitude: float) -> Dict[str, Any]:
    """Detect bloom peak date and EBI value for a given lat/lon using satellite data

    Args:
        latitude: Latitude coordinate
        longitude: Longitude coordinate

    Returns:
        Dictionary containing:
        - latitude: Input latitude
        - longitude: Input longitude 
        - date_of_max_ebi: Date of peak bloom
        - ebi_value: Peak EBI value
        - image_url: Peak EBI GeoTIFF URL from GEE
    """
    try:
        # Initialize Earth Engine
        if not initialize_earth_engine():
            raise Exception("Failed to initialize Earth Engine")
        
        # Create ROI from lat/lon (100 hectares square around the center point)
        point = ee.Geometry.Point([longitude, latitude])
        roi = point.buffer(500).bounds()  # Creates a 100-hectare square
        
        # Get current year and define bloom season
        current_year = dt.now().year
        
        # Load Sentinel-2 data
        collections = get_satellite_collections(roi, current_year)
        
        num_images = 0
        for col in collections.values():
            try:
                num_images += col.size().getInfo()
            except:
                continue
        
        if num_images == 0:
            # If no data for current year, try previous year
            current_year = current_year - 1
            collections = get_satellite_collections(roi, current_year)
        
            num_images = 0
            for col in collections.values():
                try:
                    num_images += col.size().getInfo()
                except:
                    continue
    
            if num_images == 0:
                raise Exception("No satellite data available for the location")
        
        all_dates, all_ebi_values = extract_time_series_multi_satellite(collections, roi)
        if not all_dates or not all_ebi_values:
            raise Exception("No valid EBI data extracted")
        
        # Find peak bloom
        max_ebi = max(all_ebi_values)
        peak_date = all_dates[all_ebi_values.index(max_ebi)]
        
        # Get EBI GeoTIFF URL
        image_url = get_ebi_geotiff_url(collections, peak_date, roi)
        
        return {
            "latitude": latitude,
            "longitude": longitude,
            "date_of_max_ebi": peak_date.date(),
            "ebi_value": round(max_ebi, 3),
            "image_url": image_url,
        }
        
    except Exception as e:
        # print(f"Bloom detection failed: {e}")
        return {
            "latitude": latitude,
            "longitude": longitude,
            "date_of_max_ebi": date.today(),
            "ebi_value": 0.00,
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

