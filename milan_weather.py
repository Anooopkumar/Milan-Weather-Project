
import sqlite3
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Retry Logic Function ---
def get_retry_session():
    session = requests.Session()
    retry = Retry(
        total=5, # 5 baar koshish karega
        backoff_factor=2, # Har try ke beech gap badhayega (2s, 4s, 8s...)
        status_forcelist=[429, 500, 502, 503, 504], # Rate limit aur server error handle karega
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def sync_data():
    conn = sqlite3.connect('milan_env.db')
    cursor = conn.cursor()
    
    # Session banayein jo requests handle karega
    session = get_retry_session()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS environmental_data (
            timestamp TEXT PRIMARY KEY, 
            temp REAL, 
            humidity REAL, 
            wind_speed REAL, 
            pm25 REAL, 
            pm10 REAL, 
            no2 REAL, 
            ozone REAL, 
            scraped_temp REAL, 
            description TEXT
        )
    ''')

    # 2. API se data lena (Session use karte hue)
    w_url = "https://api.open-meteo.com/v1/forecast?latitude=45.4643&longitude=9.1895&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&forecast_days=1"
    aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=45.4643&longitude=9.1895&hourly=pm10,pm2_5,nitrogen_dioxide,ozone&forecast_days=1"
    
    try:
        w_data = session.get(w_url, timeout=10).json()['hourly']
        time.sleep(1) # Chhota sa gap server ko relax dene ke liye
        aq_data = session.get(aq_url, timeout=10).json()['hourly']
    except Exception as e:
        print(f"API Error: {e}")
        return # Agar API nahi chali toh aage nahi badhega

    # 3. Live Scraping logic
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        res = session.get("https://www.timeanddate.com/weather/italy/milan", headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        temp_text = soup.find("div", {"id": "qlook"}).find("div", class_="h2").text
        
        current_temp = float(''.join(c for c in temp_text if c.isdigit() or c == '.' or c == '-'))
        
        # Temp Conversion Logic
        if current_temp > 35: # Milan mein 35C se upar aksar F hota hai agar scraping galat ho
            current_temp = round((current_temp - 32) * 5/9, 1)
            
        current_desc = soup.find("div", {"id": "qlook"}).find("p").text
    except Exception as e:
        print(f"Scraping failed: {e}")
        current_temp, current_desc = None, None

    current_hour_str = datetime.now().strftime("%Y-%m-%d %H:00")

    # 4. Data Save/Update karna
    for i in range(len(w_data['time'])):
        ts = w_data['time'][i].replace("T", " ")
        s_temp = current_temp if ts == current_hour_str else None
        s_desc = current_desc if ts == current_hour_str else None

        cursor.execute('''
            INSERT INTO environmental_data (timestamp, temp, humidity, wind_speed, pm25, pm10, no2, ozone, scraped_temp, description)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(timestamp) DO UPDATE SET 
            scraped_temp = COALESCE(excluded.scraped_temp, environmental_data.scraped_temp),
            description = COALESCE(excluded.description, environmental_data.description)
        ''', (ts, w_data['temperature_2m'][i], w_data['relative_humidity_2m'][i], 
              w_data['wind_speed_10m'][i], aq_data['pm2_5'][i], aq_data['pm10'][i], 
              aq_data['nitrogen_dioxide'][i], aq_data['ozone'][i], s_temp, s_desc))
    
    conn.commit()
    conn.close()
    print("Database updated successfully!")

if __name__ == "__main__":
    sync_data()
