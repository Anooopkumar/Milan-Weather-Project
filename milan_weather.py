import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def init_db():
    conn = sqlite3.connect('milan_env.db')
    cursor = conn.cursor()
    # Integrated table as per methodology [cite: 19, 21]
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
    conn.commit()
    return conn

def sync_data():
    conn = init_db()
    cursor = conn.cursor()

    # Data Sources integration [cite: 11, 12]
    w_url = "https://api.open-meteo.com/v1/forecast?latitude=45.4643&longitude=9.1895&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&forecast_days=1"
    aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=45.4643&longitude=9.1895&hourly=pm10,pm2_5,nitrogen_dioxide,ozone&forecast_days=1"
    
    try:
        w_res = requests.get(w_url).json()
        aq_res = requests.get(aq_url).json()
        
        w_data = w_res['hourly']
        aq_data = aq_res['hourly']

        # Web Scraping [cite: 15, 18]
        scrape_res = requests.get("https://www.timeanddate.com/weather/italy/milan", headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(scrape_res.text, "html.parser")
        qlook = soup.find("div", id="qlook")
        s_temp_raw = qlook.find("div", class_="h2").text.strip()
        s_temp = float(''.join(filter(lambda x: x.isdigit() or x=='.', s_temp_raw.split()[0])))
        desc = qlook.find("p").text.strip()

        # ZIP use karne se IndexError khatam ho jayega
        combined_data = zip(
            w_data['time'], w_data['temperature_2m'], w_data['relative_humidity_2m'], w_data['wind_speed_10m'],
            aq_data['pm2_5'], aq_data['pm10'], aq_data['nitrogen_dioxide'], aq_data['ozone']
        )

        current_hour = datetime.now().strftime('%Y-%m-%dT%H:00')

        for ts_raw, temp, hum, wind, pm25, pm10, no2, o3 in combined_data:
            ts = ts_raw.replace("T", " ") # Timestamp format alignment [cite: 21, 35]
            
            # Validation logic [cite: 20, 33]
            val_temp = s_temp if ts_raw == current_hour else None
            val_desc = desc if ts_raw == current_hour else None

            cursor.execute('''
                INSERT OR IGNORE INTO environmental_data 
                (timestamp, temp, humidity, wind_speed, pm25, pm10, no2, ozone, scraped_temp, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (ts, temp, hum, wind, pm25, pm10, no2, o3, val_temp, val_desc))
        
        conn.commit()
        print("Success: Database updated and gaps filled! [cite: 45]")
    
    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        conn.close()

sync_data()
