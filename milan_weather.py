import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def sync_data():
    conn = sqlite3.connect('milan_env.db')
    cursor = conn.cursor()
    
    # 1. Table banayein agar nahi bana hua (Is se error khatam ho jayega)
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

    # 2. API se forecast data lena
    w_url = "https://api.open-meteo.com/v1/forecast?latitude=45.4643&longitude=9.1895&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&forecast_days=1"
    aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=45.4643&longitude=9.1895&hourly=pm10,pm2_5,nitrogen_dioxide,ozone&forecast_days=1"
    
    w_data = requests.get(w_url).json()['hourly']
    aq_data = requests.get(aq_url).json()['hourly']
    
    # 3. Live Scraping logic
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get("https://www.timeanddate.com/weather/italy/milan", headers=headers)
        soup = BeautifulSoup(res.text, "html.parser")
        temp_text = soup.find("div", {"id": "qlook"}).find("div", class_="h2").text
        
        # Numbers extract karna
        current_temp = float(''.join(c for c in temp_text if c.isdigit() or c == '.' or c == '-'))
        
        # Fahrenheit ko Celsius mein badalna agar temp 35 se upar hai
        if current_temp > 10:
            current_temp = round((current_temp - 32) * 5/9, 1)
            
        current_desc = soup.find("div", {"id": "qlook"}).find("p").text
    except:
        current_temp, current_desc = None, None

    # Current hour string (e.g., "2025-12-31 15:00")
    current_hour_str = datetime.now().strftime("%Y-%m-%d %H:00")

    # 4. Data Save/Update karna
    for i in range(len(w_data['time'])):
        ts = w_data['time'][i].replace("T", " ")
        
        # Sirf current hour ke liye scraped data use karein
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
