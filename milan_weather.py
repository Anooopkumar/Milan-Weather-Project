import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def sync_data():
    conn = sqlite3.connect('milan_env.db')
    cursor = conn.cursor()
    
    # 1. API se forecast data lena (Ismein NULL nahi hote)
    w_url = "https://api.open-meteo.com/v1/forecast?latitude=45.4643&longitude=9.1895&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&forecast_days=1"
    aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=45.4643&longitude=9.1895&hourly=pm10,pm2_5,nitrogen_dioxide,ozone&forecast_days=1"
    
    w_data = requests.get(w_url).json()['hourly']
    aq_data = requests.get(aq_url).json()['hourly']
    
    # 2. Live Scraping (Sirf current hour ke liye)
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get("https://www.timeanddate.com/weather/italy/milan", headers=headers)
        soup = BeautifulSoup(res.text, "html.parser")
        # Behtar selector taake 45 jaisi galti na ho
        temp_text = soup.find("div", {"id": "qlook"}).find("div", class_="h2").text
        # Sirf numbers aur decimal rakhna
        current_temp = float(''.join(c for c in temp_text if c.isdigit() or c == '.' or c == '-'))
        current_desc = soup.find("div", {"id": "qlook"}).find("p").text
    except:
        current_temp, current_desc = None, None

    current_hour_str = datetime.now().strftime("%Y-%m-%d %H:00")

    for i in range(len(w_data['time'])):
        ts = w_data['time'][i].replace("T", " ")
        
        # Agar ye current hour hai, toh scraped data daalo, warna purana rehne do
        if ts == current_hour_str:
            s_temp, s_desc = current_temp, current_desc
        else:
            s_temp, s_desc = None, None

        # INSERT OR REPLACE use karein taake data update ho jaye
        cursor.execute('''
            INSERT INTO environmental_data (timestamp, temp, humidity, wind_speed, pm25, pm10, no2, ozone, scraped_temp, description)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(timestamp) DO UPDATE SET 
            scraped_temp = COALESCE(excluded.scraped_temp, scraped_temp),
            description = COALESCE(excluded.description, description)
        ''', (ts, w_data['temperature_2m'][i], w_data['relative_humidity_2m'][i], 
              w_data['wind_speed_10m'][i], aq_data['pm2_5'][i], aq_data['pm10'][i], 
              aq_data['nitrogen_dioxide'][i], aq_data['ozone'][i], s_temp, s_desc))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    sync_data()
