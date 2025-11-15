from pathlib import Path
import soccerdata as sd
import pandas as pd
import os
import numpy as np
from time import sleep
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Xác định BASE_DIR: ưu tiên environment variable, nếu không thì dùng relative path từ script
if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    # Lấy thư mục chứa script (scr/), rồi lên 1 level để có BASE_DIR
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

# Thư mục lưu raw data
DATA_RAW_DIR = os.path.join(BASE_DIR, "data_raw")

# Tạo thư mục nếu chưa tồn tại
os.makedirs(DATA_RAW_DIR, exist_ok=True)

def scrape_team_points():
    """Scrape team standings/points from flashscore.com"""
    print("Fetching team points/standings from flashscore.com...")
    
    # Cấu hình Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # chạy ẩn
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Tự động tải ChromeDriver phù hợp với bản Chrome hiện có
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        # Mở trang web
        url = "https://www.flashscore.com/football/england/premier-league/standings/#/OEEq9Yvp/standings/overall/"
        driver.get(url)
        sleep(random.randint(5, 10))
        
        # GET header
        headers = driver.find_elements(By.CSS_SELECTOR, "div.ui-table__headerCell")
        titles = [h.get_attribute("title").strip() for h in headers if h.get_attribute("title")]
        print(titles)
        
        # GET DATA
        data = {}
        
        # Chờ phần tử rank xuất hiện
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.table__cell--rank"))
        )
        
        # Lấy dữ liệu
        ranks = driver.find_elements(By.CSS_SELECTOR, "div.table__cell--rank")
        data['Rank'] = [rank.text.strip() for rank in ranks if rank.text.strip()]
        print(data['Rank'])
        
        teams = driver.find_elements(By.CSS_SELECTOR, "a.tableCellParticipant__name")
        data['Team'] = [t.text.strip() for t in teams if t.text.strip()]
        print(data["Team"])
        
        # Matches Played (MP)
        data['MP'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--value:nth-of-type(1)")]
        
        # Wins
        data['WINS'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--value:nth-of-type(2)")]
        
        # Draws
        data['DRAWS'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--value:nth-of-type(3)")]
        
        # Losses
        data['LOSSES'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--value:nth-of-type(4)")]
        
        # Goals For : Goals Against (VD: "86:41")
        data['GOALS'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--score")]
        
        # Goal Difference
        data['GD'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--goalsForAgainstDiff")]
        
        # Points
        data['POINTS'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--points")]
        
        data['Mùa giải'] = driver.find_element(By.CSS_SELECTOR, "div.heading__info").text.strip()
        
        # Lấy tất cả thẻ <a> chứa link standings
        categories = driver.find_elements(By.CSS_SELECTOR, 'a[href*="standings/"]')
        # Trích xuất phần cuối của href (overall, home, away)
        data['match_category'] = [cat.get_attribute('href').split('/')[-2] for cat in categories]
        print(data['match_category'])
        
        # Chuyển đổi sang DataFrame
        # Tìm độ dài tối đa để đảm bảo tất cả cột có cùng số hàng
        max_len = max(len(v) if isinstance(v, list) else 1 for v in data.values())
        
        # Chuẩn hóa dữ liệu: mở rộng giá trị scalar thành list
        for key in data:
            if not isinstance(data[key], list):
                data[key] = [data[key]] * max_len
        
        team_points_df = pd.DataFrame(data)
        
        # Lưu vào CSV
        out_path = Path(DATA_RAW_DIR) / "team_point.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        team_points_df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"Saved: {out_path}")
        
    except Exception as e:
        print(f"Error scraping team points: {e}")
        raise
    finally:
        driver.quit()

def main():

    fbref = sd.FBref(
        leagues="ENG-Premier League",
        seasons=['2021', '2122', '2223', '2324', '2425']
    )
    
    player_season = fbref.read_player_season_stats()

    player_season_df = pd.DataFrame(player_season)
    player_season_df = player_season_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_player_season_stats.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    player_season_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    

    print("Fetching player match statistics...")
    player_match = fbref.read_player_match_stats()
    player_match_df = pd.DataFrame(player_match)

    player_match_df = player_match_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_player_match_stats.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    player_match_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")

    print("Fetching team match statistics...")
    team_match = fbref.read_team_match_stats()
    team_match_df = pd.DataFrame(team_match)
    team_match_df = team_match_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_team_match.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    team_match_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    

    print("Fetching team season statistics...")
    team_season = fbref.read_team_season_stats()
    team_season_df = pd.DataFrame(team_season)
    team_season_df = team_season_df.reset_index()
    
    out_path = Path(DATA_RAW_DIR) / "fbref_fact_team_season.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    team_season_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    

    print("Fetching Understat shot events...")
    understat = sd.Understat(
        leagues="ENG-Premier League",
        seasons=['2021','2122','2223','2324','2425']
    )
    shot = understat.read_shot_events()
    shot_df = pd.DataFrame(shot)
    
    out_path = Path(DATA_RAW_DIR) / "understat_fact_shot.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shot_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    
    scrape_team_points()
    
    print("successfully")


if __name__ == "__main__":
    main()
