from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from tqdm import tqdm
import pandas as pd
import time
import os
from pathlib import Path

# --- 1. Cấu hình Chrome headless ---
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=options)

# --- 2. Danh sách 5 mùa gần nhất ---
seasons = ["2024-2025", "2023-2024", "2022-2023", "2021-2022", "2020-2021"]

# --- 2.1. Thiết lập thư mục lưu dữ liệu giống Extract.py ---
if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

DATA_RAW_DIR = os.path.join(BASE_DIR, "data_raw")
os.makedirs(DATA_RAW_DIR, exist_ok=True)

# --- 3. Tạo list để lưu kết quả ---
all_data = []

# --- 4. Duyệt qua từng mùa ---
for season in tqdm(seasons, desc="Crawling seasons", unit="season"):
    base_url = f"https://www.flashfootball.com/england/premier-league-{season}/#/lAkHuyP3/standings/"

    # --- 5. Duyệt qua 3 loại bảng ---
    categories = ["overall", "home", "away"]
    for cat in tqdm(categories, desc=f"{season}", leave=False, unit="table"):
        url = f"{base_url}{cat}/"
        driver.get(url)
        time.sleep(2.5)

        # --- Lấy mùa giải hiển thị trên trang ---
        try:
            season_label = driver.find_element(By.CSS_SELECTOR, "div.heading__info").text.strip()
        except:
            season_label = season

        # --- Lấy dữ liệu ---
        ranks = [r.text.strip() for r in driver.find_elements(By.CSS_SELECTOR, "div.tableCellRank")]
        teams = [t.text.strip() for t in driver.find_elements(By.CSS_SELECTOR, "a.tableCellParticipant__name")]
        values = [v.text.strip() for v in driver.find_elements(By.CSS_SELECTOR, "span.table__cell.table__cell--value")]

        # --- Lấy form gần đây ---
        forms_all = driver.find_elements(By.CSS_SELECTOR, "div.table__cell.table__cell--form")
        recent_forms = []
        for form_cell in forms_all:
            results = [s.text.strip() for s in form_cell.find_elements(By.CSS_SELECTOR, "span.wcl-scores-simple-text-01_8lVyp")]
            recent_forms.append("".join(results))

        # --- Chia giá trị theo hàng ---
        rows = [values[i:i + 7] for i in range(0, len(values), 7)]

        # --- Gộp dữ liệu ---
        for i, row in enumerate(rows):
            all_data.append({
                "Mùa giải": season_label,
                "Match_Category": cat,
                "Rank": ranks[i] if i < len(ranks) else "",
                "Team": teams[i] if i < len(teams) else "",
                "MP": row[0] if len(row) > 0 else "",
                "W": row[1] if len(row) > 1 else "",
                "D": row[2] if len(row) > 2 else "",
                "L": row[3] if len(row) > 3 else "",
                "GF:GA": row[4] if len(row) > 4 else "",
                "GD": row[5] if len(row) > 5 else "",
                "Pts": row[6] if len(row) > 6 else "",
                "Recent_Form": recent_forms[i] if i < len(recent_forms) else ""
            })

        # Việc tạo DataFrame và lưu file sẽ thực hiện sau khi crawl xong toàn bộ

# --- 6. Tạo DataFrame và lưu ra file CSV như Extract.py ---
columns = ["Mùa giải", "Match_Category", "Rank", "Team", "MP", "W", "D", "L", "GF:GA", "GD", "Pts", "Recent_Form"]
team_points_df = pd.DataFrame(all_data, columns=columns)

out_path = Path(DATA_RAW_DIR) / "team_point.csv"
out_path.parent.mkdir(parents=True, exist_ok=True)
team_points_df.to_csv(out_path, index=False, encoding="utf-8-sig")
print(f"Saved: {out_path}")
print(f"Total records: {len(team_points_df)}")

# --- 7. Đóng trình duyệt ---
driver.quit()