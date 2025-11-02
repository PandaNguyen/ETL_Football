from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.common.exceptions import TimeoutException
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)
import pandas as pd
import csv
import random
from selenium.common.exceptions import WebDriverException, NoSuchElementException, StaleElementReferenceException

url = "https://www.flashfootball.com/england/premier-league-2024-2025/results/"
driver.get(url)

# Click full show more để lấy được toàn bộ trận đấu
while True:
    try:
        wait = WebDriverWait(driver, 5)
        show_more_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[data-testid="wcl-buttonLink"]'))
        )
        if show_more_button.is_displayed() and show_more_button.is_enabled():
            driver.execute_script("arguments[0].click();", show_more_button)
            time.sleep(2)  
        else:
            break

    except TimeoutException:
        break
    except Exception as e:
        time.sleep(1)
        continue

print("Hiển thị tất cả 38 vòng đấu")


# Lấy ra tất cả các link trận đấu trong 1 mùa: 1 mùa 380 trận

wait = WebDriverWait(driver, 10)
all_links = []
try:
    all_link_elements = wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "div.event__match a[href]")
        )
    )
    for link_element in all_link_elements:
        href = link_element.get_attribute("href")
        all_links.append(href)

    # print(all_links)
except Exception as e:
    print("0")
# print(all_links[0])
# test 
print(len(all_links))


all_data = []
for link in all_links:
    url_link = link
    driver.get(url_link)
    time.sleep(2)
    data = {}
    matches = driver.find_elements(By.CSS_SELECTOR, "div.duelParticipant__container")
    for match in matches:
        # lay time
        time_match = match.find_element(By.CSS_SELECTOR,"div.duelParticipant__startTime")
        match_time = time_match.text.strip().split()
        ngay, gio = match_time 
        data['ngay_thi_dau'] = ngay
        data['gio_bat_dau'] = gio


        # HOME
        home_team = match.find_element(By.CSS_SELECTOR,"div.duelParticipant__home a.participant__participantName").text.strip()
        # Away
        data['home_team'] = home_team
        
        away_team = match.find_element(By.CSS_SELECTOR,"div.duelParticipant__away a.participant__participantName").text.strip()
        data['away_team'] = away_team
        # ti so

        score = match.find_elements(By.CSS_SELECTOR,"div.duelParticipant__score span")
    
        home_score = score[0].text.strip()
        away_score = score[2].text.strip()
        data['home_score'] = home_score
        data['away_score'] = away_score


    # trong tai
    values = driver.find_elements(By.CSS_SELECTOR,'div.wcl-summaryWidgetContainer_uz3SO strong[data-testid = "wcl-scores-simple-text-01"]')
    referee = values[0].text.strip()
    data['referee'] = referee
    
    
    venue = values[1].text.strip()
    data['venue'] = venue
    # mùa giải:
    rouds = driver.find_elements(By.CSS_SELECTOR,"div.detail__breadcrumbs span")
    roud = rouds[1].text.strip()
    data['roud'] = roud
    all_data.append(data)
    
fieldnames = sorted({k for d in all_data for k in d.keys()})
with open('../thongtinchung.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader(); w.writerows(all_data)
print("da luu")



ORDER = [
    "ngay_thi_dau",
    "gio_bat_dau",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "referee",
    "venue",
    "roud",
    "source_url",
]
with open("../thongtinchung.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=ORDER, extrasaction="ignore")
    w.writeheader()
    for row in all_data:
        w.writerow(row)


try:
    driver.set_page_load_timeout(25)   
    driver.set_script_timeout(20)      
except Exception:
    pass

def _stop_loading(drv):              
    try:
        drv.execute_script("return window.stop ? window.stop() : document.execCommand('Stop');")
    except Exception:
        pass

def _get_with_retry(drv, url, tries=2):  
    last = None
    for _ in range(tries):
        try:
            drv.get(url)
            return True
        except TimeoutException as e:
            last = e
            _stop_loading(drv)           
            return True
        except WebDriverException as e:
            last = e
            try: drv.get("about:blank")
            except Exception: pass
            time.sleep(random.uniform(0.5, 1.2))
    print(f"GET fail: {url} -> {last}")
    return False

wait = WebDriverWait(driver, 20)
data1 = []
for l in all_links:
   
    if not _get_with_retry(driver, l):     
        continue                           

    driver.get(l)                        

    try:                                   
        href_el = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'div.loadable.complete.section a[href]')))
    except TimeoutException:              
        print("không thấy link trong section"); continue
    tmp = href_el.get_attribute("href")

    if not _get_with_retry(driver, tmp):  
        continue                           

    driver.get(tmp)                        

    try:                               
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'div[data-testid="wcl-statistics"]')))
    except TimeoutException:              
        print("không thấy wcl-statistics"); continue

    data = {}
    stats = driver.find_elements(By.CSS_SELECTOR, 'div[data-testid="wcl-statistics"]')
    for stat in stats:
        try:   
            home = stat.find_element(By.CSS_SELECTOR,
                '[class*="wcl-homeValue_"] strong[data-testid="wcl-scores-simple-text-01"]').text.strip()
        except (NoSuchElementException, StaleElementReferenceException):
            home = ""
        try:
            away = stat.find_element(By.CSS_SELECTOR,
                '[class*="wcl-awayValue_"] strong[data-testid="wcl-scores-simple-text-01"]').text.strip()
        except (NoSuchElementException, StaleElementReferenceException):
            away = ""
        try:
            tutorial = stat.find_element(By.CSS_SELECTOR,
                'div[data-testid="wcl-statistics-category"] strong[data-testid="wcl-scores-simple-text-01"]')
            key = tutorial.text.strip().rstrip(":")
        except (NoSuchElementException, StaleElementReferenceException):
            key = ""

        if key:                          
            data[f"{key}_home"] = home
            data[f"{key}_away"]  = away

    data1.append(data)

    time.sleep(random.uniform(0.4, 1.0))  

df = pd.DataFrame(data1)
df.to_csv('../cuthe.csv', index=False, encoding='utf-8-sig')


df1 = pd.read_csv('../thongtinchung.csv')
df2 = pd.read_csv('../cuthe.csv')

merged = pd.concat([df1, df2], axis=1)
merged.to_csv('../merged.csv', index=False)