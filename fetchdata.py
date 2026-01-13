import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import time
import urllib3
import pandas as pd
import os
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_market_12_with_fallback():
    # 強制切換到檔案所在目錄 (本地測試用)
    try:
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
    except:
        pass
    
    # --- 設定區域 ---
    # 優先讀取環境變數，若無則使用你提供的預設 ID
    SPREADSHEET_ID = os.getenv('SHEET_ID')
    SHEET_NAME = 'MarketData'
    HEADERS = ["日期", "商品", "開盤價", "最高價", "最低價", "收盤價", "備註"]
    
    # 12 檔清單
    TW_STOCKS = {"2330": "2330", "2317": "2317", "2308": "2308", "2454": "2454", "2881": "2881"}
    US_INDICES = {"^SOX": "^SOX", "^IXIC": "^IXIC", "^GSPC": "^S&P 500", "^DJI": "^DJI"}

    # --- A. Google Sheets 連線邏輯 (支援環境變數) ---
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 讀取 GitHub Actions 設定的 Secret
        creds_json = os.getenv('GOOGLE_CREDS_JSON')
        
        if creds_json:
            # 如果在 GitHub 環境：將字串轉回字典並連線
            creds_dict = json.loads(creds_json)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            # 如果在本地環境：讀取實體檔案
            creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
            
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        
        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            # 若工作表不存在則建立
            ws = sh.add_worksheet(title=SHEET_NAME, rows="5000", cols=str(len(HEADERS)))
            ws.append_row(HEADERS)
            
    except Exception as e:
        print(f"❌ Google Sheets 連接失敗: {e}")
        return

    # --- B. 抓取邏輯 ---
    now = datetime.datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    exec_time_str = now.strftime('%H:%M')
    collected_rows = []

    # 1. Yahoo 財經 (台股 + 美股)
    headers = {'User-Agent': 'Mozilla/5.0'}
    for sym, name in {**TW_STOCKS, **US_INDICES}.items():
        url = f"https://finance.yahoo.com/quote/{sym}"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 取得收盤價 (大字體)
            price_tag = soup.find('fin-streamer', {'data-field': 'regularMarketPrice'})
            if not price_tag: continue
            close_p = float(price_tag.get_text().replace(',', ''))
            
            # 取得開高低 (從表格抓)
            open_p, high_p, low_p = close_p, close_p, close_p
            rows = soup.find_all('tr')
            for r in rows:
                txt = r.get_text()
                if 'Open' in txt: open_p = float(r.find_all('td')[1].get_text().replace(',', ''))
                if 'Day\'s Range' in txt:
                    rng = r.find_all('td')[1].get_text().replace(',', '').split(' - ')
                    low_p, high_p = float(rng[0]), float(rng[1])
            
            collected_rows.append([f"{today_str}_{exec_time_str}", name, open_p, high_p, low_p, close_p, "YahooFinance"])
            print(f"✅ 抓取成功: {name}")
        except Exception as e:
            print(f"⚠️ {name} 抓取失敗: {e}")

    # 2. 台指期 (期交所備援)
    prod_name = "台指期"
    success = False
    for d_offset in [0, 1]:
        d_str = (now - datetime.timedelta(days=d_offset)).strftime('%Y/%m/%d')
        contract_month = now.strftime('%Y%m')
        taifex_url = f"https://www.taifex.com.tw/cht/3/futDailyMarketReport?queryDate={d_str}&MarketCode=0&commodity_id=TX"
        try:
            r = requests.get(taifex_url, verify=False, timeout=10)
            s = BeautifulSoup(r.text, 'html.parser')
            table = s.find('table', {'class': 'table_f'})
            if table:
                trs = table.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    if len(tds) > 5 and tds[0].get_text(strip=True) == 'TX' and tds[1].get_text(strip=True) == contract_month:
                        open_p = tds[2].get_text(strip=True).replace(',', '')
                        if open_p not in ['-', '', '0']:
                            note = "期交所官方" if d_str == today_str else f"補件({d_str})"
                            collected_rows.append([
                                f"{today_str}_{exec_time_str}", prod_name,
                                float(open_p), float(tds[3].get_text(strip=True).replace(',', '')),
                                float(tds[4].get_text(strip=True).replace(',', '')), float(tds[5].get_text(strip=True).replace(',', '')), note
                            ])
                            success = True; break
                if success: break
        except: continue
    if not success: print(f"⚠️ {prod_name} 嘗試兩日均無資料")

    # --- C. 數據寫回 Sheets (去重並保持最新在上) ---
    if collected_rows:
        try:
            df_new = pd.DataFrame(collected_rows, columns=HEADERS)
            all_vals = ws.get_all_values()
            
            if len(all_vals) > 1:
                df_old = pd.DataFrame(all_vals[1:], columns=HEADERS)
                # 合併後去重，以「日期」和「商品」為基準，保留最新抓到的
                df_final = pd.concat([df_new, df_old]).drop_duplicates(subset=['日期', '商品'], keep='first')
            else:
                df_final = df_new
            
            # 排序：日期越新越上面
            df_final = df_final.sort_values(by='日期', ascending=False)
            
            # 更新回試算表
            ws.update([df_final.columns.values.tolist()] + df_final.values.tolist())
            print(f"📊 數據同步完成，共 {len(df_final)} 筆紀錄。")
        except Exception as e:
            print(f"❌ 寫入 Sheets 失敗: {e}")

if __name__ == "__main__":
    fetch_market_12_with_fallback()