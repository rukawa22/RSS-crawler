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
    # --- 1. 環境變數與路徑設定 (不動) ---
    try:
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
    except:
        pass
    
    SPREADSHEET_ID = os.getenv('SHEET_ID')
    SHEET_NAME = 'MarketData'
    HEADERS = ["日期", "商品", "開盤價", "最高價", "最低價", "收盤價", "備註"]
    
    TW_STOCKS = {"2330": "2330", "2317": "2317", "2308": "2308", "2454": "2454", "2881": "2881"}
    US_INDICES = {"^SOX": "^SOX", "^IXIC": "^IXIC", "^GSPC": "^S&P 500", "^DJI": "^DJI"}

    # --- 2. Google Sheets 連線 (不動) ---
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_json = os.getenv('GOOGLE_CREDS_JSON')
        if creds_json:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet(SHEET_NAME)
    except Exception as e:
        print(f"❌ Sheets 連接失敗: {e}"); return

    # --- 3. 時間邏輯修正 (僅改這部分) ---
    # 強制獲取 UTC 時間並轉換為台灣時間 (UTC+8)
    utc_now = datetime.datetime.utcnow()
    tw_now = utc_now + datetime.timedelta(hours=8)
    
    exec_time_str = tw_now.strftime('%H:%M')
    today_str = tw_now.strftime('%Y/%m/%d')
    # 這裡的日期用於抓取 API 資料的參數 (依舊嘗試今天與昨天)
    yesterday_str = (tw_now - datetime.timedelta(days=1)).strftime('%Y/%m/%d')
    dates_to_try = [today_str, yesterday_str]

    # --- 4. 原始抓取邏輯 ---
    collected_rows = []

    # A. 台股個股 + 加權指數 (完全不動)
    for stock_no, name in {**TW_STOCKS, "IX0001": "加權TPE: IX0001"}.items():
        success = False
        for d_str in dates_to_try:
            try:
                tw_date = d_str.replace('/', '')
                if stock_no == "IX0001":
                    url = f"https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date={tw_date}"
                else:
                    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={tw_date}&stockNo={stock_no}"
                
                resp = requests.get(url, verify=False, timeout=10)
                data = resp.json()
                if data.get('stat') == 'OK' and 'data' in data:
                    row = data['data'][-1]
                    idx = [1,2,3,4] if stock_no == "IX0001" else [3,4,5,6]
                    note = "證交所官方" if d_str == today_str else f"補件({d_str})"
                    collected_rows.append([
                        f"{today_str}_{exec_time_str}", name if stock_no == "IX0001" else stock_no,
                        float(row[idx[0]].replace(',', '')), float(row[idx[1]].replace(',', '')),
                        float(row[idx[2]].replace(',', '')), float(row[idx[3]].replace(',', '')), note
                    ])
                    success = True; break
            except: continue

    # B. 美股指數 (完全不動)
    ua = {'User-Agent': 'Mozilla/5.0'}
    for yf_code, display_name in US_INDICES.items():
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_code}?interval=1d&range=3d"
            resp = requests.get(api_url, headers=ua, timeout=10).json()
            res = resp['chart']['result'][0]
            quote = res['indicators']['quote'][0]
            i = -1
            while quote['open'][i] is None: i -= 1
            actual_dt = datetime.datetime.fromtimestamp(res['timestamp'][i]).strftime('%Y/%m/%d')
            collected_rows.append([
                f"{today_str}_{exec_time_str}", display_name,
                round(quote['open'][i], 2), round(quote['high'][i], 2),
                round(quote['low'][i], 2), round(quote['close'][i], 2), f"Yahoo({actual_dt})"
            ])
        except: pass

    # C. 台指期 (只改這裡：分別處理一般與盤後，並確保回溯至上一個交易日)
    contract_month = tw_now.strftime('%Y%m')
    
    # 建立一個針對期貨的日期清單，回溯 5 天以確保涵蓋週末與連假 (滿足「上一次有交易日」的需求)
    # 這樣在週一早上跑的時候，能抓到上週五的夜盤
    taifex_dates = []
    for i in range(15):
        d_check = (tw_now - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
        taifex_dates.append(d_check)

    # 0 = 一般交易時段 (Regular), 1 = 盤後交易時段 (After-hours)
    # 依序檢查並確保兩者都能抓到數據
    for sess_code, sess_label in [("0", ""), ("1", "-夜")]:
        success = False
        prod_name = f"TX{contract_month}{sess_label}"
        
        for d_str in taifex_dates:
            try:
                url = 'https://www.taifex.com.tw/cht/3/futDailyMarketReport'
                payload = {'queryType': '2', 'marketCode': sess_code, 'commodity_id': 'TX', 'queryDate': d_str}
                resp = requests.post(url, data=payload, verify=False, timeout=10)
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                found_in_day = False
                for tr in soup.find_all('tr'):
                    tds = tr.find_all('td')
                    if len(tds) >= 6 and tds[0].get_text(strip=True) == 'TX' and tds[1].get_text(strip=True) == contract_month:
                        open_p = tds[2].get_text(strip=True).replace(',', '')
                        if open_p not in ['-', '', '0']:
                            note = "期交所官方" if d_str == today_str else f"補件({d_str})"
                            collected_rows.append([
                                f"{today_str}_{exec_time_str}", # 記錄時間
                                prod_name,
                                float(open_p), 
                                float(tds[3].get_text(strip=True).replace(',', '')),
                                float(tds[4].get_text(strip=True).replace(',', '')), 
                                float(tds[5].get_text(strip=True).replace(',', '')), 
                                note
                            ])
                            found_in_day = True
                            break # 找到該日資料，跳出 row loop
                
                if found_in_day:
                    success = True
                    break # 找到最新一筆有效資料，跳出 date loop，處理下一個 session
            except: 
                continue
        
        if not success: 
            print(f"⚠️ {prod_name} 嘗試近 5 日均無資料")

    # --- 5. 寫回 Sheets (保持排序：最新在上) ---
    if collected_rows:
        df_new = pd.DataFrame(collected_rows, columns=HEADERS)
        all_vals = ws.get_all_values()
        df_old = pd.DataFrame(all_vals[1:], columns=HEADERS) if len(all_vals) > 1 else pd.DataFrame(columns=HEADERS)
        
        # 合併、去重、排序 (依據「日期」欄位降序排列)
        df_final = pd.concat([df_new, df_old]).drop_duplicates(subset=['日期', '商品'], keep='first')
        df_final = df_final.sort_values(by='日期', ascending=False)
        
        ws.clear()
        ws.append_row(HEADERS)
        ws.append_rows(df_final.values.tolist())
        print(f"✅ 任務完成。目前時間: {today_str}_{exec_time_str} (UTC+8)")

if __name__ == "__main__":
    fetch_market_12_with_fallback()
