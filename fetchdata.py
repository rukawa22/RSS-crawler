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
    # --- 1. 環境變數與路徑設定 (保持原樣) ---
    try:
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
    except:
        pass
    
    SPREADSHEET_ID = os.getenv('SHEET_ID')
    SHEET_NAME = 'MarketData'
    HEADERS = ["日期", "商品", "開盤價", "最高價", "最低價", "收盤價", "備註"]
    
    TW_STOCKS = {"2330": "2330", "2317": "2317", "2308": "2308", "2454": "2454", "2881": "2881"}
    US_INDICES = {"^SOX": "^SOX", "^IXIC": "^IXIC", "^GSPC": "^S&P 500", "^DJI": "^DJI"}

    # --- 2. Google Sheets 連線 (保持原樣) ---
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

    # --- 3. 時間設定 ---
    utc_now = datetime.datetime.utcnow()
    tw_now = utc_now + datetime.timedelta(hours=8)
    
    exec_time_str = tw_now.strftime('%H:%M')
    today_str = tw_now.strftime('%Y/%m/%d')
    yesterday_str = (tw_now - datetime.timedelta(days=1)).strftime('%Y/%m/%d')
    dates_to_try_generic = [today_str, yesterday_str]

    collected_rows = []

    # A. 台股個股 + 加權指數 (原始邏輯)
    for stock_no, name in {**TW_STOCKS, "IX0001": "加權TPE: IX0001"}.items():
        success = False
        for d_str in dates_to_try_generic:
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

    # B. 美股指數 (原始邏輯)
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

    # --- C. 台指期 (精準時段過濾與回溯邏輯) ---
    contract_month = tw_now.strftime('%Y%m')
    
    # 建立 15 天回溯清單，確保涵蓋長假
    taifex_backtrack = [(tw_now - datetime.timedelta(days=i)).strftime('%Y/%m/%d') for i in range(15)]

    # 交易時段標籤對照 (用於網頁文字比對，防止誤抓)
    session_map = {
        "0": {"label": "", "web_text": "一般交易時段"},
        "1": {"label": "-夜", "web_text": "盤後交易時段"}
    }

    for sess_code, info in session_map.items():
        success = False
        prod_name = f"TX{contract_month}{info['label']}"
        
        for d_str in taifex_backtrack:
            try:
                url = 'https://www.taifex.com.tw/cht/3/futDailyMarketReport'
                payload = {
                    'queryType': '2', 
                    'marketCode': sess_code, 
                    'commodity_id': 'TX', 
                    'queryDate': d_str
                }
                # 增加 headers 模擬真實瀏覽器，減少被阻擋或回傳舊快取的機率
                resp = requests.post(url, data=payload, verify=False, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # 關鍵保險 1：檢查網頁標題或內容是否包含該時段的正確文字
                # 避免 sess_code=0 (一般) 卻抓到 sess_code=1 (夜盤) 的快取網頁
                web_content = soup.get_text()
                if info['web_text'] not in web_content:
                    continue

                found_valid_row = False
                for tr in soup.find_all('tr'):
                    tds = tr.find_all('td')
                    # 關鍵保險 2：比對契約代碼與月份
                    if len(tds) >= 6 and tds[0].get_text(strip=True) == 'TX' and tds[1].get_text(strip=True) == contract_month:
                        open_p = tds[2].get_text(strip=True).replace(',', '')
                        
                        # 關鍵保險 3：排除無效報價
                        if open_p not in ['-', '', '0', '0.0']:
                            note = "期交所官方" if d_str == today_str else f"期交所官方({d_str})"
                            
                            collected_rows.append([
                                f"{today_str}_{exec_time_str}", 
                                prod_name,
                                float(open_p), 
                                float(tds[3].get_text(strip=True).replace(',', '')),
                                float(tds[4].get_text(strip=True).replace(',', '')), 
                                float(tds[5].get_text(strip=True).replace(',', '')), 
                                note
                            ])
                            found_valid_row = True
                            break
                
                if found_valid_row:
                    success = True
                    break # 找到該時段最新的一筆有效日期，停止回溯
            except Exception as e:
                print(f"⚠️ 抓取 {prod_name} 於 {d_str} 時發生錯誤: {e}")
                continue

    # --- 5. 寫回 Sheets ---
    if collected_rows:
        df_new = pd.DataFrame(collected_rows, columns=HEADERS)
        all_vals = ws.get_all_values()
        df_old = pd.DataFrame(all_vals[1:], columns=HEADERS) if len(all_vals) > 1 else pd.DataFrame(columns=HEADERS)
        
        df_final = pd.concat([df_new, df_old]).drop_duplicates(subset=['日期', '商品'], keep='first')
        df_final = df_final.sort_values(by='日期', ascending=False)
        
        ws.clear()
        ws.append_row(HEADERS)
        ws.append_rows(df_final.values.tolist())
        print(f"✅ 更新完成。目前時間: {today_str}_{exec_time_str}")

if __name__ == "__main__":
    fetch_market_12_with_fallback()



