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
    # --- 1. 環境變數與路徑設定 ---
    # 這裡保留您的 os.chdir，但在 GitHub Actions 環境中若失敗會跳過
    try:
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
    except:
        pass
    
    # 優先讀取 Secret 中的 SHEET_ID，若無則使用預設值
    SPREADSHEET_ID = os.getenv('SHEET_ID')
    SHEET_NAME = 'MarketData'
    HEADERS = ["日期", "商品", "開盤價", "最高價", "最低價", "收盤價", "備註"]
    
    # 您原始的 12 檔清單
    TW_STOCKS = {"2330": "2330", "2317": "2317", "2308": "2308", "2454": "2454", "2881": "2881"}
    US_INDICES = {"^SOX": "^SOX", "^IXIC": "^IXIC", "^GSPC": "^S&P 500", "^DJI": "^DJI"}

    # --- 2. Google Sheets 連線 (支援環境變數 JSON) ---
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 核心修正：讀取 GitHub 傳入的 JSON 字串
        creds_json = os.getenv('GOOGLE_CREDS_JSON')
        
        if creds_json:
            # GitHub 環境：將 Secret 字串解析為字典
            creds_dict = json.loads(creds_json)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            # 本地環境：讀取實體檔案
            creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
            
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet(SHEET_NAME)
    except Exception as e:
        print(f"❌ Sheets 連接失敗: {e}"); return

    # --- 3. 原始抓取邏輯 (完全保留) ---
    collected_rows = []
    now = datetime.datetime.now()
    exec_time_str = now.strftime('%H:%M')
    
    today_str = now.strftime('%Y/%m/%d')
    yesterday_str = (now - datetime.timedelta(days=1)).strftime('%Y/%m/%d')
    dates_to_try = [today_str, yesterday_str]

    # --- A. 台股個股 + 加權指數 (帶回溯邏輯) ---
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
        if not success: print(f"⚠️ {stock_no} 嘗試兩日均無資料")

    # --- B. 美股指數 (Yahoo API 內建回溯) ---
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
        except: print(f"⚠️ 美股 {yf_code} 抓取失敗")

    # --- C. 台指期 (帶回溯邏輯) ---
    contract_month = now.strftime('%Y%m')
    for sess_code, sess_label in [("0", ""), ("1", "-夜")]:
        success = False
        prod_name = f"TX{contract_month}{sess_label}"
        for d_str in dates_to_try:
            try:
                url = 'https://www.taifex.com.tw/cht/3/futDailyMarketReport'
                payload = {'queryType': '2', 'marketCode': sess_code, 'commodity_id': 'TX', 'queryDate': d_str}
                resp = requests.post(url, data=payload, verify=False, timeout=10)
                soup = BeautifulSoup(resp.text, 'html.parser')
                for tr in soup.find_all('tr'):
                    tds = tr.find_all('td')
                    if len(tds) >= 6 and tds[0].get_text(strip=True) == 'TX' and tds[1].get_text(strip=True) == contract_month:
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

    # --- D. 寫回 Sheets (去重並排序) ---
    if collected_rows:
        df_new = pd.DataFrame(collected_rows, columns=HEADERS)
        all_vals = ws.get_all_values()
        df_old = pd.DataFrame(all_vals[1:], columns=HEADERS) if len(all_vals) > 1 else pd.DataFrame(columns=HEADERS)
        
        # 這裡保留您的 drop_duplicates 與 sort 邏輯
        df_final = pd.concat([df_new, df_old]).drop_duplicates(subset=['日期', '商品'], keep='first').sort_values(by='日期', ascending=False)
        
        # 清除並寫回
        ws.clear(); ws.append_row(HEADERS); ws.append_rows(df_final.values.tolist())
        print(f"✅ 06:30 任務完成，目前更新: {len(df_new)} 檔")

if __name__ == "__main__":
    fetch_market_12_with_fallback()
