import feedparser
import gspread
from google.oauth2.service_account import Credentials
import trafilatura
from datetime import datetime, timedelta
import time
import os
import json

# --- 設定區域 ---
SHEET_ID = ''
SERVICE_ACCOUNT_FILE = 'creds.json' # 本地測試用
SHEET_NAME = 'RSS' # Google_Excel分頁名稱

RSS_URLS = [
    'https://news.cnyes.com/rss/category/tw_stock', #鉅亨網
    'https://www.ctee.com.tw/rss/news', #工商時報
    'https://tw.stock.yahoo.com/rss?category=tw-market', #Yahoo 股市
    'https://technews.tw/category/component/feed/', #科技新報
    'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664', #CNBC
    'https://finance.yahoo.com/news/rssindex', #Yahoo Finance
    'https://news.google.com/rss/search?q=China+economy&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', #Google News
    'https://www.coindesk.com/arc/outboundfeeds/rss/', #CoinDesk
    'https://cointelegraph.com/rss' #Cointelegraph
]

def get_google_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # 支援 GitHub Actions Secrets: 優先檢查環境變數
    if os.getenv('GOOGLE_CREDS_JSON'):
        creds_dict = json.loads(os.getenv('GOOGLE_CREDS_JSON'))
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    
    try:
        worksheet = sh.worksheet(SHEET_NAME)
        existing_titles = set(worksheet.col_values(2))
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows="1000", cols="4")
        worksheet.append_row(["日期時間", "標題", "內文", "來源網址"])
        existing_titles = set(["標題"])
        
    return worksheet, existing_titles

def fetch_full_text(url):
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded is None: return "無法下載"
        result = trafilatura.extract(downloaded, include_comments=False, include_tables=True)
        return result if result else "擷取不到正文"
    except:
        return "錯誤"

def cleanup_old_data(worksheet):
    """防止試算表爆炸：保留最新 7000 筆，刪除其餘舊資料"""
    try:
        all_values = worksheet.col_values(2) # 檢查標題欄
        total_rows = len(all_values)
        if total_rows > 7000: 
            print(f"清理舊資料中... 目前行數: {total_rows}，刪除第 7001 行後的資料")
            worksheet.delete_rows(5001, total_rows)
    except Exception as e:
        print(f"清理失敗: {e}")

def main():
    try:
        worksheet, existing_titles = get_google_sheet()
        print(f"成功連線！目前已有 {len(existing_titles)} 則舊紀錄。")
    except Exception as e:
        print(f"連線失敗: {e}")
        return

    new_data = []

    for rss_url in RSS_URLS:
        print(f"\n[掃描] {rss_url}")
        feed = feedparser.parse(rss_url)
        
        for entry in reversed(feed.entries):
            title = getattr(entry, 'title', '無標題').strip()
            link = getattr(entry, 'link', '')
            
            if not link or title in existing_titles:
                continue 

            dt_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if hasattr(entry, 'published'): dt_str = entry.published
            
            print(f"  - 新文章: {title[:20]}...")
            full_content = fetch_full_text(link)
            new_data.append([dt_str, title, full_content, link])
            time.sleep(1) 

    if new_data:
        new_data.reverse()
        print(f"\n正在將 {len(new_data)} 筆新資料插入到頂端...")
        worksheet.insert_rows(new_data, row=2)
        cleanup_old_data(worksheet) # 每次更新後順便清理
        print("🎉 更新與清理完成！")
    else:
        print("\n✅ 沒有發現新文章。")

if __name__ == "__main__":
    main()