import feedparser
import gspread
from google.oauth2.service_account import Credentials
import trafilatura
from datetime import datetime, timedelta
import time
import os
import json

# --- 隱私設定區域 (優先讀取 GitHub Secrets) ---
SHEET_ID = os.getenv('SHEET_ID')
SHEET_NAME = 'RSS'

RSS_URLS = [
    'https://news.cnyes.com/rss/category/tw_stock', 
    'https://www.ctee.com.tw/rss/news', 
    'https://tw.stock.yahoo.com/rss?category=tw-market', 
    'https://technews.tw/category/component/feed/', 
    'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664', 
    'https://finance.yahoo.com/news/rssindex', 
    'https://news.google.com/rss/search?q=China+economy&hl=zh-TW&gl=TW&ceid=TW:zh-Hant' 
]

def get_google_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # 統一金鑰讀取邏輯 (解決 GitHub Actions 報錯)
    creds_json = os.getenv('GOOGLE_CREDS_JSON')
    if creds_json:
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        # 本地開發用
        creds = Credentials.from_service_account_file('creds.json', scopes=scopes)
    
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    
    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows="5000", cols="4")
        worksheet.append_row(["日期", "標題", "內文摘要", "連結"])
        
    titles = worksheet.col_values(2) # 獲取現有標題以去重
    return worksheet, titles

def fetch_full_text(url):
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            content = trafilatura.extract(downloaded)
            return content if content else "無法解析內文"
    except Exception as e:
        print(f"解析失敗 {url}: {e}")
    return "抓取內文出錯"

def cleanup_old_data(worksheet):
    try:
        total_rows = len(worksheet.get_all_values())
        if total_rows > 5000:
            print(f"清理舊資料中... 目前行數: {total_rows}")
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
    # 強制獲取台灣時間 (UTC+8)
    tw_time = datetime.utcnow() + timedelta(hours=8)
    dt_str = tw_time.strftime('%Y-%m-%d %H:%M:%S')

    for rss_url in RSS_URLS:
        print(f"\n[掃描] {rss_url}")
        feed = feedparser.parse(rss_url)
        
        # 維持原始的倒序讀取邏輯
        for entry in reversed(feed.entries):
            title = getattr(entry, 'title', '無標題').strip()
            link = getattr(entry, 'link', '')
            
            if not link or title in existing_titles:
                continue 

            print(f"  - 新文章: {title[:20]}...")
            full_content = fetch_full_text(link)
            # 將最新抓到的資料放入 list
            new_data.append([dt_str, title, full_content, link])
            time.sleep(0.5) 

    if new_data:
        # 這裡反轉 new_data 確保這一批裡「最新」的在最前面
        new_data.reverse()
        print(f"\n正在插入 {len(new_data)} 筆新資料到頂端...")
        # 插入在第二行 (標題列下方)，這會確保最新的資料永遠在最上面
        worksheet.insert_rows(new_data, row=2)
        cleanup_old_data(worksheet)
        print("🎉 更新完成！")
    else:
        print("\n目前沒有新的文章。")

if __name__ == "__main__":
    main()
