import feedparser
import gspread
from google.oauth2.service_account import Credentials
import trafilatura
from datetime import datetime
import time
import os
import json

# --- 隱私設定區域 (優先讀取 GitHub Secrets) ---
# 這樣你的 SHEET_ID 就不會出現在程式碼中
SHEET_ID = os.getenv('SHEET_ID', '1ooE30J2aXm0wbsSnqXHSg--yIEVAywQ7GwJbRqFbc6g')
SHEET_NAME = 'RSS'

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
    
    # 支援 GitHub Actions Secrets
    creds_json = os.getenv('GOOGLE_CREDS_JSON')
    
    if creds_json:
        # 在 GitHub Actions 運行時讀取 Secret
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        # 在本地電腦運行時讀取檔案
        creds = Credentials.from_service_account_file('creds.json', scopes=scopes)
    
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    
    try:
        worksheet = sh.worksheet(SHEET_NAME)
        # 讀取標題欄用於去重
        existing_titles = set(worksheet.col_values(2))
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows="5000", cols="4")
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
        return "擷取錯誤"

def cleanup_old_data(worksheet):
    """只保留一週約 5000 筆資料，防止試算表卡死"""
    try:
        all_titles = worksheet.col_values(2)
        total_rows = len(all_titles)
        if total_rows > 5000:
            print(f"清理舊資料中... 目前行數: {total_rows}")
            # 刪除第 5001 行之後的所有行
            worksheet.delete_rows(5001, total_rows)
    except Exception as e:
        print(f"清理失敗: {e}")

def main():
    try:
        worksheet, existing_titles = get_google_sheet()
        print(f"成功連線！目前已有 {len(existing_titles)} 則舊紀錄。")
    except Exception as e:
        print(f"連線失敗，請檢查 SHEET_ID 或 Secrets 設定: {e}")
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
        print(f"\n正在插入 {len(new_data)} 筆新資料到頂端...")
        worksheet.insert_rows(new_data, row=2)
        cleanup_old_data(worksheet)
        print("🎉 更新完成！")
    else:
        print("\n✅ 沒有發現新文章。")

if __name__ == "__main__":
    main()
