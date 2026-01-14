import feedparser
import gspread
from google.oauth2.service_account import Credentials
import trafilatura
from datetime import datetime, timedelta
import time
import os
import json
from bs4 import BeautifulSoup

# --- 隱私設定區域 ---
SHEET_ID = os.getenv('SHEET_ID')
SHEET_NAME = 'RSS'

# 您整理後的台股/全球宏觀最優清單
RSS_URLS = [
    'https://news.cnyes.com/rss/category/tw_stock',         # 鉅亨台股
    'https://www.ctee.com.tw/rss/news',                    # 工商時報
    'https://technews.tw/category/component/feed/',        # 科技新報
    'https://udn.com/rssfeed/news/2/6644?ch=news',         # 經濟日報
    'https://www.chinatimes.com/rss/finance.xml?chdtv',     # 中時財經
    'https://www.digitimes.com.tw/rss/news.xml',           # Digitimes (半導體命脈)
    'https://cn.wsj.com/zh-hant/rss',                      # 華爾街日報 (中文)
    'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664', # CNBC
    'https://www.macromicro.me/rss',                       # 財經 M 平方 (宏觀趨勢)
    'https://www.cw.com.tw/rss/channel/3'                  # 天下雜誌 (深度評論)
]

def get_google_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv('GOOGLE_CREDS_JSON')
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
    else:
        creds = Credentials.from_service_account_file('creds.json', scopes=scopes)
    
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows="1000", cols="4")
        worksheet.append_row(["時間", "標題", "內容", "連結"])
    
    # 讀取標題避免重複
    existing_titles = set(worksheet.col_values(2)[:300]) 
    return worksheet, existing_titles

def fetch_content_with_fallback(entry):
    """嘗試抓取全文，若失敗則改抓 RSS 內建摘要"""
    url = getattr(entry, 'link', '')
    
    # 1. 優先嘗試抓取全文
    try:
        downloaded = trafilatura.fetch_url(url, user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        if downloaded:
            content = trafilatura.extract(downloaded, include_comments=False)
            if content and len(content) > 150:
                return content
    except:
        pass

    # 2. 如果全文被擋，從 RSS entry 提取摘要
    summary_raw = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
    if summary_raw:
        # 清除摘要裡的 HTML 標籤
        clean_summary = BeautifulSoup(summary_raw, "html.parser").get_text().strip()
        if clean_summary:
            return f"[摘要] {clean_summary[:600]}" # 標記為摘要，取前 600 字
            
    return "無法解析內文 (網站保護中)"

def main():
    try:
        worksheet, existing_titles = get_google_sheet()
        print(f"✅ 連線成功！")
    except Exception as e:
        print(f"❌ 連線失敗: {e}"); return

    new_data = []
    tw_time = datetime.utcnow() + timedelta(hours=8)
    now_str = tw_time.strftime('%Y-%m-%d %H:%M:%S')

    for rss_url in RSS_URLS:
        print(f"掃描中: {rss_url}")
        feed = feedparser.parse(rss_url)
        
        for entry in reversed(feed.entries):
            title = getattr(entry, 'title', '無標題').strip()
            if not title or title in existing_titles:
                continue 

            print(f"  🆕 新文章: {title[:20]}...")
            
            # 使用保險抓取邏輯
            final_content = fetch_content_with_fallback(entry)
            
            new_data.append([now_str, title, final_content, entry.link])
            time.sleep(1) # 增加穩定性

    if new_data:
        worksheet.insert_rows(new_data, row=2)
        print(f"🚀 已成功寫入 {len(new_data)} 筆新聞！")
    else:
        print("💡 目前無新文章。")

if __name__ == "__main__":
    main()
