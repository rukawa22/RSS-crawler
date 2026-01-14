import feedparser
import gspread
from google.oauth2.service_account import Credentials
import trafilatura
from datetime import datetime, timedelta
import time
import os
import json
from bs4 import BeautifulSoup

# --- 設定區域 (從 GitHub Secrets 讀取) ---
SHEET_ID = os.getenv('SHEET_ID')
SHEET_NAME = 'CryptoRss'  # 指定寫入的分頁名稱

# 您指定的純加密貨幣來源清單
RSS_URLS = [
    # --- 台灣加密貨幣與區塊鏈 (中文，成功率極高) ---
    'https://news.cnyes.com/rss/category/crypto',           # 鉅亨網加密貨幣
    'https://technews.tw/category/blockchain/feed/',        # 科技新報區塊鏈

    # --- 美國主流加密媒體 (英文，產業權威) ---
    'https://decrypt.co/feed',                             # Decrypt
    'https://www.theblock.co/rss.xml',                     # The Block
    'https://bitcoinmagazine.com/.rss/full/',              # Bitcoin Magazine
    'https://www.coindesk.com/arc/outboundfeeds/rss/',     # CoinDesk
    'https://www.newsbtc.com/feed/'                        # NewsBTC
]

def get_google_sheet():
    """連接 Google Sheets，若分頁 CryptoRss 不存在則建立"""
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv('GOOGLE_CREDS_JSON')
    
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
    else:
        creds = Credentials.from_service_account_file('creds.json', scopes=scopes)
    
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    
    # 檢查分頁是否存在，不存在就建立
    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"⚠️ 找不到分頁 '{SHEET_NAME}'，正在建立新分頁...")
        # 建立分頁並設定欄位標題
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows="2000", cols="4")
        worksheet.append_row(["日期時間", "標題", "內文", "來源網址"])
    
    # 讀取現有標題 (避免重複抓取)，取最近 300 筆
    existing_titles = set(worksheet.col_values(2)[:300])
    return worksheet, existing_titles

def fetch_content_with_fallback(entry):
    """嘗試抓取全文，若失敗則回傳 RSS 摘要備案"""
    url = getattr(entry, 'link', '')
    if not url: return "無連結"

    # 1. 嘗試抓取全文 (使用偽裝瀏覽器)
    try:
        downloaded = trafilatura.fetch_url(url, user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        if downloaded:
            content = trafilatura.extract(downloaded, include_comments=False)
            if content and len(content) > 150:
                return content
    except:
        pass

    # 2. 全文抓取失敗，改抓 RSS 摘要
    summary_raw = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
    if summary_raw:
        clean_summary = BeautifulSoup(summary_raw, "html.parser").get_text().strip()
        if clean_summary:
            return f"[摘要備案] {clean_summary[:600]}"
            
    return "無法解析內文與摘要 (受網站保護)"

def main():
    try:
        worksheet, existing_titles = get_google_sheet()
        print(f"✅ 成功連線到試算表分頁: {SHEET_NAME}")
    except Exception as e:
        print(f"❌ 連線失敗: {e}"); return

    new_data = []
    # 統一台灣時間 (UTC+8)
    tw_time = datetime.utcnow() + timedelta(hours=8)
    now_str = tw_time.strftime('%Y-%m-%d %H:%M:%S')

    for rss_url in RSS_URLS:
        print(f"正在掃描: {rss_url}")
        feed = feedparser.parse(rss_url)
        
        # 倒序處理，確保最新的在最上面
        for entry in reversed(feed.entries):
            title = getattr(entry, 'title', '無標題').strip()
            
            if not title or title in existing_titles:
                continue 

            print(f"  ✨ 發現新加密新聞: {title[:25]}...")
            
            # 取得內容 (全文或摘要)
            final_content = fetch_content_with_fallback(entry)
            
            new_data.append([now_str, title, final_content, entry.link])
            time.sleep(1.2) # 稍微增加延遲，對國外站點更友善

    if new_data:
        # 將新資料插入到標題列下方的第一列 (row=2)
        worksheet.insert_rows(new_data, row=2)
        print(f"🚀 成功更新 {len(new_data)} 筆加密貨幣新聞！")
    else:
        print("💡 目前無新資訊。")

if __name__ == "__main__":
    main()
