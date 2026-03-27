import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import time
import urllib.parse
import re
import json
import random  

# === 基本設定 ===
# 設定要爬取的目標看板與關鍵字
BOARDS = ['Military', 'Gossiping'] # 同時爬取戰爭與八卦版
KEYWORDS = '伊朗'

# 📍 設定時間範圍：修改為 2026/3/1 到 2026/3/20
# 用於過濾文章，確保只保留在此時間區間內的討論
START_DATE = datetime(2026, 3, 1, 0, 0, 0)
END_DATE = datetime(2026, 3, 20, 23, 59, 59)
CSV_FILENAME = 'ptt_iran_20260301_0320_full.csv'

# 更新 headers：偽裝成真實瀏覽器發出請求，降低被 PTT 伺服器阻擋的機率
# Connection: 'close' 避免佔用連線資源 (keep-alive 有時會導致 PTT 阻擋)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'close' 
}
# 寫入過渡期 Cookie，讓程式可以順利無聲通過八卦板 (Gossiping) 的 18 禁年齡確認機制
cookies = {'over18': '1'} 

# 使用 Session 物件，這樣同一個對話的所有請求都會自動帶上我們設定好的 headers 與 cookies
session = requests.Session()
session.headers.update(headers)
session.cookies.update(cookies)

# 📍 移除 e_ip 欄位
# 定義最後要寫入 CSV 的欄位名稱
FIELDNAMES = [
    'system_id', 'artUrl', 'artTitle', 'artDate', 'artPoster', 
    'artCatagory', 'artContent', 'artComment', 'insertedDate', 'dataSource'
]

def safe_get(url, max_retries=3):
    """安全獲取網頁，遇到斷線會自動重試。這是爬蟲穩定的關鍵！"""
    for attempt in range(max_retries):
        try:
            # 設定 timeout 為 10 秒，避免遇到無回應的伺服器而卡死
            res = session.get(url, timeout=10) 
            return res
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # 若發生連線錯誤或超時，進行指數退避 (Exponential Backoff) 的等待策略後重試
            print(f"⚠️ 連線不穩 ({e})，等待 {5 * (attempt + 1)} 秒後進行第 {attempt + 1} 次重試...")
            time.sleep(5 * (attempt + 1))
    return None

def get_article_details(url):
    """進入文章內部抓取完整資訊，並將推文 (留言) 整理成 JSON 格式"""
    res = safe_get(url)
    if not res:
        return None, "", "", "[]" # 📍 移除回傳 e_ip 的空字串

    try:
        soup = BeautifulSoup(res.text, 'html.parser')
        main_content = soup.find('div', id='main-content') # PTT 文章的主體都包在這個 div 內
        if not main_content:
            return None, "", "", "[]"

        # --- 1. 抓取文章 Meta 資訊 (時間、作者) ---
        metalines = main_content.find_all('div', class_='article-metaline')
        article_date_str = ""
        artPoster = ""
        for meta in metalines:
            tag = meta.find('span', class_='article-meta-tag')
            # 辨識標籤名稱，精準抓取時間與作者
            if tag and tag.text == '時間':
                article_date_str = meta.find('span', class_='article-meta-value').text
            elif tag and tag.text == '作者':
                author_text = meta.find('span', class_='article-meta-value').text
                artPoster = author_text.split(' ')[0] # 取得作者ID (去除後面可能帶有的暱稱)
        
        # 將 PTT 的時間字串轉為 Python 的 datetime 物件，方便後續比對日期範圍
        article_datetime = None
        if article_date_str:
            try:
                article_datetime = datetime.strptime(article_date_str, '%a %b %d %H:%M:%S %Y')
            except ValueError:
                pass

        # 📍 (已刪除抓取 e_ip 的段落)

        # --- 2. 抓取所有留言 (轉換成JSON格式) ---
        comments_list = []
        pushes = main_content.find_all('div', class_='push') # 每一個推文都是一個 class='push' 的 div
        for push in pushes:
            # 萃取推文狀態(推/噓/→)、帳號、內文
            p_tag = push.find('span', class_='push-tag').text.strip() if push.find('span', class_='push-tag') else ""
            p_userid = push.find('span', class_='push-userid').text.strip() if push.find('span', class_='push-userid') else ""
            p_content = push.find('span', class_='push-content').text.strip() if push.find('span', class_='push-content') else ""
            
            # 清理時間並補齊年份 (因為 PTT 留言時間通常只顯示 MM/DD HH:MM，沒有年份)
            p_ipdatetime = push.find('span', class_='push-ipdatetime').text.strip() if push.find('span', class_='push-ipdatetime') else ""
            formatted_date = p_ipdatetime
            if article_datetime:
                # 使用 Regex 提取 月/日 與 時:分
                date_match = re.search(r'(\d{2}/\d{2})\s+(\d{2}:\d{2})', p_ipdatetime)
                if date_match:
                    md = date_match.group(1).replace('/', '-')
                    hm = date_match.group(2)
                    # 借用發文年份來補齊推文的年份 (注意：如果是跨年文章這招會有瑕疵，但短期區間適用)
                    formatted_date = f"{article_datetime.year}-{md} {hm}:00"

            # 將單筆推文整理成字典
            comments_list.append({
                "cmtStatus": p_tag,
                "cmtPoster": p_userid,
                "cmtContent": p_content,
                "cmtDate": formatted_date
            })
            # 將推文區塊從 HTML 樹狀結構中拔除，以免干擾後面抓取「純內文」
            push.extract() 

        # 將推文清單轉為 JSON 格式字串，方便未來存入資料庫或 DataFrame
        artComment = json.dumps(comments_list, ensure_ascii=False)

        # --- 3. 抓取純內文 (移除剩餘的 Meta 資訊) ---
        # 把發文作者、時間、標題等系統標籤也拔除
        for meta in main_content.find_all('div', class_=['article-metaline', 'article-metaline-right']):
            meta.extract()
            
        # 剩下的就是純淨的文章內容
        content = main_content.text.strip()
        
        # 📍 移除回傳 e_ip
        return article_datetime, artPoster, content, artComment

    except Exception as e:
        print(f"解析文章發生錯誤 {url}: {e}")
        return None, "", "", "[]"

def crawl_ptt():
    results = []
    system_id = 1
    
    print(f"🚀 開始爬取 {BOARDS} 版，關鍵字：「{KEYWORDS}」")
    print(f"📅 目標範圍：{START_DATE.strftime('%Y-%m-%d')} 至 {END_DATE.strftime('%Y-%m-%d')}")
    
    for board in BOARDS:
        # 將中文關鍵字轉碼為 URL 格式
        encoded_keywords = urllib.parse.quote_plus(KEYWORDS)
        base_url = f"https://www.ptt.cc/bbs/{board}/search?q={encoded_keywords}"
        current_url = base_url
        has_next_page = True

        print(f"\n================ 開始掃描 {board} 版 ================")
        
        # 迴圈處理每一頁的搜尋結果
        while has_next_page:
            print(f"正在掃描搜尋頁面: {current_url}")
            
            # 📍 加快搜尋換頁速度：固定 0.5 秒 (降低對伺服器負擔同時保持效率)
            time.sleep(0.5)
            
            res = safe_get(current_url)
            if not res: break
                
            soup = BeautifulSoup(res.text, 'html.parser')
            # 抓取該頁所有的文章列表區塊 (class='r-ent')
            articles = soup.find_all('div', class_='r-ent')
            
            if not articles:
                print("沒有更多搜尋結果。")
                break

            for article in articles:
                title_a = article.find('div', class_='title').find('a')
                if not title_a: continue # 略過被刪除的文章 (被刪除的文章會沒有 <a> 標籤)
                    
                title = title_a.text
                link = "https://www.ptt.cc" + title_a['href']
                
                # 📍 加快抓取速度：縮短文章內頁抓取的隨機延遲為 0.5 ~ 1.0 秒
                delay_time = random.uniform(0.5, 1.0)
                time.sleep(delay_time) 
                
                # 📍 移除 e_ip 變數的接收
                article_datetime, artPoster, content, artComment = get_article_details(link)
                
                if article_datetime:
                    # 判斷文章發布時間是否在我們設定的時間範圍內
                    if START_DATE <= article_datetime <= END_DATE:
                        print(f"✅ [符合日期] {article_datetime.strftime('%m-%d %H:%M')} - {title}")
                        
                        # 紀錄爬蟲當下寫入資料庫的時間
                        insertedDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        # 📍 寫入時移除 e_ip 欄位，將資料整理為 Dict 準備寫入 CSV
                        results.append({
                            'system_id': system_id,
                            'artUrl': link,
                            'artTitle': title,
                            'artDate': article_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            'artPoster': artPoster,
                            'artCatagory': board,
                            'artContent': content,
                            'artComment': artComment,
                            'insertedDate': insertedDate,
                            'dataSource': 'ptt'
                        })
                        system_id += 1
                        
                    # 【效能優化關鍵】：如果文章時間「早於」我們設定的開始時間，代表後面的文章更舊，直接中斷該看板的爬取
                    elif article_datetime < START_DATE:
                        print(f"⌛ 已掃描完畢至 {START_DATE.strftime('%Y-%m-%d')}，此看板爬蟲結束。")
                        has_next_page = False
                        break
                
            if has_next_page:
                # 尋找「上頁」按鈕
                # 注意：在 PTT 的搜尋功能中，「上頁」代表的是「更舊的搜尋結果」
                paging_div = soup.find('div', class_='btn-group btn-group-paging')
                if paging_div:
                    btns = paging_div.find_all('a')
                    # btns[1] 通常是 "‹ 上頁" 的按鈕
                    if len(btns) >= 2 and 'href' in btns[1].attrs:
                        current_url = "https://www.ptt.cc" + btns[1]['href']
                    else:
                        has_next_page = False # 沒有上頁按鈕代表到底了
                else:
                    has_next_page = False

    # --- 最終儲存 CSV ---
    if results:
        # encoding='utf-8-sig' 可以避免 Excel 打開 CSV 時出現中文亂碼
        with open(CSV_FILENAME, mode='w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(results)
        print(f"\n🎉 爬取成功！共抓取 {len(results)} 篇文章 (含留言) 至 {CSV_FILENAME}")
    else:
        print("\n⚠️ 該日期範圍內找不到相關文章。")

if __name__ == "__main__":
    crawl_ptt()