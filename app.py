# -*- coding: utf-8 -*-
"""
Amazon US - Beauty & Personal Care Best Sellers Top 100
- Page 1: https://www.amazon.com/gp/bestsellers/beauty/ref=zg_b_bs_beauty_1
- Page 2: https://www.amazon.com/Best-Sellers-Beauty-Personal-Care/zgbs/beauty/ref=zg_bs_pg_2_beauty?_encoding=UTF8&pg=2
- HTTP(정적) 우선 → 429/부족 시 Playwright(동적) 폴백
- 파일명: 아마존US_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- 전일 CSV와 Top30 비교 → Slack 알림

필요 Secrets:
  SLACK_WEBHOOK_URL
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
  GDRIVE_FOLDER_ID
"""
import os, re, io, math, pytz, time, random, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ----------------- 기본 설정 -----------------
KST = pytz.timezone("Asia/Seoul")
PAGE_URLS = [
    "https://www.amazon.com/gp/bestsellers/beauty/ref=zg_b_bs_beauty_1",
    "https://www.amazon.com/Best-Sellers-Beauty-Personal-Care/zgbs/beauty/ref=zg_bs_pg_2_beauty?_encoding=UTF8&pg=2",
]
UA_POOL = [
    # 최근 Chrome UA 몇 개 로테이션
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"아마존US_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

USD_RE = re.compile(r"(?:US\$|\$)\s*([\d]{1,3}(?:,\d{3})*(?:\.\d{2})|[\d]+(?:\.\d{2})?)")
ASIN_RE = re.compile(r"/dp/([A-Z0-9]{10})")

def parse_usd_all(text: str) -> List[float]:
    vals = []
    for m in USD_RE.finditer(text or ""):
        try: vals.append(float(m.group(1).replace(",", "")))
        except: pass
    return [v for v in vals if v > 0]  # 0달러/노이즈 제거

def fmt_currency_usd(v) -> str:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)): return "$0.00"
        return f"${float(v):,.2f}"
    except: return "$0.00"

def discount_floor(orig: Optional[float], sale: Optional[float]) -> Optional[int]:
    if orig and sale and orig > 0:
        return max(0, int(math.floor((1 - sale / orig) * 100)))
    return None

# ----------------- 데이터 모델 -----------------
@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[float]
    orig_price: Optional[float]
    discount_percent: Optional[int]
    url: str
    asin: str = ""

# ----------------- 보조 -----------------
def canonical_amz_link(href: str) -> str:
    if not href: return ""
    if href.startswith("/"): href = urljoin("https://www.amazon.com", href)
    m = ASIN_RE.search(href)
    if not m: return href
    asin = m.group(1)
    return f"https://www.amazon.com/dp/{asin}"

def extract_asin(href: str) -> str:
    m = ASIN_RE.search(href or "")
    return m.group(1) if m else ""

def nearest_text_block(el):
    """가격 탐색용: 앵커의 상위 컨테이너 텍스트 합치기"""
    txt = ""
    cur = el
    for _ in range(6):
        if cur is None: break
        try:
            txt = clean_text(cur.get_text(" ", strip=True))
            if len(txt) >= 20: break
        except: pass
        cur = cur.parent
    return txt

# ----------------- 정적 파싱 -----------------
def parse_http(html: str, offset: int) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.select("a[href*='/dp/']")
    items: List[Product] = []
    seen: set = set()

    for a in anchors:
        href = a.get("href") or ""
        asin = extract_asin(href)
        if not asin or asin in seen:
            continue

        # 제목: aria-label > title > 텍스트 > img alt
        name = (a.get("aria-label") or a.get("title") or clean_text(a.get_text(" ", strip=True)) or "")
        if not name:
            img = a.find("img")
            if img and img.has_attr("alt"):
                name = clean_text(img["alt"])
        if not name:
            continue

        block_text = nearest_text_block(a)
        prices = parse_usd_all(block_text)
        sale = orig = None
        if len(prices) == 1:
            sale = prices[0]
        elif len(prices) >= 2:
            sale, orig = min(prices), max(prices)
            if sale == orig: orig = None

        link = canonical_amz_link(href)
        items.append(Product(
            rank=offset + len(items) + 1,
            brand="",
            title=name,
            price=sale,
            orig_price=orig,
            discount_percent=discount_floor(orig, sale),
            url=link,
            asin=asin
        ))
        if len(items) >= 50:  # 페이지당 50개
            break
        seen.add(asin)
    return items

def fetch_by_http() -> List[Product]:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept-Language": "en-US,en;q=0.9,ko;q=0.6",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    })
    all_items: List[Product] = []
    for idx, url in enumerate(PAGE_URLS):
        # 간단 백오프(429 등)
        last_err = None
        for attempt in range(3):
            try:
                r = s.get(url, timeout=25)
                if r.status_code == 429:
                    raise requests.HTTPError("429 Too Many Requests")
                r.raise_for_status()
                items = parse_http(r.text, offset=idx * 50)
                all_items.extend(items)
                # 페이지 간 짧은 랜덤 대기
                time.sleep(random.uniform(1.0, 2.0))
                break
            except Exception as e:
                last_err = e
                time.sleep(1.5 * (attempt + 1))
        if last_err and len(all_items) < (idx * 50 + 10):
            # 이 페이지는 사실상 실패로 보고 폴백에 맡김
            raise last_err
    return all_items

# ----------------- Playwright 폴백 -----------------
def fetch_page_playwright(url: str, offset: int) -> List[Product]:
    from playwright.sync_api import sync_playwright
    import pathlib

    def _dump(page, tag):
        pathlib.Path("data/debug").mkdir(parents=True, exist_ok=True)
        with open(f"data/debug/amazon_{tag}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        try: page.screenshot(path=f"data/debug/amazon_{tag}.png", full_page=True)
        except: pass

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            viewport={"width":1366,"height":900},
            locale="en-US",
            timezone_id="America/Los_Angeles",
            user_agent=random.choice(UA_POOL),
            extra_http_headers={"Accept-Language":"en-US,en;q=0.9,ko;q=0.6"},
        )
        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=30_000)
        except: pass

        # 쿠키/동의 모달 닫기
        for sel in ["#sp-cc-accept", "button[name='accept']", "input#sp-cc-accept", "button:has-text('Accept')"]:
            try: page.locator(sel).first.click(timeout=1200)
            except: pass

        # 충분히 로드될 때까지 천천히 스크롤
        for _ in range(10):
            try: page.mouse.wheel(0, 1600)
            except: pass
            page.wait_for_timeout(600)

        # 캡차 페이지 방어
        if "captcha" in (page.url or "").lower():
            _dump(page, f"captcha_{offset}")
            ctx.close(); browser.close()
            return []

        data = page.evaluate("""
            (offset) => {
              const rows = [];
              const seen = new Set();
              const anchors = Array.from(document.querySelectorAll("a[href*='/dp/']"));
              const usdRe = /(?:US\\$|\\$)\\s*([\\d]{1,3}(?:,\\d{3})*(?:\\.\\d{2})|[\\d]+(?:\\.\\d{2})?)/g;

              function canonical(href){
                const m = href.match(/\\/dp\\/([A-Z0-9]{10})/);
                if(!m) return href;
                return 'https://www.amazon.com/dp/' + m[1];
              }
              function nearestText(el){
                let cur = el, txt = '';
                for(let i=0;i<6 && cur;i++){
                  txt = (cur.innerText || '').replace(/\\s+/g,' ').trim();
                  if(txt.length>=20) break;
                  cur = cur.parentElement;
                }
                return txt;
              }

              for(const a of anchors){
                const href = a.getAttribute('href') || '';
                const m = href.match(/\\/dp\\/([A-Z0-9]{10})/);
                if(!m) continue;
                const asin = m[1];
                if(seen.has(asin)) continue;

                let name = (a.getAttribute('aria-label') || a.getAttribute('title') || (a.textContent||'')).replace(/\\s+/g,' ').trim();
                if(!name){
                  const img = a.querySelector('img[alt]');
                  if(img) name = (img.getAttribute('alt')||'').replace(/\\s+/g,' ').trim();
                }
                if(!name) continue;

                const block = nearestText(a);
                const prices = Array.from(block.matchAll(usdRe)).map(m => parseFloat(m[1].replace(/,/g,''))).filter(v => !isNaN(v) && v>0);
                let sale=null, orig=null;
                if(prices.length===1) sale = prices[0];
                else if(prices.length>=2){ sale=Math.min(...prices); orig=Math.max(...prices); if(sale===orig) orig=null; }

                rows.push({
                  rank: offset + rows.length + 1,
                  brand: '',
                  title: name,
                  price: sale,
                  orig_price: orig,
                  url: canonical(href),
                  asin
                });
                seen.add(asin);
                if(rows.length>=50) break;
              }
              return rows;
            }
        """, offset)
        ctx.close(); browser.close()

    out: List[Product] = []
    for r in data:
        out.append(Product(
            rank=int(r["rank"]),
            brand="",
            title=clean_text(r["title"]),
            price=r["price"],
            orig_price=r["orig_price"],
            discount_percent=discount_floor(r["orig_price"], r["price"]),
            url=r["url"],
            asin=r["asin"],
        ))
    return out

def fetch_by_playwright() -> List[Product]:
    all_items: List[Product] = []
    for idx, url in enumerate(PAGE_URLS):
        all_items.extend(fetch_page_playwright(url, offset=idx*50))
        # 페이지 간 짧은 대기(봇탐지 완화)
        time.sleep(1.0)
    return all_items

def fetch_products() -> List[Product]:
    try:
        items = fetch_by_http()
        if len(items) >= 60:
            return items[:100]
    except Exception as e:
        print("[HTTP 오류] → Playwright 폴백:", e)
    return fetch_by_playwright()[:100]

# ----------------- Drive -----------------
def normalize_folder_id(raw: str) -> str:
    if not raw: return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    cid  = os.getenv("GOOGLE_CLIENT_ID")
    csec = os.getenv("GOOGLE_CLIENT_SECRET")
    rtk  = os.getenv("GOOGLE_REFRESH_TOKEN")
    if not (cid and csec and rtk):
        raise RuntimeError("OAuth 자격정보가 없습니다. GOOGLE_* 확인")
    creds = Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                        client_id=cid, client_secret=csec)
    svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    try:
        about = svc.about().get(fields="user(displayName,emailAddress)").execute()
        u = about.get("user", {}); print(f"[Drive] user={u.get('displayName')} <{u.get('emailAddress')}>")
    except Exception as e:
        print("[Drive] whoami 실패:", e)
    return svc

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id,name)",
                               supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None
    buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        return file_id
    meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
    created = service.files().create(body=meta, media_body=media, fields="id",
                                     supportsAllDrives=True).execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload
    res = service.files().list(q=f"name = '{name}' and '{folder_id}' in parents and trashed = false",
                               fields="files(id,name)", supportsAllDrives=True,
                               includeItemsFromAllDrives=True).execute()
    files = res.get("files", [])
    if not files: return None
    fid = files[0]["id"]
    req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO(); dl = MediaIoBaseDownload(fh, req); done=False
    while not done: _, done = dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ----------------- Slack/메시지 -----------------
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text); return
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": date_str,
        "rank": p.rank,
        "brand": p.brand,
        "product_name": p.title,
        "price": p.price,
        "orig_price": p.orig_price,
        "discount_percent": p.discount_percent,
        "url": p.url,
        "asin": p.asin,
    } for p in products])

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "rising": [], "newcomers": [], "falling": [], "outs": [], "inout_count": 0}

    # TOP10
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        name_link = f"<{r['url']}|{slack_escape(clean_text(r['product_name']))}>"
        price_txt = fmt_currency_usd(r["price"])
        dc = r.get("discount_percent"); tail = f" (↓{int(dc)}%)" if pd.notnull(dc) else ""
        S["top10"].append(f"{int(r['rank'])}. {name_link} — {price_txt}{tail}")

    if df_prev is None or not len(df_prev):
        return S

    # 키: ASIN 우선, 없으면 URL
    df_t = df_today.copy()
    df_t["key"] = df_t.apply(lambda x: (str(x.get("asin")).strip() or str(x.get("url")).strip()), axis=1)
    df_t.set_index("key", inplace=True)

    df_p = df_prev.copy()
    df_p["key"] = df_p.apply(lambda x: (str(x.get("asin")).strip() or str(x.get("url")).strip()), axis=1)
    df_p.set_index("key", inplace=True)

    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)].copy()

    common = set(t30.index) & set(p30.index)
    new    = set(t30.index) - set(p30.index)
    out    = set(p30.index) - set(t30.index)

    # 급상승(3)
    rising = []
    for k in common:
        pr, cr = int(p30.loc[k,"rank"]), int(t30.loc[k,"rank"])
        imp = pr - cr
        if imp > 0:
            nm = slack_escape(t30.loc[k]["product_name"])
            rising.append((imp, cr, pr, nm, f"- <{t30.loc[k]['url']}|{nm}> {pr}위 → {cr}위 (↑{imp})"))
    rising.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["rising"] = [x[-1] for x in rising[:3]]

    # 뉴랭커(≤3)
    newcomers = []
    for k in new:
        cr = int(t30.loc[k,"rank"])
        nm = slack_escape(t30.loc[k]["product_name"])
        newcomers.append((cr, f"- <{t30.loc[k]['url']}|{nm}> NEW → {cr}위"))
    newcomers.sort(key=lambda x: x[0])
    S["newcomers"] = [x[1] for x in newcomers[:3]]

    # 급하락(5)
    falling = []
    for k in common:
        pr, cr = int(p30.loc[k,"rank"]), int(t30.loc[k,"rank"])
        drop = cr - pr
        if drop > 0:
            nm = slack_escape(t30.loc[k]["product_name"])
            falling.append((drop, cr, pr, nm, f"- <{t30.loc[k]['url']}|{nm}> {pr}위 → {cr}위 (↓{drop})"))
    falling.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["falling"] = [x[-1] for x in falling[:5]]

    # OUT
    for k in sorted(list(out)):
        pr = int(p30.loc[k,"rank"])
        nm = slack_escape(p30.loc[k]["product_name"])
        S["outs"].append(f"- <{p30.loc[k]['url']}|{nm}> {pr}위 → OUT")

    S["inout_count"] = len(new) + len(out)
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    lines: List[str] = []
    lines.append(f"*Amazon US Beauty & Personal Care Top 100 — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*");          lines.extend(S.get("top10") or ["- 데이터 없음"]); lines.append("")
    lines.append("*🔥 급상승*");       lines.extend(S.get("rising") or ["- 해당 없음"]); lines.append("")
    lines.append("*🆕 뉴랭커*");       lines.extend(S.get("newcomers") or ["- 해당 없음"]); lines.append("")
    lines.append("*📉 급하락*");       lines.extend(S.get("falling") or ["- 해당 없음"])
    lines.extend(S.get("outs") or [])
    lines.append(""); lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ----------------- 메인 -----------------
def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작: Amazon US Beauty & Personal Care")
    items = fetch_products()
    print("수집 완료:", len(items))
    if len(items) < 20:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. (차단/렌더링 점검 필요)")

    df_today = to_dataframe(items, date_str)
    os.makedirs("data", exist_ok=True)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print("로컬 저장:", file_today)

    # Google Drive 업로드 + 전일 CSV 로드
    df_prev = None
    folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID",""))
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today)
            print("Google Drive 업로드 완료:", file_today)
            df_prev = drive_download_csv(svc, folder, file_yesterday)
            print("전일 CSV", "성공" if df_prev is not None else "미발견")
        except Exception as e:
            print("Google Drive 처리 오류:", e)
            traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일 비교 생략")

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("Slack 전송 완료")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print("[오류 발생]", e); traceback.print_exc()
        try: slack_post(f"*Amazon US Beauty Top100 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
