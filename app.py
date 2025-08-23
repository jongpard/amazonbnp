# -*- coding: utf-8 -*-
"""
Amazon US - Beauty & Personal Care Best Sellers Top 100

- HTTP 우선 수집 → 부족하면 Playwright 폴백
- 2페이지 이슈 대응:
  * 스크롤/대기 강화
  * 카드 수 적으면 앵커(/dp/) 기반으로 보강(HTTP/JS 모두)
  * 1페이지 → Next 클릭, 실패 시 href 추출하여 직접 이동
- 수집 수가 100 미만이면 Playwright 한 번 더 재시도하여 병합
- Slack:
  * TOP10 전일 대비 배지 (↑/↓/(-)/(new))
  * 급상승/급하락: Top100 전체, 변동폭 10계단 이상, 최대 5개
  * OUT: 전일 1~70 → 오늘 OUT, 전일 순위 오름차순, 최대 5개
- 파일명: 아마존US_뷰티_랭킹_YYYY-MM-DD.csv (KST)
"""
import os, re, io, math, pytz, time, random, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ==================== 공통 유틸 ====================
KST = pytz.timezone("Asia/Seoul")
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"아마존US_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

PAGE_CANDIDATES = [
    [  # page 1
        "https://www.amazon.com/gp/bestsellers/beauty/ref=zg_bs_pg_1?ie=UTF8&pg=1",
        "https://www.amazon.com/gp/bestsellers/beauty/ref=zg_b_bs_beauty_1",
        "https://www.amazon.com/Best-Sellers-Beauty-Personal-Care/zgbs/beauty",
    ],
    [  # page 2
        "https://www.amazon.com/gp/bestsellers/beauty/ref=zg_bs_pg_2?ie=UTF8&pg=2",
        "https://www.amazon.com/Best-Sellers-Beauty-Personal-Care/zgbs/beauty/ref=zg_bs_pg_2_beauty?_encoding=UTF8&pg=2",
        "https://www.amazon.com/gp/bestsellers/beauty/?pg=2",
    ],
]

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

USD_RE = re.compile(r"(?:US\$|\$)\s*([\d]{1,3}(?:,\d{3})*(?:\.\d{2})|[\d]+(?:\.\d{2})?)")
ASIN_IN_HREF  = re.compile(r"/dp/([A-Z0-9]{10})")
ASIN_IN_QUERY = re.compile(r"[?&](?:pd_rd_i|asin|ASIN|m)=([A-Z0-9]{10})")
ASIN_PCT      = re.compile(r"(?:dp%2F|asin%2F)([A-Z0-9]{10})")
BY_BRAND_RE   = re.compile(r"\bby\s+([A-Za-z0-9&'’\-\.\s]{2,40})", re.I)
BRAND_LABEL_RE= re.compile(r"\bBrand\s*[:\-]\s*([A-Za-z0-9&'’\-\.\s]{2,40})", re.I)
VISIT_STORE_RE= re.compile(r"Visit the\s+(.+?)\s+Store", re.I)

def parse_usd_all(text: str) -> List[float]:
    out=[]
    for m in USD_RE.finditer(text or ""):
        try:
            v=float(m.group(1).replace(",",""))
            if v>0: out.append(v)
        except: pass
    return out

def fmt_currency_usd(v) -> str:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)): return "$0.00"
        return f"${float(v):,.2f}"
    except: return "$0.00"

def discount_floor(orig: Optional[float], sale: Optional[float]) -> Optional[int]:
    if orig and sale and orig>0: return max(0,int(math.floor((1 - sale/orig)*100)))
    return None

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

# ==================== 공통 보조 ====================
def canonical_amz_link(href: str, fallback_asin: str = "") -> str:
    if not href and fallback_asin:
        return f"https://www.amazon.com/dp/{fallback_asin}"
    if href and href.startswith("/"):
        href = urljoin("https://www.amazon.com", href)
    m = ASIN_IN_HREF.search(href or "")
    return f"https://www.amazon.com/dp/{m.group(1)}" if m else (href or (f"https://www.amazon.com/dp/{fallback_asin}" if fallback_asin else ""))

def extract_asin_from_node(node) -> str:
    for target in (node, getattr(node, "parent", None), getattr(getattr(node, "parent", None), "parent", None)):
        try:
            v = target.get("data-asin")
            if v: return v.strip()
        except: pass
    try:
        d = node.select_one("[data-asin]")
        if d:
            v = d.get("data-asin")
            if v: return v.strip()
    except: pass
    for a in node.select("a[href]"):
        h = a.get("href") or ""
        m = ASIN_IN_HREF.search(h) or ASIN_IN_QUERY.search(h) or ASIN_PCT.search(h)
        if m: return m.group(1)
    return ""

def extract_rank_from_node(node) -> Optional[int]:
    try:
        v = node.get("aria-posinset")
        if v and v.isdigit(): return int(v)
    except: pass
    try:
        b = node.select_one(".zg-badge-text, .a-badge-text")
        if b:
            m = re.search(r"#?\s*(\d{1,3})", b.get_text(" ", strip=True))
            if m: return int(m.group(1))
    except: pass
    try:
        v = node.get("data-index")
        if v and v.isdigit(): return int(v)+1
    except: pass
    return None

def extract_brand_from_container(c, title_text: str) -> str:
    for a in c.select("a[href*='/stores/']:not([href*='/dp/'])"):
        t = clean_text(a.get_text(" ", strip=True))
        if not t: continue
        m = VISIT_STORE_RE.search(t)
        if m: return clean_text(m.group(1))[:40]
        if t.lower() not in ("sponsored", "see more"):
            return t[:40]
    block = clean_text(c.get_text(" ", strip=True))
    m = BRAND_LABEL_RE.search(block)
    if m:
        cand = clean_text(m.group(1))
        if cand: return cand[:40]
    m = BY_BRAND_RE.search(block)
    if m:
        cand = clean_text(m.group(1))
        if cand: return cand[:40]
    title = clean_text(title_text or "")
    words = title.split()
    if not words: return ""
    guess = (words[0] + (" " + words[1] if len(words[0]) <= 3 and len(words) >= 2 else ""))
    return guess[:40]

# ==================== HTTP 파서 ====================
def parse_http(html: str, page_idx: int) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    selectors = [
        "ol[id*='zg-ordered-list'] > li",
        "[id*='gridItemRoot']",
        "div.p13n-sc-uncoverable-faceout",
        "div.zg-grid-general-faceout",
        "[data-asin]"
    ]
    def uniq(nodes):
        seen=set(); out=[]
        for n in nodes:
            if id(n) not in seen:
                seen.add(id(n)); out.append(n)
        return out

    candidates=[]
    for sel in selectors:
        candidates += soup.select(sel)
    candidates = uniq(candidates)

    # 카드가 부족하면 앵커(/dp/) 기반 보강 (최대 70개 될 때까지)
    if len(candidates) < 60:
        anchors = soup.select("a[href*='/dp/']")
        for a in anchors:
            blk = a.find_parent("li") or a.find_parent(attrs={"data-asin": True}) or a.find_parent("div") or a
            candidates.append(blk)
        candidates = uniq(candidates)

    by_rank: Dict[int, Product] = {}
    seen_asin=set()
    extras=[]

    for node in candidates:
        rank_in_page = extract_rank_from_node(node)
        asin = extract_asin_from_node(node)
        if not asin or asin in seen_asin: continue

        a = node.select_one("a[href*='/dp/']") or node.select_one("a.a-link-normal[href]")
        href = a.get("href") if a else ""
        link = canonical_amz_link(href or "", fallback_asin=asin)

        title = ""
        if a: title = (a.get("aria-label") or a.get("title") or clean_text(a.get_text(" ", strip=True)) or "")
        if not title:
            img = node.select_one("img[alt]")
            if img and img.has_attr("alt"): title = clean_text(img["alt"])
        if not title:
            t = node.select_one("span.a-size-medium, span.a-size-base, span.p13n-sc-truncated")
            if t: title = clean_text(t.get_text(" ", strip=True))
        if not title: 
            seen_asin.add(asin); 
            continue

        brand = extract_brand_from_container(node, title)

        block = clean_text(node.get_text(" ", strip=True))
        prices = parse_usd_all(block)
        sale = orig = None
        if len(prices)==1: sale=prices[0]
        elif len(prices)>=2:
            sale,orig=min(prices),max(prices)
            if sale==orig: orig=None

        p = Product(rank=None, brand=brand, title=title, price=sale,
                    orig_price=orig, discount_percent=discount_floor(orig, sale),
                    url=link, asin=asin)
        if rank_in_page and 1 <= rank_in_page <= 50 and rank_in_page not in by_rank:
            by_rank[rank_in_page] = p
        else:
            extras.append(p)

        seen_asin.add(asin)

    out=[]
    for r in range(1, 50+1):
        item = by_rank.get(r) or (extras.pop(0) if extras else None)
        if not item: continue
        item.rank = page_idx*50 + r
        out.append(item)
    return out

# ==================== HTTP 수집 ====================
def http_fetch_page(url: str, page_idx: int) -> List[Product]:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept-Language": "en-US,en;q=0.9,ko;q=0.6",
        "Cache-Control": "no-cache", "Pragma": "no-cache", "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    })
    last_err=None
    for attempt in range(3):
        try:
            r = s.get(url, timeout=25)
            if r.status_code==429: raise requests.HTTPError("429 Too Many Requests")
            r.raise_for_status()
            return parse_http(r.text, page_idx)
        except Exception as e:
            last_err=e; time.sleep(1.2*(attempt+1))
    if last_err: raise last_err
    return []

def fetch_by_http() -> List[Product]:
    all_items: List[Product] = []
    for page_idx, urls in enumerate(PAGE_CANDIDATES):
        got=[]
        for u in urls:
            try:
                got = http_fetch_page(u, page_idx)
                if len(got) >= 48: break
            except Exception:
                continue
        all_items.extend(got)
        time.sleep(random.uniform(0.6,1.2))
    return all_items

# ==================== Playwright 수집 ====================
def fetch_page_playwright(url: str, page_idx: int) -> List[Product]:
    """
    Playwright 수집 (페이지별 50개)
    - 기본: 후보 URL → 스크롤(강화) → JS 추출
    - page_idx==0 부족: reload 재시도
    - page_idx==1 부족: 1페이지 → Next 클릭 → 실패 시 href 직접 이동
    """
    from playwright.sync_api import sync_playwright

    js = """
    (pageIdx) => {
      function text(el){ return (el && (el.innerText||'').replace(/\\s+/g,' ').trim()) || ''; }
      const sels = [
        "ol[id*='zg-ordered-list'] > li",
        "[id*='gridItemRoot']",
        "div.p13n-sc-uncoverable-faceout",
        "div.zg-grid-general-faceout",
        "[data-asin]"
      ];
      function uniq(nodes){ const s=new Set(), out=[]; nodes.forEach(n=>{ if(!s.has(n)){s.add(n); out.push(n);} }); return out; }
      function canonical(href, asin){
        if(!href && asin) return 'https://www.amazon.com/dp/'+asin;
        if(href && href.startsWith('/')) href='https://www.amazon.com'+href;
        const m = href && href.match(/\\/dp\\/([A-Z0-9]{10})/);
        return m ? ('https://www.amazon.com/dp/'+m[1]) : (href || (asin? 'https://www.amazon.com/dp/'+asin : ''));
      }
      function extractASIN(node){
        const g=n=>n&&n.getAttribute&&n.getAttribute('data-asin');
        const d=g(node) || (node.querySelector&&g(node.querySelector('[data-asin]')));
        if(d) return d.trim();
        let p=node.parentElement;
        for(let i=0;i<2 && p;i++){ const v=g(p); if(v) return v.trim(); p=p.parentElement; }
        const links=node.querySelectorAll? node.querySelectorAll('a[href]'):[];
        for(const l of links){
          const h=l.getAttribute('href')||'';
          let m=h.match(/\\/dp\\/([A-Z0-9]{10})/)||h.match(/[?&](?:pd_rd_i|asin|ASIN|m)=([A-Z0-9]{10})/)||h.match(/(?:dp%2F|asin%2F)([A-Z0-9]{10})/);
          if(m) return m[1];
        }
        return '';
      }
      function extractRank(node){
        let v = node.getAttribute && node.getAttribute('aria-posinset');
        if(v && /^\\d+$/.test(v)) return parseInt(v,10);
        const b = node.querySelector('.zg-badge-text, .a-badge-text');
        if(b){ const m = text(b).match(/#?\\s*(\\d{1,3})/); if(m) return parseInt(m[1],10); }
        v = node.getAttribute && node.getAttribute('data-index');
        if(v && /^\\d+$/.test(v)) return parseInt(v,10)+1;
        return null;
      }
      const usdRe=/(?:US\\$|\\$)\\s*([\\d]{1,3}(?:,\\d{3})*(?:\\.\\d{2})|[\\d]+(?:\\.\\d{2})?)/g;
      const byBrandRe=/\\bby\\s+([A-Za-z0-9&'’\\-\\.\\s]{2,40})/i;
      const brandLabel=/\\bBrand\\s*[:\\-]\\s*([A-Za-z0-9&'’\\-\\.\\s]{2,40})/i;
      const visitStore=/Visit the\\s+(.+?)\\s+Store/i;

      let cards=[]; for(const s of sels){ cards = cards.concat(Array.from(document.querySelectorAll(s))); }
      if(cards.length < 60){
        const anchors = Array.from(document.querySelectorAll("a[href*='/dp/']"));
        for(const el of anchors){ cards.push(el.closest('li')||el.closest('[data-asin]')||el.closest('div')||el); }
      }
      cards = uniq(cards);

      const map = new Map(); // rank_in_page -> product
      const extras = [];
      const seen = new Set();

      for(const c of cards){
        const asin = extractASIN(c);
        if(!asin || seen.has(asin)) continue;
        const rank = extractRank(c);

        const a = c.querySelector("a[href*='/dp/']") || c.querySelector("a.a-link-normal[href]");
        let title = a ? (a.getAttribute('aria-label') || a.getAttribute('title') || text(a)) : '';
        if(!title){
          const img=c.querySelector('img[alt]');
          if(img) title=(img.getAttribute('alt')||'').replace(/\\s+/g,' ').trim();
        }
        if(!title){
          const t=c.querySelector('span.a-size-medium, span.a-size-base, span.p13n-sc-truncated');
          if(t) title=text(t);
        }
        if(!title) continue;

        // brand
        let brand='';
        const storeA = c.querySelector("a[href*='/stores/']:not([href*='/dp/'])");
        if(storeA){
          const bt=text(storeA);
          const m=bt.match(visitStore);
          brand = m? m[1].trim() : (!/^(sponsored|see more)$/i.test(bt) ? bt.trim() : '');
        }
        if(!brand){
          const blk=text(c);
          let m=blk.match(brandLabel);
          if(m) brand=(m[1]||'').trim();
          else { m=blk.match(byBrandRe); if(m) brand=(m[1]||'').trim(); }
        }
        if(!brand){
          const ws=title.split(' ');
          if(ws.length){
            brand=(ws[0].length<=3 && ws[1]) ? (ws[0]+' '+ws[1]) : ws[0];
          }
        }

        const blk=text(c);
        const prices = Array.from(blk.matchAll(usdRe))
          .map(m => parseFloat(m[1].replace(/,/g,'')))
          .filter(v => !isNaN(v) && v > 0);
        let sale=null, orig=null;
        if(prices.length===1) sale=prices[0];
        else if(prices.length>=2){
          sale=Math.min(...prices); orig=Math.max(...prices);
          if(sale===orig) orig=null;
        }

        const row = {rank:null, brand, title, price:sale, orig_price:orig, url: canonical(a ? a.getAttribute('href') : '', asin), asin};

        if(rank && rank>=1 && rank<=50 && !map.has(rank)) map.set(rank, row); else extras.push(row);
        seen.add(asin);
      }

      const out=[];
      for(let r=1;r<=50;r++){
        let row = map.get(r) || extras.shift();
        if(!row) continue;
        row.rank = pageIdx*50 + r;
        out.push(row);
      }
      return out;
    }
    """

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

        # 1) 후보 URL로 진입
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=30_000)
        except: pass

        # 쿠키 배너 닫기
        for sel in ["#sp-cc-accept","button[name='accept']","input#sp-cc-accept","button:has-text('Accept')"]:
            try: page.locator(sel).first.click(timeout=1200)
            except: pass

        # 스크롤 (강화)
        for _ in range(32):
            try: page.mouse.wheel(0, 1600)
            except: pass
            page.wait_for_timeout(650)

        # 1차 평가
        try:
            data = page.evaluate(js, page_idx)
        except Exception as e:
            print("[Playwright] evaluate 1st try failed:", e)
            data = []

        # page1 부족 → reload 재시도
        if page_idx == 0 and (not isinstance(data, list) or len(data) < 45):
            try:
                page.reload(wait_until="domcontentloaded", timeout=60_000)
                try: page.wait_for_load_state("networkidle", timeout=20_000)
                except: pass
                for _ in range(22):
                    try: page.mouse.wheel(0, 1600)
                    except: pass
                    page.wait_for_timeout(500)
                data = page.evaluate(js, page_idx)
            except Exception as e:
                print("[Playwright] page1 reload fallback failed:", e)

        # page2 부족 → 1페이지→Next 클릭 / href 이동
        if page_idx == 1 and (not isinstance(data, list) or len(data) < 45):
            try:
                print("[Playwright] page2 부족 → Next-click / href-goto fallback")
                page.goto(PAGE_CANDIDATES[0][0], wait_until="domcontentloaded", timeout=60_000)
                try: page.wait_for_load_state("networkidle", timeout=20_000)
                except: pass
                for sel in ["#sp-cc-accept","button[name='accept']","input#sp-cc-accept","button:has-text('Accept')"]:
                    try: page.locator(sel).first.click(timeout=1200)
                    except: pass
                for _ in range(8):
                    try: page.mouse.wheel(0, 1400)
                    except: pass
                    page.wait_for_timeout(250)

                clicked=False
                for sel in [
                    "a[href*='?pg=2']","a[href*='pg=2']",
                    "a[aria-label*='Next']","a[aria-label='Next page']",
                    "a[aria-label='Go to next page']","li.a-last a","a.s-pagination-next"
                ]:
                    try:
                        page.locator(sel).first.click(timeout=4000, force=True)
                        clicked=True; break
                    except: pass

                if not clicked:
                    try:
                        page2_href = page.evaluate("""
                        () => {
                          const toAbs = (h) => h && (h.startsWith('/') ? 'https://www.amazon.com'+h : h);
                          const as = Array.from(document.querySelectorAll('a[href]'));
                          let a = as.find(x => /[?&]pg=2/.test(x.getAttribute('href')||'') &&
                                               /(zgbs\\/beauty|bestsellers\\/beauty|\\/gp\\/bestsellers\\/beauty)/i.test(x.getAttribute('href')||''));
                          if(!a) a = as.find(x => /Next/i.test((x.textContent||'').trim()) && x.getAttribute('href'));
                          return a ? toAbs(a.getAttribute('href')) : null;
                        }
                        """)
                        if page2_href:
                            page.goto(page2_href, wait_until="domcontentloaded", timeout=60_000)
                            try: page.wait_for_load_state("networkidle", timeout=20_000)
                            except: pass
                            clicked=True
                    except Exception as e:
                        print("[Playwright] href-goto fallback failed:", e)

                if clicked:
                    for _ in range(28):
                        try: page.mouse.wheel(0, 1600)
                        except: pass
                        page.wait_for_timeout(500)
                    try:
                        data = page.evaluate(js, page_idx)
                        if len(data) < 45:
                            page.wait_for_timeout(1500)
                            for _ in range(6):
                                try: page.mouse.wheel(0, 1800)
                                except: pass
                                page.wait_for_timeout(400)
                            data = page.evaluate(js, page_idx)
                    except Exception as e:
                        print("[Playwright] evaluate after goto/next failed:", e)
                        data = []
                else:
                    print("[Playwright] Next 클릭/이동 폴백 모두 실패")
            except Exception as e:
                print("[Playwright] page2 fallback block failed:", e)

        ctx.close(); browser.close()

    out=[]
    for r in (data or []):
        out.append(Product(
            rank=int(r["rank"]),
            brand=clean_text(r.get("brand","")),
            title=clean_text(r["title"]),
            price=r["price"],
            orig_price=r["orig_price"],
            discount_percent=discount_floor(r["orig_price"], r["price"]),
            url=r["url"],
            asin=r["asin"],
        ))
    return out

def fetch_by_playwright() -> List[Product]:
    all_items=[]
    for page_idx, urls in enumerate(PAGE_CANDIDATES):
        got=[]
        for u in urls:
            got = fetch_page_playwright(u, page_idx)
            if len(got) >= 48: break
        all_items.extend(got)
        time.sleep(0.7)
    return all_items

# ==================== 통합 수집/병합 ====================
def merge_by_rank(*lists: List[Product]) -> List[Product]:
    by_rank: Dict[int, Product] = {}
    for L in lists:
        for p in (L or []):
            if p.rank: by_rank[p.rank] = p
    out=[]
    for r in range(1, 101):
        if r in by_rank:
            p = by_rank[r]; p.rank = r; out.append(p)
    return out

def fetch_products() -> List[Product]:
    # 1) HTTP
    try:
        items_http = fetch_by_http()
        items = items_http if len(items_http) >= 96 else []
        if not items: raise RuntimeError("HTTP 수집 부족")
    except Exception as e:
        print("[HTTP 오류] → Playwright 폴백:", e); items=[]

    # 2) Playwright 보강
    if len(items) < 96:
        items_pw = fetch_by_playwright()
        items = merge_by_rank(items, items_pw)

    # 3) 여전히 100 미만이면 한 번 더 Playwright
    if len(items) < 100:
        print("[재시도] Playwright 한 번 더 실행")
        time.sleep(1.0)
        items_pw2 = fetch_by_playwright()
        items = merge_by_rank(items, items_pw2)

    return merge_by_rank(items)

# ==================== Google Drive ====================
def normalize_folder_id(raw: str) -> str:
    if not raw: return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    cid=os.getenv("GOOGLE_CLIENT_ID"); csec=os.getenv("GOOGLE_CLIENT_SECRET"); rtk=os.getenv("GOOGLE_REFRESH_TOKEN")
    if not (cid and csec and rtk): raise RuntimeError("OAuth 자격정보가 없습니다. GOOGLE_* 확인")
    creds = Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                        client_id=cid, client_secret=csec)
    svc = build("drive","v3",credentials=creds, cache_discovery=False)
    try:
        about=svc.about().get(fields="user(displayName,emailAddress)").execute()
        u=about.get("user",{}); print(f"[Drive] user={u.get('displayName')} <{u.get('emailAddress')}>")
    except Exception as e:
        print("[Drive] whoami 실패:", e)
    return svc

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    q=f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res=service.files().list(q=q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    file_id=res.get("files",[{}])[0].get("id") if res.get("files") else None
    buf=io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
    media=MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute(); return file_id
    meta={"name":name,"parents":[folder_id],"mimeType":"text/csv"}
    created=service.files().create(body=meta, media_body=media, fields="id", supportsAllDrives=True).execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload
    res=service.files().list(q=f"name='{name}' and '{folder_id}' in parents and trashed=false",
                             fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files=res.get("files",[])
    if not files: return None
    fid=files[0]["id"]; req=service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh=io.BytesIO(); dl=MediaIoBaseDownload(fh, req); done=False
    while not done: _,done=dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ==================== Slack ====================
def slack_post(text: str):
    import requests as _r
    url=os.getenv("SLACK_WEBHOOK_URL")
    if not url: print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text); return
    r=_r.post(url, json={"text":text}, timeout=20)
    if r.status_code>=300: print("[Slack 실패]", r.status_code, r.text)

def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    cols=["date","rank","brand","product_name","price","orig_price","discount_percent","url","asin"]
    if not products: return pd.DataFrame(columns=cols)
    return pd.DataFrame([{
        "date": date_str, "rank": p.rank, "brand": p.brand, "product_name": p.title,
        "price": p.price, "orig_price": p.orig_price, "discount_percent": p.discount_percent,
        "url": p.url, "asin": p.asin,
    } for p in products], columns=cols)

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    """
    - TOP10: 전일 대비 (↑n)/(↓n)/(-)/(new)
    - 급상승/급하락: Top100 전체, 변동폭 10계단 이상, 각 5개
    - OUT: 전일 1~70 → OUT, 전일 순위 오름차순 5개
    - 뉴랭커: 30위 이내 진입 3개
    """
    S = {"top10": [], "rising": [], "newcomers": [], "falling": [], "outs": [], "inout_count": 0}
    if df_today is None or "rank" not in df_today.columns or df_today.empty: return S

    # 전일 rank 맵
    prev_rank_map={}
    if df_prev is not None and "rank" in df_prev.columns and len(df_prev):
        df_p = df_prev.copy()
        df_p["key"] = df_p.apply(lambda x: (str(x.get("asin")).strip() or str(x.get("url")).strip()), axis=1)
        for _, row in df_p[["key","rank"]].dropna().iterrows():
            try: prev_rank_map[str(row["key"])] = int(row["rank"])
            except: pass

    # TOP10
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        cur_rank = int(r["rank"])
        key = (str(r.get("asin")).strip() or str(r.get("url")).strip())
        prev_rank = prev_rank_map.get(key)
        if prev_rank is None: badge="(new)"
        elif prev_rank > cur_rank: badge=f"(↑{prev_rank-cur_rank})"
        elif prev_rank < cur_rank: badge=f"(↓{cur_rank-prev_rank})"
        else: badge="(-)"
        name = clean_text(r["product_name"]); br = clean_text(r.get("brand",""))
        name_show = f"{br} {name}" if br and not name.lower().startswith(br.lower()) else name
        price_txt = fmt_currency_usd(r["price"])
        dc = r.get("discount_percent"); dc_tail = f" (↓{int(dc)}%)" if pd.notnull(dc) else ""
        S["top10"].append(f"{cur_rank}. {badge} <{r['url']}|{slack_escape(name_show)}> — {price_txt}{dc_tail}")

    if df_prev is None or not len(df_prev) or "rank" not in df_prev.columns: return S

    df_t = df_today.copy()
    df_t = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 100)].copy()
    df_t["key"] = df_t.apply(lambda x: (str(x.get("asin")).strip() or str(x.get("url")).strip()), axis=1)
    df_t.set_index("key", inplace=True)

    df_p = df_prev.copy()
    df_p = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 100)].copy()
    df_p["key"] = df_p.apply(lambda x: (str(x.get("asin")).strip() or str(x.get("url")).strip()), axis=1)
    df_p.set_index("key", inplace=True)

    common_all = set(df_t.index) & set(df_p.index)
    new_all    = set(df_t.index) - set(df_p.index)
    out_all    = set(df_p.index) - set(df_t.index)

    # 급상승
    rising=[]
    for k in common_all:
        pr, cr = int(df_p.loc[k,"rank"]), int(df_t.loc[k,"rank"])
        imp = pr - cr
        if imp >= 10:
            nm = slack_escape(clean_text(df_t.loc[k]["product_name"]))
            rising.append((imp, cr, pr, nm, f"- <{df_t.loc[k]['url']}|{nm}> {pr}위 → {cr}위 (↑{imp})"))
    rising.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["rising"] = [x[-1] for x in rising[:5]]

    # 급하락
    falling=[]
    for k in common_all:
        pr, cr = int(df_p.loc[k,"rank"]), int(df_t.loc[k,"rank"])
        drop = cr - pr
        if drop >= 10:
            nm = slack_escape(clean_text(df_t.loc[k]["product_name"]))
            falling.append((drop, cr, pr, nm, f"- <{df_t.loc[k]['url']}|{nm}> {pr}위 → {cr}위 (↓{drop})"))
    falling.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["falling"] = [x[-1] for x in falling[:5]]

    # 뉴랭커 (30위 이내)
    t30 = df_t[df_t["rank"] <= 30].copy()
    p30 = df_p[df_p["rank"] <= 30].copy()
    newcomers=[]
    for k in (set(t30.index) - set(p30.index)):
        cr = int(t30.loc[k,"rank"])
        nm = slack_escape(clean_text(t30.loc[k]["product_name"]))
        newcomers.append((cr, f"- <{t30.loc[k]['url']}|{nm}> NEW → {cr}위"))
    newcomers.sort(key=lambda x: x[0])
    S["newcomers"] = [x[1] for x in newcomers[:3]]

    # OUT (전일 1~70 → OUT)
    outs = []
    for k in out_all:
        pr = int(df_prev.loc[k, "rank"])
        if pr <= 70:
            nm = slack_escape(clean_text(df_p_loc[k]["product_name"]))
            outs.append((pr, f"<{df_p_loc[k]['url']}|{nm}> {pr}위 → OUT"))
    outs.sort(key=lambda x: x[0])
    S["outs"] = [x[1] for x in outs[:5]]

    # 인&아웃 수 = Top100 교체 수(대칭차집합/2)
    if "asin" in df_today.columns and "asin" in df_prev.columns:
        today_keys = set(
            df_today.sort_values("rank").head(100)["asin"].astype(str).str.strip()
        )
        prev_keys = set(
            df_prev[df_prev["rank"].between(1, 100)]["asin"].astype(str).str.strip()
        )
    else:
        # asin이 없으면 URL로 폴백
        today_keys = set(
            df_today.sort_values("rank").head(100)["url"].astype(str).str.strip()
        )
        prev_keys = set(
            df_prev[df_prev["rank"].between(1, 100)]["url"].astype(str).str.strip()
        )

    S["inout_count"] = len(today_keys ^ prev_keys) // 2
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]], total_count: int) -> str:
    header = f"*Amazon US Beauty & Personal Care Top 100 — {date_str}*"
    if total_count < 100: header += f"  _(수집 {total_count}/100)_"
    lines=[header,"","*TOP 10*"]
    lines.extend(S.get("top10") or ["- 데이터 없음"]); lines.append("")
    lines.append("*🔥 급상승*"); lines.extend(S.get("rising") or ["- 해당 없음"]); lines.append("")
    lines.append("*🆕 뉴랭커*"); lines.extend(S.get("newcomers") or ["- 해당 없음"]); lines.append("")
    lines.append("*📉 급하락*"); lines.extend(S.get("falling") or ["- 해당 없음"])
    lines.extend(S.get("outs") or [])
    lines.append(""); lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ==================== 메인 ====================
def main():
    date_str=today_kst_str()
    file_today=build_filename(date_str)
    file_yest=build_filename(yesterday_kst_str())

    print("수집 시작: Amazon US Beauty & Personal Care")
    items=fetch_products()
    print("수집 완료:", len(items))

    df_today=to_dataframe(items, date_str)
    os.makedirs("data", exist_ok=True)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print("로컬 저장:", file_today)

    folder=normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID","")); df_prev=None
    if folder:
        try:
            svc=build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today); print("Google Drive 업로드 완료:", file_today)
            df_prev=drive_download_csv(svc, folder, file_yest); print("전일 CSV", "성공" if df_prev is not None else "미발견")
        except Exception as e:
            print("Google Drive 처리 오류:", e); traceback.print_exc()

    S=build_sections(df_today, df_prev)
    slack_post(build_slack_message(date_str, S, total_count=len(items)))
    print("Slack 전송 완료")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("[오류 발생]", e); traceback.print_exc()
        try: slack_post(f"*Amazon US Beauty Top100 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
