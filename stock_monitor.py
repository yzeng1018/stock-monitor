"""
股票异动监控脚本 v3
支持美股、港股、A股

运行模式：
  intraday  - 盘中实时监控（每15分钟）：当日实时价 vs 昨日收盘 > ±5%，附近期新闻标题
  close_a   - A股收盘后：条件2（30天新高/低）+ 条件3（量比）+ 条件4（MA20穿越）
  close_hk  - 港股收盘后：同上
  close_us  - 美股收盘后：同上
  daily_a   - A股日报（收盘后1小时）：大盘指数 + 个股股价/涨跌/量比 + Qwen新闻摘要
  daily_hk  - 港股日报：同上
  daily_us  - 美股日报：同上
  weekly_a  - A股周报（每周五）：本周涨跌幅排名 Top5
  weekly_hk - 港股周报：同上
  weekly_us - 美股周报：同上
"""

import os
import sys
import json
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import yfinance as yf

# 在 akshare 创建 Session 之前注入浏览器 UA，避免东方财富 API 拒绝连接
_orig_session_init = requests.Session.__init__
def _patched_session_init(self, *args, **kwargs):
    _orig_session_init(self, *args, **kwargs)
    self.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })
requests.Session.__init__ = _patched_session_init

import akshare as ak
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# ============================================================
# 配置区域
# ============================================================

PUSHPLUS_TOKEN    = os.environ.get("PUSHPLUS_TOKEN", "")
DASHSCOPE_API_KEY = os.environ.get("DASHSCOPE_API_KEY", "")
SMTP_HOST     = "smtp.gmail.com"
SMTP_PORT     = 587
SMTP_USER     = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")

PRICE_CHANGE_THRESHOLD = 5.0  # 盘中涨跌幅阈值（%）
VOLUME_MULTIPLIER      = 1.8  # 收盘后成交量倍数阈值

_SCRIPT_DIR        = os.path.dirname(os.path.abspath(__file__))
ALERTED_TODAY_FILE = os.path.join(_SCRIPT_DIR, "alerted_today.json")
STOCK_NAMES_FILE   = os.path.join(_SCRIPT_DIR, "stock_names.json")


def load_alerted_today():
    """读取当日已推送异动的股票代码集合，跨 Actions 运行去重用"""
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        with open(ALERTED_TODAY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") == today:
            return set(data.get("symbols", []))
    except Exception:
        pass
    return set()


def save_alerted_today(new_symbols):
    """将本次新推送的股票代码追加保存到当日去重文件"""
    today = datetime.now().strftime("%Y-%m-%d")
    existing = load_alerted_today()
    existing.update(new_symbols)
    try:
        with open(ALERTED_TODAY_FILE, "w", encoding="utf-8") as f:
            json.dump({"date": today, "symbols": list(existing)}, f, ensure_ascii=False)
    except Exception as e:
        print(f"  WARNING 保存 alerted_today.json 失败: {e}")


US_STOCKS = [
    "GOOG", "PDD", "NIO", "TSM", "AMZN", "CRCL", "SBUX", "BKNG",
    "META", "ABNB", "DUOL", "AAPL", "UBER", "FUTU", "XNET", "NVDA",
    "DIDIY", "FIG", "BEKE", "EDU", "LKNCY", "TAL", "SE",
    "DASH", "TSLA", "MELI", "LI", "GOTU", "XPEV", "BIDU",
    "SY", "TCOM", "PONY", "BILI", "LU", "APP", "SOFI", "OWL"
]

HK_STOCKS = [
    "02513.HK", "00100.HK", "02252.HK", "08083.HK",
    "06030.HK", "00853.HK", "02333.HK", "02013.HK",
    "03750.HK", "03690.HK", "09618.HK", "00700.HK",
    "01211.HK", "09868.HK", "09992.HK", "01024.HK", "01810.HK",
    "00981.HK", "02643.HK", "09988.HK", "09626.HK"
]

A_STOCKS = [
    "688207", "688256", "688981", "600519", "688277", "603019",
    "600030", "002594", "002230", "601318",
    "300750", "000737", "300418"
]

# ============================================================
# 推送（PushPlus）——汇总模式，一次发一条
# ============================================================

def send_to_wechat(title, content):
    if not PUSHPLUS_TOKEN:
        print("WARNING 未配置 PUSHPLUS_TOKEN，打印到控制台")
        print(f"\n{'='*50}\n{title}\n{content}\n{'='*50}")
        return
    try:
        resp = requests.post(
            "https://www.pushplus.plus/send",
            json={"token": PUSHPLUS_TOKEN, "title": title,
                  "content": content, "template": "markdown"},
            timeout=10
        )
        data = resp.json()
        if data.get("code") == 200:
            print(f"  OK 推送成功：{title}")
        else:
            print(f"  FAIL 推送失败：{data.get('msg')} | {title}")
    except Exception as e:
        print(f"  FAIL 推送异常：{e}")


def send_email(to_addr, subject, content_md):
    """将 Markdown 内容转为 HTML 发送邮件"""
    if not all([SMTP_USER, SMTP_PASSWORD]):
        print(f"WARNING 未配置SMTP，跳过邮件: {subject}")
        return
    html = content_md
    html = html.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
    html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
    html = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', html)
    html = html.replace('\n---\n', '<hr>')
    html = html.replace('\n', '<br>')
    html = f'<html><body style="font-family:sans-serif;max-width:640px;margin:0 auto;line-height:1.6">{html}</body></html>'

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SMTP_USER
        msg["To"]      = to_addr
        msg.attach(MIMEText(content_md, "plain", "utf-8"))
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_addr, msg.as_string())
        print(f"  OK 邮件发送成功：{to_addr} | {subject}")
    except Exception as e:
        print(f"  FAIL 邮件发送失败：{e}")


_USERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.json")

def load_users():
    """读取 users.json，返回用户列表"""
    try:
        with open(_USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"WARNING 读取 users.json 失败: {e}")
        return []


# ============================================================
# 股票名称缓存（含自动回写）
# ============================================================

_NAME_CACHE = None


def _ensure_name_cache():
    global _NAME_CACHE
    if _NAME_CACHE is None:
        try:
            with open(STOCK_NAMES_FILE, "r", encoding="utf-8") as f:
                _NAME_CACHE = json.load(f)
        except Exception:
            _NAME_CACHE = {}
    return _NAME_CACHE


def _flush_name_cache():
    """将内存中的名称缓存写回 stock_names.json"""
    cache = _ensure_name_cache()
    try:
        with open(STOCK_NAMES_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  WARNING 写回 stock_names.json 失败: {e}")


def get_stock_name(symbol, market):
    """取股票中文名：① stock_names.json ② 新浪财经 API ③ 降级返回代码。
    美股直接返回 ticker，无需中文名。
    成功从新浪取到名称后自动更新本地缓存。
    """
    if market == "美股":
        return symbol

    cache = _ensure_name_cache()
    if symbol in cache:
        return cache[symbol]

    try:
        if market == "港股":
            code_4d = f"{int(symbol.replace('.HK', '')):04d}"
            url = f"https://hq.sinajs.cn/list=hk{code_4d}"
        else:
            prefix = "sh" if symbol.startswith("6") else "sz"
            url = f"https://hq.sinajs.cn/list={prefix}{symbol}"
        resp = requests.get(url, headers={"Referer": "https://finance.sina.com.cn"}, timeout=5)
        m = re.search(r'"([^"]+)"', resp.text)
        if m:
            name = m.group(1).split(",")[0].strip()
            if name and re.search(r'[\u4e00-\u9fff]', name):
                cache[symbol] = name
                _flush_name_cache()
                return name
    except Exception:
        pass

    return symbol


# ============================================================
# 大盘指数
# ============================================================

_INDEX_MAP = {
    "a":  [("000001.SS", "上证"), ("399001.SZ", "深成"), ("399006.SZ", "创业板")],
    "hk": [("^HSI", "恒生"), ("^HSTECH", "恒生科技")],
    "us": [("SPY", "SPY"), ("QQQ", "QQQ"), ("^DJI", "道指")],
}


def get_market_indices(market):
    """获取大盘指数当日涨跌幅，返回 [(name, price, change_pct), ...]"""
    results = []
    for ticker_sym, name in _INDEX_MAP.get(market, []):
        try:
            fi = yf.Ticker(ticker_sym).fast_info
            price      = fi.last_price
            prev_close = fi.previous_close
            if price and prev_close and prev_close != 0:
                chg = (price - prev_close) / prev_close * 100
                results.append((name, round(float(price), 2), round(float(chg), 2)))
        except Exception as e:
            print(f"  WARNING 指数 {ticker_sym} 获取失败: {e}")
    return results


def _format_indices(indices):
    """将指数列表格式化为一行文字"""
    if not indices:
        return "（指数数据暂不可用）"
    parts = []
    for name, price, chg in indices:
        arrow = "+" if chg >= 0 else ""
        parts.append(f"{name} {price} ({arrow}{chg:.2f}%)")
    return "  |  ".join(parts)


# ============================================================
# 模式一：盘中实时监控（条件1）
# ============================================================

def get_intraday_news(symbol, market):
    """盘中异动时快速获取 1-2 条最新新闻标题（纯标题，不调用 Qwen）"""
    headlines = []
    try:
        if market in ["美股", "港股"]:
            yf_sym = (f"{int(symbol.replace('.HK', '')):04d}.HK"
                      if market == "港股" else symbol)
            for n in yf.Ticker(yf_sym).news[:2]:
                if "content" in n and "title" in n["content"]:
                    headlines.append(n["content"]["title"])
        elif market == "A股":
            news_df = ak.stock_news_em(symbol=symbol)
            if news_df is not None and not news_df.empty:
                for _, row in news_df.head(2).iterrows():
                    t = row.get("新闻标题", "")
                    if t:
                        headlines.append(t)
    except Exception:
        pass
    return headlines


def get_intraday_us(symbols):
    """并发拉取美股实时价 vs 昨日收盘"""
    def _fetch(symbol):
        try:
            fi = yf.Ticker(symbol).fast_info
            current    = fi.last_price
            prev_close = fi.previous_close
            if not current or not prev_close or prev_close == 0:
                return None
            change_pct = (current - prev_close) / prev_close * 100
            vol     = getattr(fi, "last_volume", None)
            avg_vol = getattr(fi, "three_month_average_volume", None)
            vol_ratio = round(vol / avg_vol, 2) if vol and avg_vol else None
            return {
                "symbol":     symbol,
                "name":       symbol,
                "price":      round(float(current), 3),
                "prev_close": round(float(prev_close), 3),
                "change_pct": round(float(change_pct), 2),
                "vol_ratio":  vol_ratio,
                "market":     "美股",
            }
        except Exception as e:
            print(f"  WARNING  {symbol} 实时数据获取失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch, s): s for s in symbols}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_intraday_hk():
    """用 yfinance 并发拉取港股实时价。
    东方财富 API 屏蔽 GitHub Actions IP，改用 Yahoo Finance（全球可访问）。
    """
    def _fetch(original):
        yf_sym = f"{int(original.replace('.HK', '')):04d}.HK"
        try:
            fi = yf.Ticker(yf_sym).fast_info
            current    = fi.last_price
            prev_close = fi.previous_close
            if not current or not prev_close or prev_close == 0:
                return None
            change_pct = (current - prev_close) / prev_close * 100
            vol     = getattr(fi, "last_volume", None)
            avg_vol = getattr(fi, "three_month_average_volume", None)
            vol_ratio = round(vol / avg_vol, 2) if vol and avg_vol else None
            return {
                "symbol":     original,
                "name":       yf_sym,
                "price":      round(float(current), 3),
                "prev_close": round(float(prev_close), 3),
                "change_pct": round(float(change_pct), 2),
                "vol_ratio":  vol_ratio,
                "market":     "港股",
            }
        except Exception as e:
            print(f"  WARNING  {yf_sym} 实时数据获取失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, s): s for s in HK_STOCKS}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_intraday_a():
    """用 yfinance 并发拉取A股实时价。
    东方财富 API 屏蔽 GitHub Actions IP，改用 Yahoo Finance（全球可访问）。
    """
    def _fetch(original):
        yf_sym = f"{original}.SS" if original.startswith("6") else f"{original}.SZ"
        try:
            fi = yf.Ticker(yf_sym).fast_info
            current    = fi.last_price
            prev_close = fi.previous_close
            if not current or not prev_close or prev_close == 0:
                return None
            change_pct = (current - prev_close) / prev_close * 100
            vol     = getattr(fi, "last_volume", None)
            avg_vol = getattr(fi, "three_month_average_volume", None)
            vol_ratio = round(vol / avg_vol, 2) if vol and avg_vol else None
            return {
                "symbol":     original,
                "name":       yf_sym,
                "price":      round(float(current), 3),
                "prev_close": round(float(prev_close), 3),
                "change_pct": round(float(change_pct), 2),
                "vol_ratio":  vol_ratio,
                "market":     "A股",
            }
        except Exception as e:
            print(f"  WARNING  {yf_sym} 实时数据获取失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, s): s for s in A_STOCKS}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def _is_us_regular_session():
    """通过 Yahoo Finance chart API 查询 SPY 的 marketState，确认美股是否处于正式交易时段。
    自动处理夏令时(EDT)、冬令时(EST)和节假日，只有 REGULAR 才返回 True。
    """
    now = datetime.utcnow()
    utc_min = now.hour * 60 + now.minute
    # UTC 时间快速预判：明显不在美股窗口内（UTC 12:00-22:00）时直接返回 False，省去 API 调用
    if not (720 <= utc_min < 1320):
        return False
    try:
        resp = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/SPY",
            params={"interval": "1d", "range": "1d"},
            timeout=5,
        )
        meta = resp.json()["chart"]["result"][0]["meta"]
        return meta.get("marketState", "CLOSED") == "REGULAR"
    except Exception:
        return 870 <= utc_min < 1260


def run_intraday(market=None):
    """盘中监控。触发异动时附带 1-2 条近期新闻标题。
    market='a'|'hk'|'us'  → 仅监控指定市场
    market=None            → 自动判断所有当前开盘市场
    """
    now_utc = datetime.utcnow()
    utc_min = now_utc.hour * 60 + now_utc.minute

    open_status = {
        "a":  90  <= utc_min < 420,
        "hk": 90  <= utc_min < 480,
        "us": _is_us_regular_session(),
    }
    name_map  = {"a": "A股", "hk": "港股", "us": "美股"}
    fetch_map = {
        "a":  get_intraday_a,
        "hk": get_intraday_hk,
        "us": lambda: get_intraday_us(US_STOCKS),
    }

    targets = [market] if market else [m for m, o in open_status.items() if o]

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 盘中监控 "
          f"(UTC {now_utc.strftime('%H:%M')}) "
          f"A股:{'开' if open_status['a'] else '休'} "
          f"港股:{'开' if open_status['hk'] else '休'} "
          f"美股:{'开' if open_status['us'] else '休'}")

    if not targets:
        print("当前无开盘市场，跳过监控")
        return

    alerted_today = load_alerted_today()
    for mkt in targets:
        mkt_name = name_map[mkt]
        if not open_status.get(mkt, False):
            print(f"{mkt_name}当前休市，跳过")
            continue

        print(f"获取{mkt_name}实时数据...")
        stocks = fetch_map[mkt]()
        print(f"成功获取 {len(stocks)} 支{mkt_name}实时数据")

        triggered = sorted(
            [s for s in stocks
             if abs(s["change_pct"]) >= PRICE_CHANGE_THRESHOLD
             and s["symbol"] not in alerted_today],
            key=lambda x: -abs(x["change_pct"])
        )
        if not triggered:
            print(f"{mkt_name}无盘中异动触发（或均已在今日推送过）")
            continue

        alert_lines = []
        for stock in triggered:
            name    = get_stock_name(stock["symbol"], stock["market"])
            arrow   = "up" if stock["change_pct"] > 0 else "down"
            vr      = stock.get("vol_ratio")
            vol_str = f"{vr:.2f}x" if vr is not None else "-"
            line = (
                f"| [{arrow}] {name}({stock['symbol']})"
                f" | {stock['prev_close']}"
                f" | {stock['price']}"
                f" | **{stock['change_pct']:+.2f}%**"
                f" | {vol_str} |"
            )
            headlines = get_intraday_news(stock["symbol"], stock["market"])
            if headlines:
                line += "\n" + "\n".join(f"  - {h}" for h in headlines)
            alert_lines.append(line)

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        content = "\n".join([
            f"## {mkt_name}盘中异动汇总（{now_str}）",
            f"共 **{len(alert_lines)}** 支股票涨跌幅超过 ±{PRICE_CHANGE_THRESHOLD}%",
            "",
            "| 股票 | 昨收 | 现价 | 涨跌幅 | 量比 |",
            "|------|------|------|--------|------|",
        ] + alert_lines)

        send_to_wechat(
            f"{mkt_name}盘中异动 {len(alert_lines)} 支（{now_str}）",
            content
        )
        save_alerted_today([s["symbol"] for s in triggered])
        alerted_today.update(s["symbol"] for s in triggered)
        print(f"{mkt_name}共 {len(alert_lines)} 条异动，已汇总推送")


# ============================================================
# 模式二/三：收盘后检测（条件2 + 条件3 + 条件4）
# ============================================================

def get_close_data_us(symbols):
    """并发获取美股收盘价 + 历史数据（用于条件2/3/4），返回 (results, failed)"""
    def _fetch(symbol):
        try:
            hist = yf.Ticker(symbol).history(period="60d")
            if hist.empty or len(hist) < 22:
                return None
            current_price = float(hist["Close"].iloc[-1])
            prev_close    = float(hist["Close"].iloc[-2])
            current_vol   = float(hist["Volume"].iloc[-1])
            hist_30       = hist.iloc[-31:-1]
            avg_vol_30    = float(hist_30["Volume"].mean())
            max_price_30  = float(hist_30["Close"].max())
            min_price_30  = float(hist_30["Close"].min())
            vol_ratio     = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0
            ma20          = float(hist["Close"].iloc[-20:].mean())
            prev_ma20     = float(hist["Close"].iloc[-21:-1].mean())
            return {
                "symbol":     symbol,
                "name":       symbol,
                "price":      round(current_price, 3),
                "prev_close": round(prev_close, 3),
                "volume":     int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio":  round(vol_ratio, 2),
                "max_30d":    round(max_price_30, 3),
                "min_30d":    round(min_price_30, 3),
                "ma20":       round(ma20, 3),
                "prev_ma20":  round(prev_ma20, 3),
                "market":     "美股",
            }
        except Exception as e:
            print(f"  WARNING  {symbol} 收盘数据获取失败: {e}")
            return None

    results, failed = [], []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch, s): s for s in symbols}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
            else:
                failed.append(futures[future])
    return results, failed


def get_close_data_hk():
    """获取港股收盘价 + 历史数据（yfinance），返回 (results, failed)"""
    def _fetch(sym):
        code_4d = _hk_to_yf(sym)
        try:
            hist = yf.Ticker(code_4d).history(period="60d")
            if hist.empty or len(hist) < 22:
                return None
            current_price = float(hist["Close"].iloc[-1])
            prev_close    = float(hist["Close"].iloc[-2])
            current_vol   = float(hist["Volume"].iloc[-1])
            hist_30       = hist.iloc[-31:-1]
            avg_vol_30    = float(hist_30["Volume"].mean())
            max_price_30  = float(hist_30["Close"].max())
            min_price_30  = float(hist_30["Close"].min())
            vol_ratio     = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0
            ma20          = float(hist["Close"].iloc[-20:].mean())
            prev_ma20     = float(hist["Close"].iloc[-21:-1].mean())
            return {
                "symbol":     sym,
                "name":       get_stock_name(sym, "港股"),
                "price":      round(current_price, 3),
                "prev_close": round(prev_close, 3),
                "volume":     int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio":  round(vol_ratio, 2),
                "max_30d":    round(max_price_30, 3),
                "min_30d":    round(min_price_30, 3),
                "ma20":       round(ma20, 3),
                "prev_ma20":  round(prev_ma20, 3),
                "market":     "港股",
            }
        except Exception as e:
            print(f"  WARNING  港股 {sym} 收盘数据获取失败: {e}")
            return None

    results, failed = [], []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, s): s for s in HK_STOCKS}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
            else:
                failed.append(futures[future])
    return results, failed


def get_close_data_a():
    """并发获取A股收盘价 + 历史数据（yfinance），返回 (results, failed)"""
    def _fetch(code):
        yf_sym = f"{code}.SS" if code.startswith("6") else f"{code}.SZ"
        try:
            hist = yf.Ticker(yf_sym).history(period="60d")
            if hist.empty or len(hist) < 22:
                return None
            current_price = float(hist["Close"].iloc[-1])
            prev_close    = float(hist["Close"].iloc[-2])
            current_vol   = float(hist["Volume"].iloc[-1])
            hist_30       = hist.iloc[-31:-1]
            avg_vol_30    = float(hist_30["Volume"].mean())
            max_price_30  = float(hist_30["Close"].max())
            min_price_30  = float(hist_30["Close"].min())
            vol_ratio     = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0
            ma20          = float(hist["Close"].iloc[-20:].mean())
            prev_ma20     = float(hist["Close"].iloc[-21:-1].mean())
            return {
                "symbol":     code,
                "name":       get_stock_name(code, "A股"),
                "price":      round(current_price, 3),
                "prev_close": round(prev_close, 3),
                "volume":     int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio":  round(vol_ratio, 2),
                "max_30d":    round(max_price_30, 3),
                "min_30d":    round(min_price_30, 3),
                "ma20":       round(ma20, 3),
                "prev_ma20":  round(prev_ma20, 3),
                "market":     "A股",
            }
        except Exception as e:
            print(f"  WARNING  A股 {code} 收盘数据获取失败: {e}")
            return None

    results, failed = [], []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, code): code for code in A_STOCKS}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
            else:
                failed.append(futures[future])
    return results, failed


def check_close_alerts(stock):
    """检查条件2（30天新高/低）+ 条件3（成交量异常）+ 条件4（MA20穿越）"""
    triggered = []
    price      = stock["price"]
    prev_close = stock.get("prev_close")
    ma20       = stock.get("ma20")
    prev_ma20  = stock.get("prev_ma20")

    if price >= stock["max_30d"]:
        triggered.append(f"[peak] 条件2 收盘创近30天新高：{price} >= 30日最高 {stock['max_30d']}")
    elif price <= stock["min_30d"]:
        triggered.append(f"[trough] 条件2 收盘创近30天新低：{price} <= 30日最低 {stock['min_30d']}")

    if stock["vol_ratio"] >= VOLUME_MULTIPLIER:
        triggered.append(
            f"[fire] 条件3 成交量异常：今日 {stock['volume']:,}，"
            f"是30日均量的 {stock['vol_ratio']:.1f} 倍（阈值 {VOLUME_MULTIPLIER}x）"
        )

    if prev_close is not None and ma20 is not None and prev_ma20 is not None:
        if prev_close < prev_ma20 and price >= ma20:
            triggered.append(
                f"[cross-up] 条件4 上穿MA20：昨收 {prev_close} < 昨日MA20 {prev_ma20:.3f}，"
                f"今收 {price} >= 今日MA20 {ma20:.3f}"
            )
        elif prev_close > prev_ma20 and price <= ma20:
            triggered.append(
                f"[cross-down] 条件4 下穿MA20：昨收 {prev_close} > 昨日MA20 {prev_ma20:.3f}，"
                f"今收 {price} <= 今日MA20 {ma20:.3f}"
            )

    return triggered


def run_close_check(market):
    """收盘后检测模式，汇总推送一条，末尾附失败列表"""
    market_name = {"a": "A股", "hk": "港股", "us": "美股"}[market]
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {market_name}收盘后检测...")

    if market == "a":
        stocks, failed = get_close_data_a()
    elif market == "hk":
        stocks, failed = get_close_data_hk()
    else:
        stocks, failed = get_close_data_us(US_STOCKS)

    print(f"成功获取 {len(stocks)} 支{market_name}收盘数据，{len(failed)} 支失败")

    alert_blocks = []
    for stock in stocks:
        conditions = check_close_alerts(stock)
        if not conditions:
            continue
        block = "\n".join([
            f"### {stock['name']}（{stock['symbol']}）",
            f"市场：{stock['market']} | 收盘价：**{stock['price']}** | MA20：{stock.get('ma20', '-')}",
            f"近30天：{stock['min_30d']} ~ {stock['max_30d']} | 量比：{stock['vol_ratio']:.1f}x",
        ] + conditions)
        alert_blocks.append(block)

    if not alert_blocks and not failed:
        print(f"{market_name}无收盘异动触发")
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    sections = [
        f"## {market_name}收盘异动汇总（{now_str}）\n共 **{len(alert_blocks)}** 支触发"
    ] + alert_blocks
    if failed:
        sections.append(f"---\n**数据获取失败（{len(failed)} 支）：** {', '.join(failed)}")

    content = "\n\n---\n\n".join(sections)
    send_to_wechat(
        f"{market_name}收盘异动 {len(alert_blocks)} 支（{now_str}）",
        content
    )
    print(f"共 {len(alert_blocks)} 条异动，已汇总推送")


# ============================================================
# 模式四/五/六：日报（大盘指数 + 个股 + Qwen新闻摘要）
# ============================================================

_qwen_client = None

def _get_qwen_client():
    global _qwen_client
    if _qwen_client is None and DASHSCOPE_API_KEY:
        _qwen_client = openai.OpenAI(
            api_key=DASHSCOPE_API_KEY,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
    return _qwen_client


def get_news_summary(symbol, name, market):
    """获取股票新闻并用 Qwen 总结（最多10条新闻，含内容摘要）"""
    news_texts = []

    try:
        if market in ["美股", "港股"]:
            yf_sym = f"{int(symbol.replace('.HK', '')):04d}.HK" if market == "港股" else symbol
            ticker = yf.Ticker(yf_sym)
            for n in ticker.news[:10]:
                if "content" in n and "title" in n["content"]:
                    title   = n["content"]["title"]
                    summary = n["content"].get("summary", "")
                    text = f"- {title}"
                    if summary:
                        text += f"\n  {summary[:300]}"
                    news_texts.append(text)
        elif market == "A股":
            news_df = ak.stock_news_em(symbol=symbol)
            if news_df is not None and not news_df.empty:
                for _, row in news_df.head(10).iterrows():
                    title   = row.get('新闻标题', '')
                    content = str(row.get('新闻内容', '') or '')
                    text = f"- {title}"
                    if content and content != 'nan':
                        text += f"\n  {content[:300]}"
                    news_texts.append(text)
    except Exception as e:
        print(f"  WARNING  {symbol} 新闻获取失败: {e}")

    if not news_texts:
        return "暂无近期新闻"

    if not DASHSCOPE_API_KEY:
        return "（未配置 DASHSCOPE_API_KEY）\n" + "\n".join(news_texts[:3])

    try:
        client = _get_qwen_client()
        prompt = (
            f"以下是{name}（{symbol}）的最新相关新闻：\n"
            + "\n".join(news_texts)
            + "\n\n请对该股票的近期动态进行详细分析，涵盖以下几点：\n"
            + "1. 核心新闻事件与主要催化剂\n"
            + "2. 对公司基本面或股价的潜在影响\n"
            + "3. 市场情绪与投资者关注焦点\n"
            + "4. 行业或宏观层面的重要背景\n"
            + "用中文回答，约200-300字，条理清晰，重点突出。"
        )
        resp = client.chat.completions.create(
            model="qwen-plus",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"  WARNING  {symbol} Qwen摘要失败: {e}")
        return "新闻摘要获取失败"


def get_daily_data_us(symbols=None):
    """并发获取美股日报数据，返回 (results, failed)"""
    if symbols is None:
        symbols = US_STOCKS

    def _fetch(symbol):
        try:
            hist = yf.Ticker(symbol).history(period="15d")
            if hist.empty or len(hist) < 5:
                return None
            current_price = hist["Close"].iloc[-1]
            prev_close    = hist["Close"].iloc[-2]
            current_vol   = hist["Volume"].iloc[-1]
            avg_vol_7     = hist["Volume"].iloc[-8:-1].mean()
            change_pct    = (current_price - prev_close) / prev_close * 100
            vol_ratio     = current_vol / avg_vol_7 if avg_vol_7 > 0 else 0
            return {
                "symbol":     symbol,
                "name":       symbol,
                "price":      round(float(current_price), 3),
                "change_pct": round(float(change_pct), 2),
                "volume":     int(current_vol),
                "avg_vol_7":  int(avg_vol_7),
                "vol_ratio":  round(float(vol_ratio), 2),
                "market":     "美股",
            }
        except Exception as e:
            print(f"  WARNING  {symbol} 日报数据失败: {e}")
            return None

    results, failed = [], []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch, s): s for s in symbols}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
            else:
                failed.append(futures[future])
    return results, failed


def get_daily_data_hk(stock_list=None):
    """获取港股日报数据，返回 (results, failed)"""
    if stock_list is None:
        stock_list = HK_STOCKS

    def _fetch(sym):
        code_4d = _hk_to_yf(sym)
        try:
            hist = yf.Ticker(code_4d).history(period="30d")
            if hist.empty or len(hist) < 5:
                return None
            close      = float(hist["Close"].iloc[-1])
            prev       = float(hist["Close"].iloc[-2])
            change_pct = (close - prev) / prev * 100
            vol        = float(hist["Volume"].iloc[-1])
            avg_vol_7  = hist["Volume"].iloc[-8:-1].mean()
            vol_ratio  = vol / avg_vol_7 if avg_vol_7 > 0 else 0
            return {
                "symbol":     sym,
                "name":       get_stock_name(sym, "港股"),
                "price":      round(close, 3),
                "change_pct": round(change_pct, 2),
                "volume":     int(vol),
                "avg_vol_7":  int(avg_vol_7),
                "vol_ratio":  round(float(vol_ratio), 2),
                "market":     "港股",
            }
        except Exception as e:
            print(f"  WARNING  港股 {sym} 历史数据失败: {e}")
            return None

    results, failed = [], []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, s): s for s in stock_list}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
            else:
                failed.append(futures[future])
    return results, failed


def get_daily_data_a(stock_list=None):
    """获取A股日报数据，返回 (results, failed)"""
    if stock_list is None:
        stock_list = A_STOCKS

    def _fetch(code):
        yf_sym = f"{code}.SS" if code.startswith("6") else f"{code}.SZ"
        try:
            hist = yf.Ticker(yf_sym).history(period="30d")
            if hist.empty or len(hist) < 5:
                return None
            close      = float(hist["Close"].iloc[-1])
            prev       = float(hist["Close"].iloc[-2])
            change_pct = (close - prev) / prev * 100
            vol        = float(hist["Volume"].iloc[-1])
            avg_vol_7  = hist["Volume"].iloc[-8:-1].mean()
            vol_ratio  = vol / avg_vol_7 if avg_vol_7 > 0 else 0
            return {
                "symbol":     code,
                "name":       get_stock_name(code, "A股"),
                "price":      round(close, 3),
                "change_pct": round(change_pct, 2),
                "volume":     int(vol),
                "avg_vol_7":  int(avg_vol_7),
                "vol_ratio":  round(float(vol_ratio), 2),
                "market":     "A股",
            }
        except Exception as e:
            print(f"  WARNING  A股 {code} 历史数据失败: {e}")
            return None

    results, failed = [], []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, code): code for code in stock_list}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
            else:
                failed.append(futures[future])
    return results, failed


def run_daily_report(market, user=None):
    """
    日报模式：大盘指数 + 个股（并发获取新闻摘要）+ 失败列表。
    user=None  → owner，使用全局股票列表，通过 PushPlus 推送微信
    user=dict  → 外部用户，使用其自定义列表，通过 Email 推送
    """
    market_name = {"a": "A股", "hk": "港股", "us": "美股"}[market]
    tag = f"（{user['name']}）" if user else ""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 生成{market_name}日报{tag}...")

    if user:
        us_list = user.get("us_stocks") or []
        hk_list = user.get("hk_stocks") or []
        a_list  = user.get("a_stocks")  or []
    else:
        us_list = hk_list = a_list = None

    if market == "a":
        stocks, failed = get_daily_data_a(a_list)
    elif market == "hk":
        stocks, failed = get_daily_data_hk(hk_list)
    else:
        stocks, failed = get_daily_data_us(us_list)

    if not stocks:
        print(f"{market_name}无数据，跳过日报{tag}")
        return

    stocks = sorted(stocks, key=lambda x: -x["change_pct"])

    # 并发获取所有新闻摘要
    print(f"  并发获取 {len(stocks)} 支{market_name}新闻摘要...")
    summaries = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        fs = {
            executor.submit(get_news_summary, s["symbol"], s["name"], s["market"]): s["symbol"]
            for s in stocks
        }
        for f in as_completed(fs):
            sym = fs[f]
            try:
                summaries[sym] = f.result()
            except Exception:
                summaries[sym] = "新闻摘要获取失败"

    # 大盘指数
    indices      = get_market_indices(market)
    indices_line = _format_indices(indices)

    blocks = []
    for stock in stocks:
        arrow = "up" if stock["change_pct"] >= 0 else "down"
        block = "\n".join([
            f"### [{arrow}] {stock['name']}（{stock['symbol']}）",
            f"收盘价：**{stock['price']}** | 涨跌幅：**{stock['change_pct']:+.2f}%**",
            f"今日成交量：{stock['volume']:,} | 7日均量：{stock['avg_vol_7']:,} | 量比：{stock['vol_ratio']:.2f}x",
            f"**新闻摘要：** {summaries.get(stock['symbol'], '-')}",
        ])
        blocks.append(block)

    now_str  = datetime.now().strftime('%Y-%m-%d %H:%M')
    title    = f"{market_name}日报 {datetime.now().strftime('%Y-%m-%d')}"
    header   = "\n".join([
        f"## {market_name}日报（{now_str}）",
        f"共 **{len(stocks)}** 支股票",
        f"**今日大盘：** {indices_line}",
    ])
    sections = [header] + blocks
    if failed:
        sections.append(f"---\n**数据获取失败（{len(failed)} 支）：** {', '.join(failed)}")

    content = "\n\n---\n\n".join(sections)

    if user:
        send_email(user["email"], title, content)
    else:
        send_to_wechat(title, content)

    print(f"{market_name}日报已推送{tag}，共 {len(stocks)} 支，失败 {len(failed)} 支")


def run_daily_report_all(market):
    """依次为 owner 和 users.json 中所有用户生成并推送日报"""
    run_daily_report(market)
    for user in load_users():
        run_daily_report(market, user=user)


# ============================================================
# 模式七/八/九：周报（每周五）
# ============================================================

def get_weekly_data(symbols, market):
    """
    获取本周涨跌幅（最近5个交易日）。
    返回 (results, failed)，每条包含 symbol/name/week_open/week_close/week_change_pct。
    market 为中文字符串："A股"/"港股"/"美股"
    """
    def _to_yf(sym):
        if market == "A股":
            return f"{sym}.SS" if sym.startswith("6") else f"{sym}.SZ"
        elif market == "港股":
            return f"{int(sym.replace('.HK', '')):04d}.HK"
        return sym

    def _fetch(sym):
        yf_sym = _to_yf(sym)
        try:
            hist = yf.Ticker(yf_sym).history(period="10d")
            if hist.empty or len(hist) < 5:
                return None
            week_open  = float(hist["Close"].iloc[-5])
            week_close = float(hist["Close"].iloc[-1])
            week_high  = float(hist["High"].iloc[-5:].max())
            week_low   = float(hist["Low"].iloc[-5:].min())
            week_chg   = (week_close - week_open) / week_open * 100
            return {
                "symbol":          sym,
                "name":            get_stock_name(sym, market),
                "week_open":       round(week_open, 3),
                "week_close":      round(week_close, 3),
                "week_high":       round(week_high, 3),
                "week_low":        round(week_low, 3),
                "week_change_pct": round(week_chg, 2),
                "market":          market,
            }
        except Exception as e:
            print(f"  WARNING  {yf_sym} 周数据失败: {e}")
            return None

    workers = 8 if market == "美股" else 5
    results, failed = [], []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch, s): s for s in symbols}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
            else:
                failed.append(futures[future])
    return results, failed


def run_weekly_report(market, user=None):
    """
    周报：今日大盘 + 本周涨幅 Top5 / 跌幅 Top5。
    user=None → owner (PushPlus)，user=dict → Email
    """
    market_name = {"a": "A股", "hk": "港股", "us": "美股"}[market]
    tag = f"（{user['name']}）" if user else ""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 生成{market_name}周报{tag}...")

    if user:
        sym_map = {
            "a":  user.get("a_stocks")  or [],
            "hk": user.get("hk_stocks") or [],
            "us": user.get("us_stocks") or [],
        }
    else:
        sym_map = {"a": A_STOCKS, "hk": HK_STOCKS, "us": US_STOCKS}

    symbols = sym_map[market]
    if not symbols:
        print(f"{market_name}周报无股票列表，跳过{tag}")
        return

    stocks, failed = get_weekly_data(symbols, market_name)
    if not stocks:
        print(f"{market_name}无周报数据{tag}")
        return

    stocks_sorted = sorted(stocks, key=lambda x: -x["week_change_pct"])
    top_gainers   = stocks_sorted[:5]
    top_losers    = stocks_sorted[-5:][::-1]

    table_header = (
        "| 股票 | 周初收盘 | 周末收盘 | 周涨跌幅 | 周振幅 |\n"
        "|------|---------|---------|---------|--------|"
    )

    def _row(s):
        arrow = "up" if s["week_change_pct"] >= 0 else "down"
        return (
            f"| [{arrow}] {s['name']}（{s['symbol']}）"
            f" | {s['week_open']}"
            f" | {s['week_close']}"
            f" | **{s['week_change_pct']:+.2f}%**"
            f" | {s['week_low']} ~ {s['week_high']} |"
        )

    indices      = get_market_indices(market)
    indices_line = _format_indices(indices)

    now_str  = datetime.now().strftime('%Y-%m-%d %H:%M')
    week_str = datetime.now().strftime('%Y 第%W周')
    title    = f"{market_name}周报 {datetime.now().strftime('%Y-%m-%d')}"

    sections = [
        f"## {market_name}周报（{week_str}，{now_str}）\n**今日大盘：** {indices_line}",
        f"### 本周涨幅 Top5\n{table_header}\n" + "\n".join(_row(s) for s in top_gainers),
        f"### 本周跌幅 Top5\n{table_header}\n" + "\n".join(_row(s) for s in top_losers),
    ]
    if failed:
        sections.append(f"---\n**数据获取失败（{len(failed)} 支）：** {', '.join(failed)}")

    content = "\n\n".join(sections)

    if user:
        send_email(user["email"], title, content)
    else:
        send_to_wechat(title, content)

    print(f"{market_name}周报已推送{tag}，共 {len(stocks)} 支，失败 {len(failed)} 支")


def run_weekly_report_all(market):
    """依次为 owner 和所有 users.json 用户生成并推送周报"""
    run_weekly_report(market)
    for user in load_users():
        run_weekly_report(market, user=user)


# ============================================================
# 主入口
# ============================================================

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "intraday"

    if mode == "intraday":
        run_intraday()
    elif mode == "intraday_a":
        run_intraday("a")
    elif mode == "intraday_hk":
        run_intraday("hk")
    elif mode == "intraday_us":
        run_intraday("us")
    elif mode == "close_a":
        run_close_check("a")
    elif mode == "close_hk":
        run_close_check("hk")
    elif mode == "close_us":
        run_close_check("us")
    elif mode == "daily_a":
        run_daily_report_all("a")
    elif mode == "daily_hk":
        run_daily_report_all("hk")
    elif mode == "daily_us":
        run_daily_report_all("us")
    elif mode == "weekly_a":
        run_weekly_report_all("a")
    elif mode == "weekly_hk":
        run_weekly_report_all("hk")
    elif mode == "weekly_us":
        run_weekly_report_all("us")
    else:
        print(f"未知模式：{mode}，可选：intraday / close_a/hk/us / daily_a/hk/us / weekly_a/hk/us")
        sys.exit(1)
