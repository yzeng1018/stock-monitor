"""
股票异动监控脚本 v2
支持美股、港股、A股

运行模式：
  intraday  - 盘中实时监控（每5分钟）：当日实时价 vs 昨日收盘 > ±4%
  close_a   - A股收盘后30分钟：条件2（30天新高/低）+ 条件3（成交量异常）
  close_hk  - 港股收盘后30分钟：条件2 + 条件3
  close_us  - 美股收盘后30分钟：条件2 + 条件3
  daily_a   - A股日报（收盘后1小时）：股价/涨跌/成交量/7日均量 + ChatGPT新闻摘要
  daily_hk  - 港股日报（收盘后1小时）：同上
  daily_us  - 美股日报（收盘后1小时）：同上
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

ALERTED_TODAY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerted_today.json")


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
        print(f"  ⚠️ 保存 alerted_today.json 失败: {e}")

US_STOCKS = [
    "GOOG", "PDD", "NIO", "TSM", "AMZN", "CRCL", "SBUX", "BKNG",
    "META", "ABNB", "DUOL", "AAPL", "UBER", "FUTU", "XNET", "NVDA",
    "DIDIY", "FIG", "BEKE", "EDU", "LKNCY", "TAL", "SE",
    "DASH", "TSLA", "MELI", "LI", "GOTU", "XPEV", "BIDU",
    "SY", "TCOM", "PONY", "BILI", "WRD", "LU", "APP", "SOFI", "OWL"
]

HK_STOCKS = [
    "02513.HK", "00100.HK", "02252.HK", "08083.HK",
    "02550.HK", "06030.HK", "00853.HK", "02333.HK", "02013.HK",
    "03750.HK", "03690.HK", "01797.HK", "09618.HK", "00700.HK",
    "01211.HK", "09868.HK", "09992.HK", "01024.HK", "01810.HK",
    "00981.HK", "02643.HK", "09988.HK", "09626.HK"
]

A_STOCKS = [
    "688207", "688256", "688981", "600519", "688277", "603019",
    "000034", "600030", "002594", "300896", "002230", "601318",
    "300750", "000737", "300418"
]

# ============================================================
# 推送（PushPlus）——汇总模式，一次发一条
# ============================================================

def send_to_wechat(title, content):
    if not PUSHPLUS_TOKEN:
        print("⚠️ 未配置 PUSHPLUS_TOKEN，打印到控制台")
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
            print(f"  ✅ 推送成功：{title}")
        else:
            print(f"  ❌ 推送失败：{data.get('msg')} | {title}")
    except Exception as e:
        print(f"  ❌ 推送异常：{e}")


def send_email(to_addr, subject, content_md):
    """将 Markdown 内容转为 HTML 发送邮件"""
    if not all([SMTP_USER, SMTP_PASSWORD]):
        print(f"⚠️ 未配置SMTP，跳过邮件: {subject}")
        return
    # 简单 Markdown → HTML 转换
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
        print(f"  ✅ 邮件发送成功：{to_addr} | {subject}")
    except Exception as e:
        print(f"  ❌ 邮件发送失败：{e}")


_USERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.json")

def load_users():
    """读取 users.json，返回用户列表"""
    try:
        with open(_USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ 读取 users.json 失败: {e}")
        return []


# ============================================================
# 模式一：盘中实时监控（条件1）
# ============================================================

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
            print(f"  ⚠️  {symbol} 实时数据获取失败: {e}")
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
    代码格式：'00700.HK' → '700.HK'（去前导零）
    """
    def _fetch(original):
        yf_sym = _hk_to_yf(original)
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
            print(f"  ⚠️  {yf_sym} 实时数据获取失败: {e}")
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
    代码格式：6开头 → .SS（上交所），其余 → .SZ（深交所）
    """
    def _to_yf(code):
        return f"{code}.SS" if code.startswith("6") else f"{code}.SZ"

    def _fetch(original):
        yf_sym = _to_yf(original)
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
            print(f"  ⚠️  {yf_sym} 实时数据获取失败: {e}")
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
        # 降级：保守时间窗口（仅覆盖 EST 冬令时 UTC 14:30-21:00）
        return 870 <= utc_min < 1260


def _hk_to_yf(sym):
    """将港股代码转为 yfinance 4位格式：'02513.HK' → '2513.HK'"""
    return f"{int(sym.replace('.HK', '')):04d}.HK"


_NAME_CACHE = None

def _ensure_name_cache():
    global _NAME_CACHE
    if _NAME_CACHE is None:
        try:
            cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stock_names.json")
            with open(cache_path, "r", encoding="utf-8") as f:
                _NAME_CACHE = json.load(f)
        except Exception:
            _NAME_CACHE = {}
    return _NAME_CACHE


def get_stock_name(symbol, market):
    """取股票中文名：① stock_names.json ② 新浪财经 API ③ 降级返回代码。
    美股直接返回 ticker，无需中文名。
    """
    if market == "美股":
        return symbol

    # ① 本地名称表（stock_names.json）
    cache = _ensure_name_cache()
    if symbol in cache:
        return cache[symbol]

    # ② 新浪财经 API（交易时段可用，休市时返回空）
    try:
        if market == "港股":
            code_4d = f"{int(symbol.replace('.HK', '')):04d}"
            url = f"https://hq.sinajs.cn/list=hk{code_4d}"
        else:  # A股
            prefix = "sh" if symbol.startswith("6") else "sz"
            url = f"https://hq.sinajs.cn/list={prefix}{symbol}"
        resp = requests.get(url, headers={"Referer": "https://finance.sina.com.cn"}, timeout=5)
        m = re.search(r'"([^"]+)"', resp.text)
        if m:
            name = m.group(1).split(",")[0].strip()
            if name and re.search(r'[\u4e00-\u9fff]', name):
                return name
    except Exception:
        pass

    # ③ 降级：返回代码
    return symbol


def run_intraday(market=None):
    """盘中监控。
    market='a'|'hk'|'us'  → 仅监控指定市场
    market=None            → 自动判断所有当前开盘市场
    每个市场独立推送一条通知，标题注明市场名称。
    """
    now_utc = datetime.utcnow()
    utc_min = now_utc.hour * 60 + now_utc.minute

    open_status = {
        "a":  90  <= utc_min < 420,   # A股  UTC 01:30-07:00
        "hk": 90  <= utc_min < 480,   # 港股 UTC 01:30-08:00
        "us": _is_us_regular_session(),
    }
    name_map  = {"a": "A股", "hk": "港股", "us": "美股"}
    fetch_map = {
        "a":  lambda: get_intraday_a(),
        "hk": lambda: get_intraday_hk(),
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

        # 仅对触发异动的少量股票按需查名称，降低 API 开销
        alert_lines = []
        for stock in triggered:
            name      = get_stock_name(stock["symbol"], stock["market"])
            emoji     = "📈" if stock["change_pct"] > 0 else "📉"
            vr        = stock.get("vol_ratio")
            vol_str   = f"{vr:.2f}x" if vr is not None else "-"
            alert_lines.append(
                f"| {emoji} {name}（{stock['symbol']}）"
                f" | {stock['prev_close']}"
                f" | {stock['price']}"
                f" | **{stock['change_pct']:+.2f}%**"
                f" | {vol_str} |"
            )

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        content = "\n".join([
            f"## 📊 {mkt_name}盘中异动汇总（{now_str}）",
            f"共 **{len(alert_lines)}** 支股票涨跌幅超过 ±{PRICE_CHANGE_THRESHOLD}%",
            "",
            "| 股票 | 昨收 | 现价 | 涨跌幅 | 量比 |",
            "|------|------|------|--------|------|",
        ] + alert_lines)

        send_to_wechat(
            f"📊 {mkt_name}盘中异动 {len(alert_lines)} 支（{now_str}）",
            content
        )
        save_alerted_today([s["symbol"] for s in triggered])
        alerted_today.update(s["symbol"] for s in triggered)
        print(f"{mkt_name}共 {len(alert_lines)} 条异动，已汇总推送")


# ============================================================
# 模式二/三：收盘后检测（条件2 + 条件3）
# ============================================================

def get_close_data_us(symbols):
    """并发获取美股收盘价 + 30天历史"""
    def _fetch(symbol):
        try:
            hist = yf.Ticker(symbol).history(period="35d")
            if hist.empty or len(hist) < 5:
                return None
            current_price = hist["Close"].iloc[-1]
            current_vol   = hist["Volume"].iloc[-1]
            hist_30       = hist.iloc[-31:-1]
            avg_vol_30    = hist_30["Volume"].mean()
            max_price_30  = hist_30["Close"].max()
            min_price_30  = hist_30["Close"].min()
            vol_ratio     = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0
            return {
                "symbol":    symbol,
                "name":      symbol,
                "price":     round(float(current_price), 3),
                "volume":    int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio": round(float(vol_ratio), 2),
                "max_30d":   round(float(max_price_30), 3),
                "min_30d":   round(float(min_price_30), 3),
                "market":    "美股",
            }
        except Exception as e:
            print(f"  ⚠️  {symbol} 收盘数据获取失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch, s): s for s in symbols}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_close_data_hk():
    """获取港股收盘价 + 30天历史（yfinance）"""
    def _fetch(sym):
        code_4d = _hk_to_yf(sym)
        try:
            hist = yf.Ticker(code_4d).history(period="35d")
            if hist.empty or len(hist) < 5:
                return None
            current_price = float(hist["Close"].iloc[-1])
            current_vol   = float(hist["Volume"].iloc[-1])
            hist_30       = hist.iloc[-31:-1]
            avg_vol_30    = hist_30["Volume"].mean()
            max_price_30  = hist_30["Close"].max()
            min_price_30  = hist_30["Close"].min()
            vol_ratio     = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0
            return {
                "symbol":     sym,
                "name":       get_stock_name(sym, "港股"),
                "price":      round(current_price, 3),
                "volume":     int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio":  round(float(vol_ratio), 2),
                "max_30d":    round(float(max_price_30), 3),
                "min_30d":    round(float(min_price_30), 3),
                "market":     "港股",
            }
        except Exception as e:
            print(f"  ⚠️  港股 {sym} 收盘数据获取失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, s): s for s in HK_STOCKS}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_close_data_a():
    """并发获取A股收盘价 + 30天历史（yfinance）"""
    def _fetch(code):
        yf_sym = f"{code}.SS" if code.startswith("6") else f"{code}.SZ"
        try:
            hist = yf.Ticker(yf_sym).history(period="35d")
            if hist.empty or len(hist) < 5:
                return None
            current_price = float(hist["Close"].iloc[-1])
            current_vol   = float(hist["Volume"].iloc[-1])
            hist_30       = hist.iloc[-31:-1]
            avg_vol_30    = hist_30["Volume"].mean()
            max_price_30  = hist_30["Close"].max()
            min_price_30  = hist_30["Close"].min()
            vol_ratio     = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0
            return {
                "symbol":     code,
                "name":       get_stock_name(code, "A股"),
                "price":      round(current_price, 3),
                "volume":     int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio":  round(float(vol_ratio), 2),
                "max_30d":    round(float(max_price_30), 3),
                "min_30d":    round(float(min_price_30), 3),
                "market":     "A股",
            }
        except Exception as e:
            print(f"  ⚠️  A股 {code} 收盘数据获取失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, code): code for code in A_STOCKS}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def check_close_alerts(stock):
    """检查条件2（30天新高/低）和条件3（成交量异常）"""
    triggered = []
    price = stock["price"]

    if price >= stock["max_30d"]:
        triggered.append(f"🏔️ 条件2 收盘创近30天新高：{price} ≥ 30日最高 {stock['max_30d']}")
    elif price <= stock["min_30d"]:
        triggered.append(f"🕳️ 条件2 收盘创近30天新低：{price} ≤ 30日最低 {stock['min_30d']}")

    if stock["vol_ratio"] >= VOLUME_MULTIPLIER:
        triggered.append(
            f"🔥 条件3 成交量异常：今日 {stock['volume']:,}，"
            f"是30日均量的 {stock['vol_ratio']:.1f} 倍（阈值 {VOLUME_MULTIPLIER}x）"
        )
    return triggered


def run_close_check(market):
    """收盘后检测模式，汇总推送一条"""
    market_name = {"a": "A股", "hk": "港股", "us": "美股"}[market]
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {market_name}收盘后检测...")

    if market == "a":
        stocks = get_close_data_a()
    elif market == "hk":
        stocks = get_close_data_hk()
    else:
        stocks = get_close_data_us(US_STOCKS)

    print(f"成功获取 {len(stocks)} 支{market_name}收盘数据")

    # 收集所有触发项
    alert_blocks = []
    for stock in stocks:
        triggered = check_close_alerts(stock)
        if not triggered:
            continue
        block = "\n".join([
            f"### 📊 {stock['name']}（{stock['symbol']}）",
            f"市场：{stock['market']} | 收盘价：**{stock['price']}**",
            f"近30天：{stock['min_30d']} ～ {stock['max_30d']} | "
            f"量比：{stock['vol_ratio']:.1f}x",
        ] + triggered)
        alert_blocks.append(block)

    if not alert_blocks:
        print(f"{market_name}无收盘异动触发")
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    content = "\n\n---\n\n".join([
        f"## {market_name}收盘异动汇总（{now_str}）\n共 **{len(alert_blocks)}** 支触发",
    ] + alert_blocks)

    send_to_wechat(
        f"📊 {market_name}收盘异动 {len(alert_blocks)} 支（{now_str}）",
        content
    )
    print(f"共 {len(alert_blocks)} 条异动，已汇总推送")


# ============================================================
# 模式四/五/六：日报（股价 + 成交量 + ChatGPT新闻摘要）
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
            # 港股 symbol 需转为 yfinance 4位格式（02513.HK → 2513.HK）
            yf_sym = _hk_to_yf(symbol) if market == "港股" else symbol
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
        print(f"  ⚠️  {symbol} 新闻获取失败: {e}")

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
        print(f"  ⚠️  {symbol} Qwen摘要失败: {e}")
        return "新闻摘要获取失败"


def get_daily_data_us(symbols=None):
    """并发获取美股日报数据：收盘价、涨跌幅、成交量、7日均量"""
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
                "symbol":    symbol,
                "name":      symbol,
                "price":     round(float(current_price), 3),
                "change_pct": round(float(change_pct), 2),
                "volume":    int(current_vol),
                "avg_vol_7": int(avg_vol_7),
                "vol_ratio": round(float(vol_ratio), 2),
                "market":    "美股",
            }
        except Exception as e:
            print(f"  ⚠️  {symbol} 日报数据失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch, s): s for s in symbols}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_daily_data_hk(stock_list=None):
    """获取港股日报数据：yfinance 历史K线，收盘价/涨跌幅/成交量/7日均量"""
    if stock_list is None:
        stock_list = HK_STOCKS

    def _fetch(sym):
        code_4d = _hk_to_yf(sym)
        try:
            hist = yf.Ticker(code_4d).history(period="30d")
            if hist.empty or len(hist) < 5:
                return None
            close     = float(hist["Close"].iloc[-1])
            prev      = float(hist["Close"].iloc[-2])
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
            print(f"  ⚠️  港股 {sym} 历史数据失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, s): s for s in stock_list}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_daily_data_a(stock_list=None):
    """获取A股日报数据：yfinance 历史K线，收盘价/涨跌幅/成交量/7日均量"""
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
            print(f"  ⚠️  A股 {code} 历史数据失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch, code): code for code in stock_list}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def run_daily_report(market, user=None):
    """
    日报模式：每支股票展示股价/涨跌/成交量/7日均量 + Qwen新闻摘要，汇总推送一条。
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
        us_list = hk_list = a_list = None  # 使用全局默认列表

    if market == "a":
        stocks = get_daily_data_a(a_list)
    elif market == "hk":
        stocks = get_daily_data_hk(hk_list)
    else:
        stocks = get_daily_data_us(us_list)

    if not stocks:
        print(f"{market_name}无数据，跳过日报{tag}")
        return

    stocks = sorted(stocks, key=lambda x: -x["change_pct"])

    print(f"  并发获取 {len(stocks)} 支股票新闻摘要...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_stock = {
            executor.submit(get_news_summary, s["symbol"], s["name"], s["market"]): s
            for s in stocks
        }
        summaries = {}
        for future in as_completed(future_to_stock):
            s = future_to_stock[future]
            summaries[s["symbol"]] = future.result()
            print(f"  ✓ {s['symbol']} 新闻摘要完成")

    blocks = []
    for stock in stocks:
        summary = summaries.get(stock["symbol"], "新闻摘要获取失败")
        emoji   = "📈" if stock["change_pct"] >= 0 else "📉"
        block   = "\n".join([
            f"### {emoji} {stock['name']}（{stock['symbol']}）",
            f"收盘价：**{stock['price']}** | 涨跌幅：**{stock['change_pct']:+.2f}%**",
            f"今日成交量：{stock['volume']:,} | 7日均量：{stock['avg_vol_7']:,} | 量比：{stock['vol_ratio']:.2f}x",
            f"**新闻摘要：** {summary}",
        ])
        blocks.append(block)

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    title   = f"📋 {market_name}日报 {datetime.now().strftime('%Y-%m-%d')}"
    content = "\n\n---\n\n".join([
        f"## 📋 {market_name}日报（{now_str}）\n共 **{len(stocks)}** 支股票",
    ] + blocks)

    if user:
        send_email(user["email"], title, content)
    else:
        send_to_wechat(title, content)

    print(f"{market_name}日报已推送{tag}，共 {len(stocks)} 支股票")


def run_daily_report_all(market):
    """依次为 owner 和 users.json 中所有用户生成并推送日报"""
    # owner：PushPlus 微信推送
    run_daily_report(market)
    # 外部用户：Email 推送
    for user in load_users():
        run_daily_report(market, user=user)


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
    else:
        print(f"未知模式：{mode}，可选：intraday / close_a / close_hk / close_us / daily_a / daily_hk / daily_us")
        sys.exit(1)
