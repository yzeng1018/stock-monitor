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
import akshare as ak
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import time

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

US_STOCKS = [
    "GOOG", "PDD", "NIO", "TSM", "AMZN", "CRCL", "SBUX", "BKNG",
    "META", "ABNB", "DUOL", "AAPL", "UBER", "FUTU", "XNET", "NVDA",
    "DIDIY", "FIG", "BEKE", "EDU", "HOOD", "LKNCY", "TAL", "SE",
    "DASH", "TSLA", "MELI", "LI", "GOTU", "ZH", "XPEV", "BIDU",
    "SY", "TCOM", "PONY", "BILI", "WRD", "RBLX", "LU"
]

HK_STOCKS = [
    "02513.HK", "00100.HK", "02252.HK", "08083.HK", "02559.HK",
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


def load_users():
    """读取 users.json，返回用户列表"""
    try:
        with open("users.json", "r", encoding="utf-8") as f:
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
            return {
                "symbol":     symbol,
                "name":       symbol,
                "price":      round(float(current), 3),
                "prev_close": round(float(prev_close), 3),
                "change_pct": round(float(change_pct), 2),
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
    """用 akshare 批量拉取港股实时价（yfinance 在 GitHub Actions 上无法访问 HK 数据）"""
    results = []
    hk_codes = [s.replace(".HK", "") for s in HK_STOCKS]
    try:
        spot_df = ak.stock_hk_spot_em()
        spot_df = spot_df[spot_df["代码"].isin(hk_codes)].copy()
        for _, row in spot_df.iterrows():
            prev_close = float(row["昨收"])
            current    = float(row["最新价"])
            if prev_close == 0:
                continue
            change_pct = (current - prev_close) / prev_close * 100
            results.append({
                "symbol":     row["代码"] + ".HK",
                "name":       row["名称"],
                "price":      round(current, 3),
                "prev_close": round(prev_close, 3),
                "change_pct": round(change_pct, 2),
                "market":     "港股",
            })
    except Exception as e:
        print(f"港股实时行情获取失败: {e}")
    return results


def get_intraday_a():
    """用 akshare 批量拉取A股实时价，失败自动重试3次"""
    results = []
    for attempt in range(3):
        try:
            spot_df = ak.stock_zh_a_spot_em()
            spot_df = spot_df[spot_df["代码"].isin(A_STOCKS)].copy()
            for _, row in spot_df.iterrows():
                prev_close = float(row["昨收"])
                current    = float(row["最新价"])
                if prev_close == 0:
                    continue
                results.append({
                    "symbol":     row["代码"],
                    "name":       row["名称"],
                    "price":      round(current, 3),
                    "prev_close": round(prev_close, 3),
                    "change_pct": round(float(row["涨跌幅"]), 2),
                    "market":     "A股",
                })
            return results
        except Exception as e:
            print(f"A股实时行情获取失败（第{attempt+1}次）: {e}")
            if attempt < 2:
                time.sleep(5)
    return results


def run_intraday():
    """盘中模式：实时价 vs 昨日收盘，涨跌幅 > ±4%，汇总推送一条"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 盘中实时监控...")

    all_stocks = []
    print("获取美股实时数据（并发）...")
    all_stocks.extend(get_intraday_us(US_STOCKS))
    print("获取港股实时数据（并发）...")
    all_stocks.extend(get_intraday_hk())
    print("获取A股实时数据...")
    all_stocks.extend(get_intraday_a())
    print(f"成功获取 {len(all_stocks)} 支股票实时数据")

    # 收集所有触发项
    alert_lines = []
    for stock in sorted(all_stocks, key=lambda x: -abs(x["change_pct"])):
        if abs(stock["change_pct"]) < PRICE_CHANGE_THRESHOLD:
            continue
        emoji = "📈" if stock["change_pct"] > 0 else "📉"
        alert_lines.append(
            f"| {emoji} {stock['name']}（{stock['symbol']}）"
            f" | {stock['market']}"
            f" | {stock['price']}"
            f" | **{stock['change_pct']:+.2f}%** |"
        )

    if not alert_lines:
        print("无盘中异动触发")
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    content = "\n".join([
        f"## 📊 盘中异动汇总（{now_str}）",
        f"共 **{len(alert_lines)}** 支股票涨跌幅超过 ±{PRICE_CHANGE_THRESHOLD}%",
        "",
        "| 股票 | 市场 | 现价 | 涨跌幅 |",
        "|------|------|------|--------|",
    ] + alert_lines)

    send_to_wechat(
        f"📊 盘中异动 {len(alert_lines)} 支（{now_str}）",
        content
    )
    print(f"共 {len(alert_lines)} 条异动，已汇总推送")


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
    """获取港股收盘价 + 30天历史（akshare，并发拉历史）"""
    hk_codes = [s.replace(".HK", "") for s in HK_STOCKS]
    end_date   = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")

    # 先获取实时快照（当日收盘价/最新价）
    spot_map = {}
    try:
        spot_df = ak.stock_hk_spot_em()
        spot_df = spot_df[spot_df["代码"].isin(hk_codes)].copy()
        for _, row in spot_df.iterrows():
            spot_map[row["代码"]] = {
                "name":   row["名称"],
                "price":  float(row["最新价"]),
                "volume": float(row["成交量"]),
            }
    except Exception as e:
        print(f"港股实时行情获取失败: {e}")
        return []

    def _fetch_hist(code):
        try:
            hist = ak.stock_hk_hist(
                symbol=code, period="daily",
                start_date=start_date, end_date=end_date, adjust="qfq"
            )
            if hist is None or len(hist) < 5:
                return None
            hist = hist.sort_values("日期").reset_index(drop=True)
            hist_30      = hist.iloc[-31:-1]
            avg_vol_30   = hist_30["成交量"].mean()
            max_price_30 = hist_30["收盘"].max()
            min_price_30 = hist_30["收盘"].min()
            info = spot_map.get(code, {})
            current_vol = info.get("volume", 0)
            vol_ratio   = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0
            return {
                "symbol":    code + ".HK",
                "name":      info.get("name", code),
                "price":     round(info.get("price", 0), 3),
                "volume":    int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio": round(float(vol_ratio), 2),
                "max_30d":   round(float(max_price_30), 3),
                "min_30d":   round(float(min_price_30), 3),
                "market":    "港股",
            }
        except Exception as e:
            print(f"  ⚠️  港股 {code} 历史数据失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_hist, code): code for code in hk_codes if code in spot_map}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_close_data_a():
    """并发获取A股收盘价 + 30天历史"""
    end_date   = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")

    # 先获取当日实时数据（单次批量请求）
    spot_map = {}
    try:
        spot_df = ak.stock_zh_a_spot_em()
        spot_df = spot_df[spot_df["代码"].isin(A_STOCKS)].copy()
        for _, row in spot_df.iterrows():
            spot_map[row["代码"]] = {
                "name":   row["名称"],
                "price":  float(row["最新价"]),
                "volume": float(row["成交量"]),
            }
    except Exception as e:
        print(f"A股实时行情获取失败: {e}")
        return []

    def _fetch_hist(code):
        try:
            hist = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start_date, end_date=end_date, adjust="qfq"
            )
            if hist is None or len(hist) < 5:
                return None
            hist = hist.sort_values("日期").reset_index(drop=True)
            hist_30      = hist.iloc[-31:-1]
            avg_vol_30   = hist_30["成交量"].mean()
            max_price_30 = hist_30["收盘"].max()
            min_price_30 = hist_30["收盘"].min()
            info = spot_map.get(code, {})
            current_vol = info.get("volume", 0)
            vol_ratio   = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0
            return {
                "symbol":    code,
                "name":      info.get("name", code),
                "price":     round(info.get("price", 0), 3),
                "volume":    int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio": round(float(vol_ratio), 2),
                "max_30d":   round(float(max_price_30), 3),
                "min_30d":   round(float(min_price_30), 3),
                "market":    "A股",
            }
        except Exception as e:
            print(f"  ⚠️  A股 {code} 历史数据失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_hist, code): code for code in spot_map}
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

def get_news_summary(symbol, name, market):
    """获取股票新闻并用 ChatGPT 总结（最多5条新闻标题）"""
    news_texts = []

    try:
        if market in ["美股", "港股"]:
            ticker = yf.Ticker(symbol)
            for n in ticker.news[:5]:
                if "content" in n and "title" in n["content"]:
                    title   = n["content"]["title"]
                    summary = n["content"].get("summary", "")
                    news_texts.append(f"- {title}: {summary}" if summary else f"- {title}")
        elif market == "A股":
            news_df = ak.stock_news_em(symbol=symbol)
            if news_df is not None and not news_df.empty:
                for _, row in news_df.head(5).iterrows():
                    news_texts.append(f"- {row.get('新闻标题', '')}")
    except Exception as e:
        print(f"  ⚠️  {symbol} 新闻获取失败: {e}")

    if not news_texts:
        return "暂无今日新闻"

    if not DASHSCOPE_API_KEY:
        return "（未配置 DASHSCOPE_API_KEY）\n" + "\n".join(news_texts[:3])

    try:
        client = openai.OpenAI(
            api_key=DASHSCOPE_API_KEY,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        prompt = (
            f"以下是{name}（{symbol}）的最新相关新闻：\n"
            + "\n".join(news_texts)
            + "\n\n请用2-3句话简洁总结该股票今日的重点新闻和市场关注点。用中文回答，不超过100字。"
        )
        resp = client.chat.completions.create(
            model="qwen-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
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
    """获取港股日报数据：实时快照 + 7日均量（akshare）"""
    if stock_list is None:
        stock_list = HK_STOCKS
    hk_codes   = [s.replace(".HK", "") for s in stock_list]
    end_date   = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=20)).strftime("%Y%m%d")

    spot_map = {}
    try:
        spot_df = ak.stock_hk_spot_em()
        spot_df = spot_df[spot_df["代码"].isin(hk_codes)].copy()
        for _, row in spot_df.iterrows():
            prev_close = float(row["昨收"])
            current    = float(row["最新价"])
            change_pct = (current - prev_close) / prev_close * 100 if prev_close > 0 else 0
            spot_map[row["代码"]] = {
                "name":       row["名称"],
                "price":      current,
                "change_pct": round(change_pct, 2),
                "volume":     float(row["成交量"]),
            }
    except Exception as e:
        print(f"港股实时行情获取失败: {e}")
        return []

    def _fetch_hist(code):
        try:
            hist = ak.stock_hk_hist(
                symbol=code, period="daily",
                start_date=start_date, end_date=end_date, adjust="qfq"
            )
            if hist is None or len(hist) < 5:
                return None
            hist      = hist.sort_values("日期").reset_index(drop=True)
            avg_vol_7 = hist["成交量"].iloc[-8:-1].mean()
            info      = spot_map.get(code, {})
            current_vol = info.get("volume", 0)
            vol_ratio   = current_vol / avg_vol_7 if avg_vol_7 > 0 else 0
            return {
                "symbol":    code + ".HK",
                "name":      info.get("name", code),
                "price":     round(info.get("price", 0), 3),
                "change_pct": info.get("change_pct", 0),
                "volume":    int(current_vol),
                "avg_vol_7": int(avg_vol_7),
                "vol_ratio": round(float(vol_ratio), 2),
                "market":    "港股",
            }
        except Exception as e:
            print(f"  ⚠️  港股 {code} 历史数据失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_hist, code): code for code in hk_codes if code in spot_map}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_daily_data_a(stock_list=None):
    """获取A股日报数据：实时快照 + 7日均量（akshare）"""
    if stock_list is None:
        stock_list = A_STOCKS
    end_date   = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=20)).strftime("%Y%m%d")

    spot_map = {}
    for attempt in range(3):
        try:
            spot_df = ak.stock_zh_a_spot_em()
            spot_df = spot_df[spot_df["代码"].isin(stock_list)].copy()
            for _, row in spot_df.iterrows():
                spot_map[row["代码"]] = {
                    "name":       row["名称"],
                    "price":      float(row["最新价"]),
                    "change_pct": round(float(row["涨跌幅"]), 2),
                    "volume":     float(row["成交量"]),
                }
            break
        except Exception as e:
            print(f"A股实时行情获取失败（第{attempt+1}次）: {e}")
            if attempt < 2:
                time.sleep(5)

    def _fetch_hist(code):
        try:
            hist = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start_date, end_date=end_date, adjust="qfq"
            )
            if hist is None or len(hist) < 5:
                return None
            hist      = hist.sort_values("日期").reset_index(drop=True)
            avg_vol_7 = hist["成交量"].iloc[-8:-1].mean()
            info      = spot_map.get(code, {})
            current_vol = info.get("volume", 0)
            vol_ratio   = current_vol / avg_vol_7 if avg_vol_7 > 0 else 0
            return {
                "symbol":    code,
                "name":      info.get("name", code),
                "price":     round(info.get("price", 0), 3),
                "change_pct": info.get("change_pct", 0),
                "volume":    int(current_vol),
                "avg_vol_7": int(avg_vol_7),
                "vol_ratio": round(float(vol_ratio), 2),
                "market":    "A股",
            }
        except Exception as e:
            print(f"  ⚠️  A股 {code} 历史数据失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_hist, code): code for code in spot_map}
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

    blocks = []
    for stock in stocks:
        print(f"  获取 {stock['symbol']} 新闻摘要...")
        summary = get_news_summary(stock["symbol"], stock["name"], stock["market"])
        emoji   = "📈" if stock["change_pct"] >= 0 else "📉"
        block   = "\n".join([
            f"### {emoji} {stock['name']}（{stock['symbol']}）",
            f"收盘价：**{stock['price']}** | 涨跌幅：**{stock['change_pct']:+.2f}%**",
            f"今日成交量：{stock['volume']:,} | 7日均量：{stock['avg_vol_7']:,} | 量比：{stock['vol_ratio']:.2f}x",
            f"**新闻摘要：** {summary}",
        ])
        blocks.append(block)
        time.sleep(0.3)

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
