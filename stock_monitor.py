"""
股票异动监控脚本
支持美股、港股、A股

触发条件（满足任一即推送）：
  条件1：当日涨跌幅 > ±7%
  条件2：当日价格达到近30天最高价或最低价
  条件3：当日交易量 > 过去30天平均日交易量的 2.5 倍
"""

import os
import requests
import yfinance as yf
import akshare as ak
from datetime import datetime, timedelta
import time
import pandas as pd

# ============================================================
# 配置区域
# ============================================================

# PushPlus Token（在 https://www.pushplus.plus 获取）
PUSHPLUS_TOKEN = os.environ.get("PUSHPLUS_TOKEN", "")

# 异动阈值
PRICE_CHANGE_THRESHOLD = 7.0   # 涨跌幅阈值（%）
VOLUME_MULTIPLIER = 2.5         # 交易量倍数阈值

# 股票列表
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
# 数据获取（美股 + 港股）
# ============================================================

def get_us_hk_stock(symbol):
    """获取单支美股/港股完整数据，含30天历史"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="35d")
        if hist.empty or len(hist) < 5:
            return None

        current_price = hist["Close"].iloc[-1]
        prev_close    = hist["Close"].iloc[-2]
        current_vol   = hist["Volume"].iloc[-1]

        hist_30 = hist.iloc[-31:-1]
        avg_vol_30   = hist_30["Volume"].mean()
        max_price_30 = hist_30["Close"].max()
        min_price_30 = hist_30["Close"].min()

        change_pct = (current_price - prev_close) / prev_close * 100
        vol_ratio  = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0

        return {
            "symbol":     symbol,
            "name":       symbol,
            "price":      round(float(current_price), 3),
            "change_pct": round(float(change_pct), 2),
            "volume":     int(current_vol),
            "avg_vol_30": int(avg_vol_30),
            "vol_ratio":  round(float(vol_ratio), 2),
            "max_30d":    round(float(max_price_30), 3),
            "min_30d":    round(float(min_price_30), 3),
            "market":     "港股" if symbol.endswith(".HK") else "美股",
        }
    except Exception as e:
        print(f"  ⚠️  {symbol} 获取失败: {e}")
        return None


def get_us_hk_data(symbols):
    results = []
    for symbol in symbols:
        data = get_us_hk_stock(symbol)
        if data:
            results.append(data)
        time.sleep(0.3)
    return results


# ============================================================
# 数据获取（A股）
# ============================================================

def get_a_stock_data(codes):
    """获取A股实时 + 30天历史数据"""
    results = []
    try:
        spot_df = ak.stock_zh_a_spot_em()
        spot_df = spot_df[spot_df["代码"].isin(codes)].copy()
    except Exception as e:
        print(f"A股实时行情获取失败: {e}")
        return results

    end_date   = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")

    for _, row in spot_df.iterrows():
        code = row["代码"]
        try:
            hist = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start_date, end_date=end_date, adjust="qfq"
            )
            if hist is None or len(hist) < 5:
                continue

            hist = hist.sort_values("日期").reset_index(drop=True)
            current_price = float(hist["收盘"].iloc[-1])
            current_vol   = float(hist["成交量"].iloc[-1])

            hist_30 = hist.iloc[-31:-1]
            avg_vol_30   = hist_30["成交量"].mean()
            max_price_30 = hist_30["收盘"].max()
            min_price_30 = hist_30["收盘"].min()

            change_pct = float(row["涨跌幅"])
            vol_ratio  = current_vol / avg_vol_30 if avg_vol_30 > 0 else 0

            results.append({
                "symbol":     code,
                "name":       row["名称"],
                "price":      round(current_price, 3),
                "change_pct": round(change_pct, 2),
                "volume":     int(current_vol),
                "avg_vol_30": int(avg_vol_30),
                "vol_ratio":  round(float(vol_ratio), 2),
                "max_30d":    round(float(max_price_30), 3),
                "min_30d":    round(float(min_price_30), 3),
                "market":     "A股",
            })
            time.sleep(0.2)
        except Exception as e:
            print(f"  ⚠️  A股 {code} 历史数据失败: {e}")

    return results


# ============================================================
# 异动检测
# ============================================================

def check_alerts(stock):
    """检查三个触发条件，返回触发的条件描述列表"""
    triggered = []

    # 条件1：涨跌幅 > ±7%
    if abs(stock["change_pct"]) >= PRICE_CHANGE_THRESHOLD:
        direction = "大涨" if stock["change_pct"] > 0 else "大跌"
        triggered.append(f"📊 条件1 {direction}：{stock['change_pct']:+.2f}%（阈值 ±{PRICE_CHANGE_THRESHOLD}%）")

    # 条件2：价格创近30天新高/新低
    price = stock["price"]
    if price >= stock["max_30d"]:
        triggered.append(f"🏔️ 条件2 价格创近30天新高：当前 {price} ≥ 30日最高 {stock['max_30d']}")
    elif price <= stock["min_30d"]:
        triggered.append(f"🕳️ 条件2 价格创近30天新低：当前 {price} ≤ 30日最低 {stock['min_30d']}")

    # 条件3：成交量异常放大
    if stock["vol_ratio"] >= VOLUME_MULTIPLIER:
        triggered.append(f"🔥 条件3 成交量异常：今日 {stock['volume']:,}，是30日均量的 {stock['vol_ratio']:.1f} 倍")

    return triggered


# ============================================================
# 新闻获取
# ============================================================

def get_stock_news(symbol, market):
    """获取相关新闻标题（最多3条，仅美股/港股）"""
    try:
        if market in ["美股", "港股"]:
            ticker = yf.Ticker(symbol)
            news = ticker.news[:3]
            return [f"- {n['content']['title']}" for n in news
                    if 'content' in n and 'title' in n['content']]
    except:
        pass
    return []


# ============================================================
# 推送（PushPlus）
# ============================================================

def send_to_wechat(title, content):
    """通过 PushPlus 推送到微信"""
    if not PUSHPLUS_TOKEN:
        print("⚠️ 未配置 PUSHPLUS_TOKEN，打印到控制台")
        print(f"\n{'='*50}\n{title}\n{content}\n{'='*50}")
        return

    url = "https://www.pushplus.plus/send"
    payload = {
        "token":    PUSHPLUS_TOKEN,
        "title":    title,
        "content":  content,
        "template": "markdown",
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        data = resp.json()
        if data.get("code") == 200:
            print(f"  ✅ 推送成功：{title}")
        else:
            print(f"  ❌ 推送失败：{data.get('msg')} | {title}")
    except Exception as e:
        print(f"  ❌ 推送异常：{e}")


# ============================================================
# 消息格式化
# ============================================================

def format_alert_message(stock, triggered_conditions, news):
    """格式化单支股票异动消息（Markdown）"""
    name  = stock.get("name", stock["symbol"])
    emoji = "📈" if stock["change_pct"] >= 0 else "📉"

    lines = [
        f"## {emoji} {name}（{stock['symbol']}）",
        f"**市场**：{stock['market']}",
        f"**当前价**：{stock['price']}",
        f"**今日涨跌**：{stock['change_pct']:+.2f}%",
        f"**近30天区间**：{stock['min_30d']} ～ {stock['max_30d']}",
        f"**今日成交量**：{stock['volume']:,}（30日均量：{stock['avg_vol_30']:,} | {stock['vol_ratio']:.1f}倍）",
        f"**推送时间**：{datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "### 触发原因",
    ]
    lines.extend(triggered_conditions)

    if news:
        lines.append("\n### 相关新闻")
        lines.extend(news)

    return "\n".join(lines)


def format_daily_summary(all_stocks):
    """格式化每日涨跌榜汇总"""
    if not all_stocks:
        return "今日无数据"

    sorted_stocks = sorted(all_stocks, key=lambda x: x["change_pct"], reverse=True)
    top_gainers = [s for s in sorted_stocks if s["change_pct"] > 0][:5]
    top_losers  = sorted_stocks[-5:]

    lines = [f"# 📊 股票日报 {datetime.now().strftime('%Y-%m-%d')}"]
    lines.append(f"\n**监控股票数**：{len(all_stocks)} 支\n")

    if top_gainers:
        lines.append("### 🚀 今日涨幅前5")
        for s in top_gainers:
            lines.append(f"- **{s.get('name', s['symbol'])}**（{s['symbol']}）{s['change_pct']:+.2f}% @ {s['price']}")

    if top_losers:
        lines.append("\n### 🔴 今日跌幅前5")
        for s in reversed(top_losers):
            lines.append(f"- **{s.get('name', s['symbol'])}**（{s['symbol']}）{s['change_pct']:+.2f}% @ {s['price']}")

    return "\n".join(lines)


# ============================================================
# 主逻辑
# ============================================================

def run_monitor(mode="alert"):
    """
    mode:
      - "alert"   : 检测异动，满足任一条件即推送
      - "summary" : 推送每日涨跌榜汇总
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 开始获取股票数据...")

    all_stocks = []

    print("正在获取美股 + 港股...")
    all_stocks.extend(get_us_hk_data(US_STOCKS + HK_STOCKS))

    print("正在获取A股...")
    all_stocks.extend(get_a_stock_data(A_STOCKS))

    print(f"成功获取 {len(all_stocks)} 支股票数据")

    if mode == "summary":
        content = format_daily_summary(all_stocks)
        send_to_wechat("📊 每日股票汇总", content)

    elif mode == "alert":
        alert_count = 0
        for stock in all_stocks:
            triggered = check_alerts(stock)
            if not triggered:
                continue
            alert_count += 1
            news    = get_stock_news(stock["symbol"], stock["market"])
            content = format_alert_message(stock, triggered, news)
            name    = stock.get("name", stock["symbol"])
            emoji   = "📈" if stock["change_pct"] >= 0 else "📉"
            title   = f"{emoji} {name}（{stock['symbol']}）异动提醒"
            send_to_wechat(title, content)
            time.sleep(1)

        print(f"共推送 {alert_count} 条异动提醒" if alert_count else "无异动触发")


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "alert"
    run_monitor(mode)
