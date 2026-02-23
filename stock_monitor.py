"""
股票异动监控脚本 v2
支持美股、港股、A股

运行模式：
  intraday  - 盘中实时监控（每5分钟）：当日实时价 vs 昨日收盘 > ±4%
  close_a   - A股收盘后30分钟：条件2（30天新高/低）+ 条件3（成交量异常）
  close_hk  - 港股收盘后30分钟：条件2 + 条件3
  close_us  - 美股收盘后30分钟：条件2 + 条件3
"""

import os
import sys
import requests
import yfinance as yf
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import time

# ============================================================
# 配置区域
# ============================================================

PUSHPLUS_TOKEN = os.environ.get("PUSHPLUS_TOKEN", "")

PRICE_CHANGE_THRESHOLD = 4.0  # 盘中涨跌幅阈值（%）
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
    """并发拉取港股实时价（逐支，避免批量接口慢速分页）"""
    def _fetch(symbol):
        try:
            fi = yf.Ticker(symbol).fast_info
            current    = fi.last_price
            prev_close = fi.previous_close
            if not current or not prev_close or prev_close == 0:
                return None
            # 从 akshare 格式映射名称（仅在有数据时）
            change_pct = (current - prev_close) / prev_close * 100
            return {
                "symbol":     symbol,
                "name":       symbol.replace(".HK", ""),
                "price":      round(float(current), 3),
                "prev_close": round(float(prev_close), 3),
                "change_pct": round(float(change_pct), 2),
                "market":     "港股",
            }
        except Exception as e:
            print(f"  ⚠️  {symbol} 港股实时数据获取失败: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch, s): s for s in HK_STOCKS}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    return results


def get_intraday_a():
    """并发拉取A股实时价（逐支，避免批量接口慢速分页）"""
    def _fetch(code):
        try:
            # akshare 单支实时行情：返回 DataFrame，取最新一行
            df = ak.stock_zh_a_spot_em()
            row = df[df["代码"] == code]
            if row.empty:
                return None
            row = row.iloc[0]
            prev_close = float(row["昨收"])
            current    = float(row["最新价"])
            if prev_close == 0:
                return None
            return {
                "symbol":     code,
                "name":       row["名称"],
                "price":      round(current, 3),
                "prev_close": round(prev_close, 3),
                "change_pct": round(float(row["涨跌幅"]), 2),
                "market":     "A股",
            }
        except Exception as e:
            print(f"  ⚠️  A股 {code} 实时数据获取失败: {e}")
            return None

    # A股数量少(15支)，先批量拉一次再过滤（akshare无单支实时接口）
    results = []
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
    except Exception as e:
        print(f"A股实时行情获取失败: {e}")
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
    else:
        print(f"未知模式：{mode}，可选：intraday / close_a / close_hk / close_us")
        sys.exit(1)
