import akshare as ak
import pandas as pd
import os
import json
import requests
import time
import glob
import random
from datetime import datetime, timedelta

# --- 1. 配置项 ---
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
TG_CHAT_IDS = os.environ.get("TG_CHAT_IDS", "").split(",")
PAGE_URL_PREFIX = os.environ.get("PAGE_URL_PREFIX", "")

HISTORY_FILE = 'concept_history.json'
ARCHIVE_DIR = 'archive'
HTML_FILE = 'index.html'

# --- 2. 基础工具 ---
def send_telegram_message(message):
    if not TG_BOT_TOKEN or not TG_CHAT_IDS: 
        print("❌ 未检测到 TG 配置")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for chat_id in TG_CHAT_IDS:
        chat_id = chat_id.strip()
        if not chat_id: continue
        try:
            if len(message) > 4000: message = message[:4000] + "\n...(截断)"
            payload = {'chat_id': chat_id, 'text': message, 'parse_mode': 'Markdown', 'disable_web_page_preview': True}
            requests.post(url, json=payload)
        except Exception as e:
            print(f"❌ 推送失败: {e}")

def call_with_retry(func, max_retries=3, delay=2, *args, **kwargs):
    """通用重试装饰器"""
    for i in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == max_retries - 1:
                print(f"⚠️ 接口失败 [{func.__name__}]: {e}")
                return None
            time.sleep(delay * (2 ** i))
    return None

# --- 3. 选股逻辑 ---
def check_stock_criteria(symbol, name, price, current_change):
    """
    对单只股票进行深度扫描 (K线 + 资金流)
    """
    try:
        # 1. 资金流检查 (最耗时，先查这个，如果主力没流进就直接pass，省去查K线的时间)
        # 注意：这里我们用个股流向接口，而不是全市场的大表
        market = "sh" if symbol.startswith("6") else "sz"
        df_flow = call_with_retry(ak.stock_individual_fund_flow, stock=symbol, market=market)
        
        if df_flow is None or df_flow.empty: return None
        
        # 检查3日主力净流入
        recent_flow = df_flow.tail(3)
        flow_sum = recent_flow['主力净流入'].sum()
        if flow_sum <= 0: return None # 资金流出，淘汰

        # 计算 DDE (近似值: 净流入/大致流通盘)
        # 这里的 DDE 计算比较粗略，因为没有实时流通市值，但只要资金是正的就行
        # 我们用流入力度代替 DDE 展示
        
        # 2. 历史K线检查
        df_hist = call_with_retry(ak.stock_zh_a_hist_df_cf, symbol=symbol, adjust="qfq", period="daily")
        if df_hist is None or len(df_hist) < 5: return None
        
        recent = df_hist.tail(4)
        today = recent.iloc[-1]
        yesterday = recent.iloc[-2]
        
        # A. 连续3天上涨 (收盘价抬升 且 为阳线)
        last_3_days = recent.iloc[-3:]
        is_uptrend = all(row['收盘'] >= row['开盘'] for _, row in last_3_days.iterrows())
        if not is_uptrend: return None

        # B. 3天累计涨幅 < 15% (稍微放宽一点，避免漏掉强势股)
        cum_rise = last_3_days['涨跌幅'].sum()
        if cum_rise >= 15: return None

        # C. 温和放量
        vol_today = today['成交量']
        vol_yest = yesterday['成交量']
        if vol_today <= vol_yest: return None 
        if vol_today > (vol_yest * 3.5): return None # 放宽倍数

        # 通过所有检查
        return {
            "name": name,
            "symbol": symbol,
            "cum_rise": round(cum_rise, 2),
            "price": price,
            "dde": round(flow_sum / 10000000, 2), # 这里展示主力净流入(千万)
            "mkt_cap": "热点成分" # 暂时无法获取实时市值，用标签代替
        }
    except:
        return None

def get_hot_stocks_pool(top_concepts):
    """
    策略核心：只获取【Top10概念板块】里的成分股
    """
    print(f"🎯 正在从 {len(top_concepts)} 个热点板块中提取成分股...")
    pool = pd.DataFrame()
    
    for concept_name, _ in top_concepts:
        try:
            # 获取该板块的成分股
            df = call_with_retry(ak.stock_board_concept_cons_em, symbol=concept_name)
            if df is not None and not df.empty:
                pool = pd.concat([pool, df])
            time.sleep(1) # 稍微歇一下
        except:
            continue
            
    if pool.empty: return []

    # 数据清洗
    # 不同的接口返回列名可能不同，通常是 '代码', '名称', '最新价', '涨跌幅'
    pool = pool.drop_duplicates(subset=['代码']) # 去重
    
    # 初筛：只要涨幅 > 0 且 < 8% 的 (未涨停，且是红盘)
    pool = pool[
        (pool['涨跌幅'] > 0) & 
        (pool['涨跌幅'] < 8) & 
        (~pool['名称'].str.contains('ST|退'))
    ]
    
    print(f"✅ 提取并初筛完成，共锁定 {len(pool)} 只热点潜力股")
    return pool

def run_strict_selection(top_concepts):
    """执行扫描"""
    selected_stocks = []
    
    # 1. 获取热点池 (大大缩小范围)
    candidates = get_hot_stocks_pool(top_concepts)
    
    if len(candidates) == 0:
        print("❌ 未能获取热点股池")
        return []

    print("🔍 开始深度扫描热点股...")
    
    # 2. 限制扫描数量 (为了防止超时，只看前 80 个)
    # 优先看涨幅适中的
    check_list = candidates.head(80)

    for _, row in check_list.iterrows():
        try:
            res = check_stock_criteria(row['代码'], row['名称'], row['最新价'], row['涨跌幅'])
            if res:
                selected_stocks.append(res)
                print(f"🌟 命中: {row['名称']}")
            time.sleep(0.5)
        except:
            continue
            
    return selected_stocks

# --- 4. 网页生成 ---
def generate_html_report(today_str, new_concepts, top_concepts, picks):
    stock_rows = ""
    if picks:
        for s in picks:
            stock_rows += f"""
            <tr>
                <td><div class="stock-name">{s['name']}</div><div class="stock-code">{s['symbol']}</div></td>
                <td class="red-text">+{s['cum_rise']}%</td>
                <td class="red-text">{s['dde']}</td>
                <td>{s['mkt_cap']}</td>
            </tr>"""
    else:
        stock_rows = "<tr><td colspan='4' style='text-align:center;color:#999'>今日热点板块中无符合严选条件的个股</td></tr>"

    concept_html = "".join([f'<span class="tag">{n}</span>' for n in new_concepts]) if new_concepts else '<span style="color:#999;font-size:12px">无新面孔</span>'
    top_html = "".join([f'<span class="tag tag-gray">{n}</span>' for n, _ in top_concepts])

    history_links_html = ""
    if os.path.exists(ARCHIVE_DIR):
        files = sorted(glob.glob(f"{ARCHIVE_DIR}/*.html"), reverse=True)[:7]
        if files:
            history_links_html = "<h3>📅 历史回顾</h3><div class='history-list'>"
            for f_path in files:
                fname = os.path.basename(f_path) 
                date_label = fname.replace(".html", "")
                history_links_html += f"<a href='{ARCHIVE_DIR}/{fname}' class='history-link'>{date_label}</a>"
            history_links_html += "</div>"

    html = f"""
    <!DOCTYPE html>
    <html lang="zh">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>A股复盘 {today_str}</title>
        <style>
            body {{ font-family: -apple-system, sans-serif; background: #f0f2f5; padding: 15px; margin: 0; }}
            .container {{ max_width: 600px; margin: 0 auto; background: white; padding: 20px; border-radius: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }}
            h1 {{ font-size: 20px; text-align: center; color: #333; }}
            h2 {{ font-size: 16px; border-left: 4px solid #e74c3c; padding-left: 10px; margin-top: 25px; }}
            .tag {{ display: inline-block; background: #ffe2e2; color: #e74c3c; padding: 4px 8px; border-radius: 4px; font-size: 12px; margin: 0 5px 5px 0; }}
            .tag-gray {{ background: #f4f4f5; color: #909399; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }}
            th {{ text-align: left; color: #909399; font-weight: normal; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
            td {{ padding: 10px 0; border-bottom: 1px solid #f5f5f5; }}
            .red-text {{ color: #f56c6c; font-weight: bold; }}
            .stock-name {{ font-weight: bold; }}
            .stock-code {{ font-size: 12px; color: #999; }}
            .history-list {{ display: flex; gap: 8px; flex-wrap: wrap; }}
            .history-link {{ text-decoration: none; font-size: 12px; color: #666; background: #eee; padding: 4px 8px; border-radius: 4px; }}
            .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #ccc; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📅 A股复盘日报 <small>{today_str}</small></h1>
            <h2>🔥 新风口</h2>
            <div>{concept_html}</div>
            <h2>📊 领涨板块</h2>
            <div>{top_html}</div>
            <h2>💎 热点严选 (Top板块成分股)</h2>
            <p style="font-size:12px;color:#999">筛选: 3连阳<15% | 温和放量 | 3日净流入</p>
            <table>
                <thead><tr><th>股票</th><th>3日涨幅</th><th>主力净流入</th><th>备注</th></tr></thead>
                <tbody>{stock_rows}</tbody>
            </table>
            {history_links_html}
            <div class="footer">Data by AkShare | Designed by Kevin Xing</div>
        </div>
    </body>
    </html>
    """
    return html

# --- 5. 主程序 ---
def run_task():
    today_str = datetime.now().strftime('%Y-%m-%d')
    print(f"🚀 启动: {today_str}")

    # A. 获取板块
    top_concepts = []
    try:
        df = call_with_retry(ak.stock_board_concept_name_em)
        if df is not None:
            df = df.sort_values('涨跌幅', ascending=False)
            df = df[~df['板块名称'].str.contains('涨停|连板')]
            top_concepts = list(zip(df.head(10)['板块名称'], df.head(10)['涨跌幅']))
    except:
        print("板块获取失败")

    # B. 历史对比
    history_data = {}
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f: history_data = json.load(f)
        except: pass
    
    past_set = set()
    cutoff = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    for d, names in history_data.items():
        if d > cutoff and d != today_str: past_set.update(names)
    
    new_concepts = [n for n, r in top_concepts if n not in past_set]

    # C. 执行【热点聚焦】选股
    # 注意：这里我们把 top_concepts 传进去，只扫描这些板块里的股票
    picks = run_strict_selection(top_concepts)

    # D. 归档
    if not os.path.exists(ARCHIVE_DIR): os.makedirs(ARCHIVE_DIR)
    html = generate_html_report(today_str, new_concepts, top_concepts, picks)
    with open(f"{ARCHIVE_DIR}/{today_str}.html", 'w', encoding='utf-8') as f: f.write(html)
    with open(HTML_FILE, 'w', encoding='utf-8') as f: f.write(html)

    # E. 推送
    msg = [f"📊 *A股复盘* ({today_str})"]
    if new_concepts: msg.append(f"🔥 *新风口*: {', '.join(new_concepts)}")
    
    if picks:
        msg.append(f"\n💎 *热点严选 {len(picks)} 只*")
        for s in picks[:3]: msg.append(f"• {s['name']} (流入:{s['dde']}千万)")
    else:
        msg.append("\n🍵 今日热点板块内无严选个股")

    if PAGE_URL_PREFIX: msg.append(f"\n🔗 [查看完整日报]({PAGE_URL_PREFIX})")
    
    send_telegram_message("\n".join(msg))

    # F. 保存
    if top_concepts:
        history_data[today_str] = [x[0] for x in top_concepts]
        with open(HISTORY_FILE, 'w') as f: json.dump(history_data, f)

if __name__ == "__main__":
    run_task()
