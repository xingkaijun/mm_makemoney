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
                # 某些接口偶尔报错很正常，不用打印太惊悚的日志
                return None
            time.sleep(delay)
    return None

# --- 3. 选股逻辑 (带调试输出) ---
def check_stock_criteria(symbol, name, price, current_change):
    """
    返回: (ResultDict, ReasonString)
    ResultDict: 成功时返回字典，失败时返回 None
    ReasonString: 失败的具体原因
    """
    try:
        # 1. 资金流检查
        market = "sh" if symbol.startswith("6") else "sz"
        df_flow = call_with_retry(ak.stock_individual_fund_flow, stock=symbol, market=market)
        
        if df_flow is None or df_flow.empty: 
            return None, "获取资金流失败"
        
        recent_flow = df_flow.tail(3)
        flow_sum = recent_flow['主力净流入'].sum()
        if flow_sum <= 0: 
            return None, "3日资金净流出"

        # 2. 历史K线检查
        df_hist = call_with_retry(ak.stock_zh_a_hist_df_cf, symbol=symbol, adjust="qfq", period="daily")
        if df_hist is None or len(df_hist) < 5: 
            return None, "K线数据不足"
        
        recent = df_hist.tail(4)
        today = recent.iloc[-1]
        yesterday = recent.iloc[-2]
        
        # A. 连续3天上涨
        last_3_days = recent.iloc[-3:]
        is_uptrend = all(row['收盘'] >= row['开盘'] for _, row in last_3_days.iterrows())
        if not is_uptrend: 
            return None, "非连续3日阳线"

        # B. 3天累计涨幅 < 15%
        cum_rise = last_3_days['涨跌幅'].sum()
        if cum_rise >= 15: 
            return None, f"涨幅过大({cum_rise:.1f}%)"

        # C. 温和放量
        vol_today = today['成交量']
        vol_yest = yesterday['成交量']
        if vol_today <= vol_yest: return None, "今日缩量"
        if vol_today > (vol_yest * 3.5): return None, "今日爆量(>3.5倍)"

        # 成功
        return {
            "name": name,
            "symbol": symbol,
            "cum_rise": round(cum_rise, 2),
            "price": price,
            "dde": round(flow_sum / 10000000, 2),
            "mkt_cap": "热点成分"
        }, "OK"
    except Exception as e:
        return None, f"异常: {str(e)}"

def get_hot_stocks_pool(top_concepts):
    print(f"🎯 正在从 {len(top_concepts)} 个热点板块中提取成分股...")
    pool = pd.DataFrame()
    for concept_name, _ in top_concepts:
        try:
            df = call_with_retry(ak.stock_board_concept_cons_em, symbol=concept_name)
            if df is not None and not df.empty:
                pool = pd.concat([pool, df])
            time.sleep(0.5)
        except: continue
            
    if pool.empty: return []
    pool = pool.drop_duplicates(subset=['代码'])
    pool = pool[(pool['涨跌幅'] > 0) & (pool['涨跌幅'] < 8) & (~pool['名称'].str.contains('ST|退'))]
    
    print(f"✅ 提取并初筛完成，共锁定 {len(pool)} 只热点潜力股")
    return pool

def run_strict_selection(top_concepts):
    selected_stocks = []
    candidates = get_hot_stocks_pool(top_concepts)
    
    if len(candidates) == 0:
        print("❌ 未能获取热点股池")
        return []

    print("🔍 开始深度扫描热点股 (显示前50条日志)...")
    
    # 限制扫描数量，防止超时
    check_list = candidates.head(80)
    total = len(check_list)

    for i, (_, row) in enumerate(check_list.iterrows()):
        try:
            # 这里的 print 是关键，让你知道它在动
            log_prefix = f"[{i+1}/{total}] {row['名称']}: "
            
            res, reason = check_stock_criteria(row['代码'], row['名称'], row['最新价'], row['涨跌幅'])
            
            if res:
                selected_stocks.append(res)
                print(f"{log_prefix}🌟 命中！")
            else:
                # 这里的日志会告诉你为什么没选上
                print(f"{log_prefix}淘汰 ({reason})")
                
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
        stock_rows = "<tr><td colspan='4' style='text-align:center;color:#999;padding:20px'>今日热点板块中无符合严选条件的个股</td></tr>"

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
            .container {{ max_width: 600px; margin: 0 auto; background: white; padding: 20px; border-radius: 12px; }}
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

    top_concepts = []
    try:
        df = call_with_retry(ak.stock_board_concept_name_em)
        if df is not None:
            df = df.sort_values('涨跌幅', ascending=False)
            df = df[~df['板块名称'].str.contains('涨停|连板')]
            top_concepts = list(zip(df.head(10)['板块名称'], df.head(10)['涨跌幅']))
    except:
        print("板块获取失败")

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

    # 执行选股
    picks = run_strict_selection(top_concepts)

    if not os.path.exists(ARCHIVE_DIR): os.makedirs(ARCHIVE_DIR)
    html = generate_html_report(today_str, new_concepts, top_concepts, picks)
    with open(f"{ARCHIVE_DIR}/{today_str}.html", 'w', encoding='utf-8') as f: f.write(html)
    with open(HTML_FILE, 'w', encoding='utf-8') as f: f.write(html)

    # 发送 Telegram (确保无论有没有结果都发)
    msg = [f"📊 *A股复盘* ({today_str})"]
    if new_concepts: msg.append(f"🔥 *新风口*: {', '.join(new_concepts)}")
    
    if picks:
        msg.append(f"\n💎 *热点严选 {len(picks)} 只*")
        for s in picks[:3]: msg.append(f"• {s['name']} (流入:{s['dde']}千万)")
        if len(picks) > 3: msg.append(f"...更多见网页")
    else:
        msg.append("\n🍵 热点板块内无严选个股 (条件可能太严)")

    if PAGE_URL_PREFIX: msg.append(f"\n🔗 [查看完整日报]({PAGE_URL_PREFIX})")
    
    send_telegram_message("\n".join(msg))

    if top_concepts:
        history_data[today_str] = [x[0] for x in top_concepts]
        with open(HISTORY_FILE, 'w') as f: json.dump(history_data, f)

if __name__ == "__main__":
    run_task()
