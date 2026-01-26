import akshare as ak
import pandas as pd
import os
import json
import requests
import time
import glob
from datetime import datetime, timedelta
from collections import Counter

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

def call_with_retry(func, max_retries=3, delay=1, *args, **kwargs):
    for i in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == max_retries - 1: return None
            time.sleep(delay)
    return None

# --- 3. 选股逻辑 (返回具体淘汰原因) ---
def check_stock_criteria(symbol, name, price, concept_name):
    try:
        # 1. 获取K线
        df_hist = call_with_retry(ak.stock_zh_a_hist_df_cf, symbol=symbol, adjust="qfq", period="daily")
        if df_hist is None or len(df_hist) < 5: return None, "数据缺失"
        
        recent = df_hist.tail(4)
        today = recent.iloc[-1]
        yesterday = recent.iloc[-2]
        
        # --- 关卡 1: 形态 (3连阳) ---
        last_3_days = recent.iloc[-3:]
        is_uptrend = all(row['收盘'] >= row['开盘'] for _, row in last_3_days.iterrows())
        if not is_uptrend: return None, "❌ 形态(非3连阳)"

        # --- 关卡 2: 涨幅 (拒绝暴涨) ---
        cum_rise = last_3_days['涨跌幅'].sum()
        if cum_rise >= 20: return None, f"❌ 涨幅(过大{cum_rise:.1f}%)"
        if cum_rise <= 0: return None, "❌ 涨幅(累积下跌)"

        # --- 关卡 3: 量能 (温和放量) ---
        vol_today = today['成交量']
        vol_yest = yesterday['成交量']
        if vol_yest == 0: return None, "❌ 停牌"
        
        vol_ratio = vol_today / vol_yest
        
        if vol_ratio <= 1.0: return None, f"❌ 量能(缩量{vol_ratio:.1f})"
        if vol_ratio > 3.0: return None, f"❌ 量能(爆量{vol_ratio:.1f})"

        # 全部通关
        return {
            "name": name,
            "symbol": symbol,
            "concept": concept_name,
            "cum_rise": round(cum_rise, 2),
            "price": price,
            "vol_ratio": round(vol_ratio, 2)
        }, "✅ 晋级"
    except Exception as e:
        return None, f"⚠️ 异常({str(e)})"

def get_hot_stocks_pool(top_concepts, new_concepts):
    print(f"🎯 正在提取成分股...")
    # 按照是否为新概念排序，确保去重时优先保留新概念标签
    sorted_concepts = sorted(top_concepts, key=lambda x: x[0] in new_concepts, reverse=True)
    
    all_dfs = []
    for concept_name, _ in sorted_concepts:
        try:
            df = call_with_retry(ak.stock_board_concept_cons_em, symbol=concept_name)
            if df is not None and not df.empty:
                df['所属板块'] = concept_name
                all_dfs.append(df)
            time.sleep(0.3)
        except: continue
            
    if not all_dfs: return []
    pool = pd.concat(all_dfs)
    # 去重
    pool = pool.drop_duplicates(subset=['代码'], keep='first')
    # 初筛: 涨跌幅 0~9.8%, 非ST
    pool = pool[(pool['涨跌幅'] > 0) & (pool['涨跌幅'] < 9.8) & (~pool['名称'].str.contains('ST|退'))]
    
    print(f"✅ 锁定 {len(pool)} 只潜力股 (已过滤涨停/跌绿/ST)")
    return pool

def run_strict_selection(top_concepts, new_concepts):
    selected_stocks = []
    rejection_stats = Counter() # 统计淘汰原因
    
    candidates = get_hot_stocks_pool(top_concepts, new_concepts)
    
    if len(candidates) == 0:
        print("❌ 热点股池为空")
        return []

    # 扫描前 100 只
    check_list = candidates.head(100)
    total = len(check_list)
    
    print("\n" + "="*50)
    print(f"🔍 开始深度扫描 (目标: {total} 只)")
    print("="*50)
    
    for i, (_, row) in enumerate(check_list.iterrows()):
        try:
            # 执行检查
            res, reason = check_stock_criteria(row['代码'], row['名称'], row['最新价'], row['所属板块'])
            
            # 记录统计
            rejection_stats[reason] += 1
            
            # 打印进度条
            status_icon = "✨" if res else "  "
            print(f"[{i+1}/{total}] {row['名称']}\t -> {reason} {status_icon}")
            
            if res:
                selected_stocks.append(res)
            
            time.sleep(0.1)
        except: continue

    # --- 打印淘汰漏斗报告 ---
    print("\n" + "="*50)
    print("📊 淘汰原因统计报告 (Funnel Report)")
    print("="*50)
    if selected_stocks:
        print(f"🎉 成功选出: {len(selected_stocks)} 只")
    else:
        print(f"😭 成功选出: 0 只 (全军覆没)")
    
    print("-" * 30)
    for reason, count in rejection_stats.most_common():
        # 简单的ASCII条形图
        bar_len = int(count / total * 20) if total > 0 else 0
        bar = "█" * bar_len
        print(f"{reason:<15} : {count:>3} {bar}")
    print("="*50 + "\n")
            
    return selected_stocks

# --- 4. 网页生成 ---
def generate_html_report(today_str, new_concepts, top_concepts, picks):
    stock_rows = ""
    if picks:
        picks_sorted = sorted(picks, key=lambda x: x['vol_ratio'], reverse=True)
        for s in picks_sorted:
            is_new = s['concept'] in new_concepts
            concept_class = "red-text" if is_new else "gray-text"
            concept_icon = "🔥" if is_new else ""
            stock_rows += f"""
            <tr>
                <td><div class="stock-name">{s['name']}</div><div class="stock-code">{s['symbol']}</div></td>
                <td><span class="{concept_class}">{concept_icon}{s['concept']}</span></td>
                <td class="red-text">+{s['cum_rise']}%</td>
                <td>{s['vol_ratio']}</td>
            </tr>"""
    else:
        stock_rows = "<tr><td colspan='4' style='text-align:center;color:#999;padding:30px'>今日无个股符合条件<br><small>请查看GitHub Actions日志获取淘汰详情</small></td></tr>"

    concept_html = "".join([f'<span class="tag">{n}</span>' for n in new_concepts]) if new_concepts else '<span style="color:#999;font-size:12px">无新面孔</span>'
    top_html = "".join([f'<span class="tag tag-gray">{n}</span>' for n, _ in top_concepts])

    # 历史链接逻辑
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
            td {{ padding: 10px 0; border-bottom: 1px solid #f5f5f5; vertical-align: middle; }}
            .red-text {{ color: #e74c3c; font-weight: bold; }}
            .gray-text {{ color: #666; }}
            .stock-name {{ font-weight: bold; font-size: 15px; }}
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
            <h2>💎 热点严选</h2>
            <p style="font-size:12px;color:#999">条件: Top板块 | 3连阳<20% | 温和放量(1-3倍)</p>
            <table>
                <thead><tr><th width="30%">股票</th><th width="35%">概念板块</th><th width="20%">3日涨幅</th><th width="15%">量比</th></tr></thead>
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
    except: pass

    history_data = {}
    if os.path.exists(HISTORY_FILE):
        # --- 修复后的代码块 ---
        try:
            with open(HISTORY_FILE, 'r') as f:
                history_data = json.load(f)
        except:
            pass
        # ---------------------
    
    past_set = set()
    cutoff = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    for d, names in history_data.items():
        if d > cutoff and d != today_str: past_set.update(names)
    
    new_concepts = [n for n, r in top_concepts if n not in past_set]

    picks = run_strict_selection(top_concepts, new_concepts)

    if not os.path.exists(ARCHIVE_DIR): os.makedirs(ARCHIVE_DIR)
    html = generate_html_report(today_str, new_concepts, top_concepts, picks)
    with open(f"{ARCHIVE_DIR}/{today_str}.html", 'w', encoding='utf-8') as f: f.write(html)
    with open(HTML_FILE, 'w', encoding='utf-8') as f: f.write(html)

    # 发送 Telegram
    msg = [f"📊 *A股复盘* ({today_str})"]
    if new_concepts: msg.append(f"🔥 *新风口*: {', '.join(new_concepts)}")
    
    if picks:
        picks_sorted = sorted(picks, key=lambda x: x['vol_ratio'], reverse=True)
        top_picks = picks_sorted[:10]
        msg.append(f"\n💎 *热点严选 Top {len(top_picks)}*")
        for s in top_picks:
            is_new = s['concept'] in new_concepts
            concept_str = f"🔥*{s['concept']}*" if is_new else f"({s['concept']})"
            msg.append(f"• {s['name']} {concept_str}")
            msg.append(f"   量比:{s['vol_ratio']} | 涨幅:+{s['cum_rise']}%")
        if len(picks) > 10: msg.append(f"...更多见网页")
    else:
        msg.append("\n🍵 今日无严选个股")

    if PAGE_URL_PREFIX: msg.append(f"\n🔗 [点击查看网页报表]({PAGE_URL_PREFIX})")
    
    send_telegram_message("\n".join(msg))

    if top_concepts:
        history_data[today_str] = [x[0] for x in top_concepts]
        with open(HISTORY_FILE, 'w') as f: json.dump(history_data, f)

if __name__ == "__main__":
    run_task()
