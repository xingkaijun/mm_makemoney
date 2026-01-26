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

# --- 2. 基础工具函数 ---
def send_telegram_message(message):
    """发送消息到 Telegram"""
    if not TG_BOT_TOKEN or not TG_CHAT_IDS: 
        print("❌ 未检测到 Telegram 配置，跳过推送")
        return
        
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for chat_id in TG_CHAT_IDS:
        chat_id = chat_id.strip()
        if not chat_id: continue
        try:
            if len(message) > 4000: message = message[:4000] + "\n...(内容过长截断)"
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': True
            }
            requests.post(url, json=payload)
        except Exception as e:
            print(f"❌ 推送失败 ({chat_id}): {e}")

def call_with_retry(func, max_retries=3, delay=2, *args, **kwargs):
    """网络请求重试装饰器"""
    for i in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == max_retries - 1:
                print(f"⚠️ 接口调用最终失败 [{func.__name__}]: {e}")
                return None
            wait_time = delay * (2 ** i)
            print(f"🔄 网络波动，正在第 {i+1} 次重试 (等待 {wait_time}s)...")
            time.sleep(wait_time)
    return None

# --- 3. 选股核心逻辑 ---
def check_stock_criteria(symbol, name, dde_now):
    try:
        # 获取个股历史行情
        df = call_with_retry(ak.stock_zh_a_hist_df_cf, symbol=symbol, adjust="qfq", period="daily")
        
        if df is None or len(df) < 5: return None
        
        recent = df.tail(4) 
        today = recent.iloc[-1]
        yesterday = recent.iloc[-2]
        
        # A. 连续3天上涨
        last_3_days = recent.iloc[-3:]
        is_all_up = all((row['收盘'] >= row['开盘']) and (row['涨跌幅'] > 0) for _, row in last_3_days.iterrows())
        if not is_all_up: return None

        # B. 3天累计涨幅 < 10%
        cum_rise = last_3_days['涨跌幅'].sum()
        if cum_rise >= 10: return None

        # C. 温和放量 (放宽到3倍)
        vol_today = today['成交量']
        vol_yest = yesterday['成交量']
        if vol_today <= vol_yest: return None 
        if vol_today > (vol_yest * 3.0): return None 

        # D. 3天资金净流入 (查个股流向)
        try:
            market = "sh" if symbol.startswith("6") else "sz"
            df_flow = call_with_retry(ak.stock_individual_fund_flow, stock=symbol, market=market)
            if df_flow is not None:
                flow_sum = df_flow.tail(3)['主力净流入'].sum()
                if flow_sum <= 0: return None
            else:
                return None
        except:
            return None 

        return {
            "name": name,
            "symbol": symbol,
            "cum_rise": round(cum_rise, 2),
            "price": today['收盘'],
            "dde": round(dde_now, 2),
            "mkt_cap": 0 
        }
    except Exception as e:
        return None

def run_strict_selection():
    print("🔍 开始执行严选扫描 (正在合并行情与资金流数据)...")
    selected_stocks = []
    
    try:
        # 1. 获取全市场行情 (包含市值、价格)
        df_spot = call_with_retry(ak.stock_zh_a_spot_em, max_retries=5)
        if df_spot is None:
            print("❌ 无法获取行情数据")
            return []

        # 2. 获取全市场资金流向 (包含主力净流入)
        # 注意：这里可能比较慢，也容易断，必须重试
        df_flow = call_with_retry(ak.stock_individual_fund_flow_rank, indicator="今日", max_retries=5)
        if df_flow is None:
            print("❌ 无法获取资金流数据")
            return []
        
        # 3. 数据清洗与合并
        # df_flow 的列名通常是 "主力净流入-净额"，我们需要重命名方便处理
        # 先找一下这一列叫什么，防止名字变动
        flow_col = None
        for col in df_flow.columns:
            if "主力净流入" in col and "净额" in col:
                flow_col = col
                break
        
        if not flow_col:
            print("❌ 在资金流数据中找不到 '主力净流入' 列")
            return []

        # 重命名并只保留需要的列
        df_flow = df_flow[['代码', flow_col]].rename(columns={flow_col: '主力净流入'})
        
        # 合并两个表 (Inner Join，只保留两者都有的数据)
        # df_spot 和 df_flow 都有 '代码' 列
        df_merge = pd.merge(df_spot, df_flow, on='代码', how='inner')
        
        print(f"✅ 数据合并完成，共 {len(df_merge)} 只股票，开始筛选...")

        # 4. 初筛逻辑
        # 排除 ST, 排除无数据
        mask = (
            (~df_merge['名称'].str.contains('ST|退')) & 
            (df_merge['主力净流入'].notnull()) & 
            (df_merge['流通市值'] > 0)
        )
        pool = df_merge[mask].copy()
        
        # 计算 DDE: 主力净流入 / 流通市值 * 100
        pool['DDE'] = (pool['主力净流入'] / pool['流通市值']) * 100
        
        # 筛选: DDE > 0.5, 涨幅 > 0, 涨幅 < 8
        pool = pool[
            (pool['DDE'] > 0.5) & 
            (pool['涨跌幅'] > 0) & 
            (pool['涨跌幅'] < 8)
        ]
        
        # 按市值排序
        pool = pool.sort_values(by='总市值', ascending=True)
        
        # 取前 60 个进入深度扫描
        check_list = pool.head(60)
        print(f"✅ 初筛通过 {len(check_list)} 只，进入深度扫描...")

        # 5. 深度扫描
        for _, row in check_list.iterrows():
            res = check_stock_criteria(row['代码'], row['名称'], row['DDE'])
            if res:
                res['mkt_cap'] = round(row['总市值'] / 100000000, 2)
                selected_stocks.append(res)
                print(f"🌟 命中: {row['名称']}")
            
            # 随机延时防封
            time.sleep(random.uniform(0.5, 0.8))
            
    except Exception as e:
        print(f"❌ 选股逻辑严重错误: {e}")
        # 打印一下出错时的列名，方便调试
        try: print(f"DEBUG: Spot Cols: {df_spot.columns[:5]}")
        except: pass
        
    return selected_stocks

# --- 4. 网页生成 (含名字) ---
def generate_html_report(today_str, new_concepts, top_concepts, picks):
    if picks:
        stock_rows = ""
        for s in picks:
            stock_rows += f"""
            <tr>
                <td><div class="stock-name">{s['name']}</div><div class="stock-code">{s['symbol']}</div></td>
                <td class="red-text">+{s['cum_rise']}%</td>
                <td class="red-text">{s['dde']}</td>
                <td>{s['mkt_cap']}亿</td>
            </tr>"""
    else:
        stock_rows = "<tr><td colspan='4' style='text-align:center;padding:20px;color:#999'>今日无符合严选条件的个股</td></tr>"

    concept_html = "".join([f'<span class="tag">{n}</span>' for n in new_concepts]) if new_concepts else '<span style="color:#999;font-size:12px">今日无新面孔</span>'
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

    html_content = f"""
    <!DOCTYPE html>
    <html lang="zh">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>A股复盘日报 {today_str}</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f0f2f5; margin: 0; padding: 15px; color: #333; }}
            .container {{ max_width: 600px; margin: 0 auto; background: white; padding: 20px; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); }}
            h1 {{ font-size: 22px; margin: 0 0 20px 0; text-align: center; color: #1a1a1a; }}
            h2 {{ font-size: 16px; margin: 25px 0 10px 0; padding-left: 10px; border-left: 4px solid #e74c3c; color: #2c3e50; font-weight: 600; }}
            h3 {{ font-size: 14px; margin-top: 30px; color: #7f8c8d; border-top: 1px solid #eee; padding-top: 15px; }}
            .tag {{ display: inline-block; background: #ffe2e2; color: #e74c3c; padding: 4px 10px; border-radius: 15px; font-size: 12px; margin: 0 6px 8px 0; font-weight: 500; }}
            .tag-gray {{ background: #f0f2f5; color: #606266; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
            th {{ text-align: left; color: #909399; font-size: 12px; font-weight: normal; padding-bottom: 8px; border-bottom: 1px solid #ebeef5; }}
            td {{ padding: 12px 0; border-bottom: 1px solid #f5f7fa; vertical-align: middle; }}
            .stock-name {{ font-weight: 600; font-size: 15px; color: #303133; }}
            .stock-code {{ font-size: 12px; color: #909399; margin-top: 2px; }}
            .red-text {{ color: #f56c6c; font-weight: 600; }}
            .history-list {{ display: flex; flex-wrap: wrap; gap: 10px; }}
            .history-link {{ text-decoration: none; background: #fff; border: 1px solid #dcdfe6; color: #606266; padding: 5px 12px; border-radius: 4px; font-size: 13px; transition: all 0.2s; }}
            .history-link:hover {{ border-color: #409eff; color: #409eff; }}
            .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #c0c4cc; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📅 A股复盘日报 <br><small style="font-size:14px;color:#909399">{today_str}</small></h1>
            <h2>🔥 概念新风口 (5日新进)</h2>
            <div>{concept_html}</div>
            <h2>📊 今日涨幅 Top 10</h2>
            <div>{top_html}</div>
            <h2>💎 主力潜伏严选</h2>
            <p style="font-size:12px;color:#909399;margin:5px 0">筛选: 3连阳<10% | 温和放量 | 3日净流入 | DDE>0.5</p>
            <table>
                <thead>
                    <tr><th width="35%">股票</th><th width="20%">3日涨幅</th><th width="15%">DDE</th><th width="30%">市值</th></tr>
                </thead>
                <tbody>{stock_rows}</tbody>
            </table>
            {history_links_html}
            <div class="footer">Data by AkShare | Designed by Kevin Xing</div>
        </div>
    </body>
    </html>
    """
    return html_content

# --- 5. 主任务流程 ---
def run_task():
    today_str = datetime.now().strftime('%Y-%m-%d')
    print(f"🚀 任务启动: {today_str}")

    # A. 获取板块数据 (含过滤逻辑)
    top_concepts = []
    try:
        df_concept = call_with_retry(ak.stock_board_concept_name_em)
        if df_concept is not None:
            df_concept = df_concept.sort_values('涨跌幅', ascending=False)
            ignore_keywords = '涨停|连板'
            df_concept = df_concept[~df_concept['板块名称'].str.contains(ignore_keywords)]
            top_concepts = list(zip(df_concept.head(10)['板块名称'], df_concept.head(10)['涨跌幅']))
    except Exception as e:
        print(f"板块数据获取失败: {e}")

    # B. 历史对比
    history_data = {}
    if os.path.exists(HISTORY_FILE):
        try: with open(HISTORY_FILE, 'r') as f: history_data = json.load(f)
        except: pass
    
    past_set = set()
    cutoff_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    for d, names in history_data.items():
        if d > cutoff_date and d != today_str:
            past_set.update(names)
    
    new_concepts = [n for n, r in top_concepts if n not in past_set]

    # C. 执行选股
    picks = run_strict_selection()

    # D. 归档处理
    if not os.path.exists(ARCHIVE_DIR): os.makedirs(ARCHIVE_DIR)
    html_content = generate_html_report(today_str, new_concepts, top_concepts, picks)
    
    with open(f"{ARCHIVE_DIR}/{today_str}.html", 'w', encoding='utf-8') as f: f.write(html_content)
    with open(HTML_FILE, 'w', encoding='utf-8') as f: f.write(html_content)

    # E. 推送消息
    msg_lines = [f"📊 *A股复盘日报* ({today_str})"]
    if new_concepts: msg_lines.append(f"🔥 *新风口*: {', '.join(new_concepts)}")
    else: msg_lines.append("👀 无新风口，老热点轮动")
    
    if picks:
        msg_lines.append(f"\n💎 *严选出 {len(picks)} 只潜力股*")
        for s in picks[:3]: msg_lines.append(f"• {s['name']} (DDE:{s['dde']})")
        if len(picks) > 3: msg_lines.append(f"...更多请看网页")
    else:
        msg_lines.append("\n🍵 今日无符合严苛条件的个股")

    if PAGE_URL_PREFIX:
        msg_lines.append(f"\n🔗 [点击查看完整图表]({PAGE_URL_PREFIX})")
    
    send_telegram_message("\n".join(msg_lines))

    # F. 保存历史
    if top_concepts:
        history_data[today_str] = [x[0] for x in top_concepts]
        with open(HISTORY_FILE, 'w') as f: json.dump(history_data, f)

if __name__ == "__main__":
    run_task()
