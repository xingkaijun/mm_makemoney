import akshare as ak
import pandas as pd
import os
import json
import requests
import time
import glob
from datetime import datetime, timedelta

# --- 1. 配置项 ---
# 必须配置的环境变量
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
# 支持多个ID，用逗号分隔
TG_CHAT_IDS = os.environ.get("TG_CHAT_IDS", "").split(",")

# GitHub Pages 的基础链接，用于生成跳转链接
# 格式通常是: https://<你的用户名>.github.io/<仓库名>
# 如果你不确定，可以先填空字符串，部署好Page后再来修改这里
PAGE_URL_PREFIX = os.environ.get("PAGE_URL_PREFIX", "")

HISTORY_FILE = 'concept_history.json'
ARCHIVE_DIR = 'archive'
HTML_FILE = 'index.html'

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

# --- 2. 选股核心逻辑 ---
def check_stock_criteria(symbol, name, dde_now):
    """
    严苛选股标准:
    1. 连续3天上涨 (True)
    2. 3天累计涨幅 < 10% (True)
    3. 今天温和放量 (1 < 量比 < 2.5)
    4. 3天主力净流入 > 0
    """
    try:
        # 获取个股历史行情 (前复权)
        df = ak.stock_zh_a_hist_df_cf(symbol=symbol, adjust="qfq", period="daily")
        if len(df) < 5: return None
        
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

        # C. 温和放量
        vol_today = today['成交量']
        vol_yest = yesterday['成交量']
        if vol_today <= vol_yest: return None 
        if vol_today > (vol_yest * 2.5): return None 

        # D. 资金流入 (放在最后以减少请求)
        try:
            market = "sh" if symbol.startswith("6") else "sz"
            df_flow = ak.stock_individual_fund_flow(stock=symbol, market=market)
            flow_sum = df_flow.tail(3)['主力净流入'].sum()
            if flow_sum <= 0: return None
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
    except:
        return None

def run_strict_selection():
    print("🔍 开始执行严选扫描 (预计耗时 1-2 分钟)...")
    selected_stocks = []
    try:
        # 1. 全市场快照
        df_spot = ak.stock_zh_a_spot_em()
        
        # 2. 初筛 (向量化过滤)
        # 去除ST、无资金数据、停牌股
        mask = (
            (~df_spot['名称'].str.contains('ST|退')) & 
            (df_spot['主力净流入'].notnull()) & 
            (df_spot['流通市值'] > 0)
        )
        df_spot = df_spot[mask].copy()
        
        # 计算 DDE
        df_spot['DDE'] = (df_spot['主力净流入'] / df_spot['流通市值']) * 100
        
        # 初筛条件: DDE>0.5, 今日红盘且未涨停(留空间)
        pool = df_spot[
            (df_spot['DDE'] > 0.5) & 
            (df_spot['涨跌幅'] > 0) & 
            (df_spot['涨跌幅'] < 8)
        ].copy()
        
        # 按市值排序，优先看小市值
        pool = pool.sort_values(by='总市值', ascending=True)
        
        # 取前 60 个进入决赛圈
        check_list = pool.head(60) 
        
        # 3. 深度扫描
        for _, row in check_list.iterrows():
            res = check_stock_criteria(row['代码'], row['名称'], row['DDE'])
            if res:
                res['mkt_cap'] = round(row['总市值'] / 100000000, 2) # 亿
                selected_stocks.append(res)
            time.sleep(0.15) # 防封限流
            
    except Exception as e:
        print(f"❌ 选股过程异常: {e}")
        
    return selected_stocks

# --- 3. 网页生成 & 归档 ---
def generate_html_report(today_str, new_concepts, top_concepts, picks):
    """生成包含历史链接的HTML"""
    
    # 1. 准备个股 HTML 片段
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

    # 2. 准备概念 HTML 片段
    concept_html = ""
    if new_concepts:
        concept_html = "".join([f'<span class="tag">{n}</span>' for n in new_concepts])
    else:
        concept_html = '<span style="color:#999;font-size:12px">今日无新面孔，资金在老热点轮动</span>'

    top_html = "".join([f'<span class="tag tag-gray">{n}</span>' for n, _ in top_concepts])

    # 3. 扫描 archive 目录生成历史链接
    history_links_html = ""
    if os.path.exists(ARCHIVE_DIR):
        # 获取所有 html 文件并按文件名(日期)倒序排列
        files = sorted(glob.glob(f"{ARCHIVE_DIR}/*.html"), reverse=True)
        # 只取最近 7 天
        files = files[:7]
        
        if files:
            history_links_html = "<h3>📅 历史回顾</h3><div class='history-list'>"
            for f_path in files:
                # 文件名如 archive/2026-01-25.html
                fname = os.path.basename(f_path) 
                date_label = fname.replace(".html", "")
                # 相对路径链接
                history_links_html += f"<a href='{ARCHIVE_DIR}/{fname}' class='history-link'>{date_label}</a>"
            history_links_html += "</div>"

    # 4. 完整的 HTML 模板
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

            <div class="footer">Data by AkShare | Auto-generated</div>
        </div>
    </body>
    </html>
    """
    return html_content

# --- 4. 主任务流程 ---
def run_task():
    today_str = datetime.now().strftime('%Y-%m-%d')
    print(f"🚀 任务启动: {today_str}")

    # A. 获取板块数据
    try:
        df_concept = ak.stock_board_concept_name_em().sort_values('涨跌幅', ascending=False).head(10)
        top_concepts = list(zip(df_concept['板块名称'], df_concept['涨跌幅']))
    except Exception as e:
        print(f"板块数据获取失败: {e}")
        top_concepts = []

    # B. 读取并对比历史
    history_data = {}
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f: history_data = json.load(f)
        except: pass
    
    past_set = set()
    cutoff_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    for d, names in history_data.items():
        if d > cutoff_date and d != today_str:
            past_set.update(names)
    
    new_concepts = [n for n, r in top_concepts if n not in past_set]

    # C. 执行选股
    picks = run_strict_selection()

    # D. 生成并保存网页
    # 确保 archive 目录存在
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)
    
    # 生成 HTML 内容
    html_content = generate_html_report(today_str, new_concepts, top_concepts, picks)
    
    # 1. 保存为归档文件 (永久保存)
    archive_path = f"{ARCHIVE_DIR}/{today_str}.html"
    with open(archive_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"✅ 归档已保存: {archive_path}")
    
    # 2. 保存为首页 (index.html)
    with open(HTML_FILE, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"✅ 首页已更新: {HTML_FILE}")

    # E. 发送 Telegram
    msg_lines = [f"📊 *A股复盘日报* ({today_str})"]
    
    if new_concepts: msg_lines.append(f"🔥 *新风口*: {', '.join(new_concepts)}")
    else: msg_lines.append("👀 无新风口，老热点轮动")
    
    if picks:
        msg_lines.append(f"\n💎 *严选出 {len(picks)} 只潜力股*")
        # 仅展示前3只摘要，引导点击网页
        for s in picks[:3]:
            msg_lines.append(f"• {s['name']} (DDE:{s['dde']})")
        if len(picks) > 3:
            msg_lines.append(f"...更多请看网页")
    else:
        msg_lines.append("\n🍵 今日无符合严苛条件的个股")

    if PAGE_URL_PREFIX:
        msg_lines.append(f"\n🔗 [点击查看完整图表]({PAGE_URL_PREFIX})")
    
    send_telegram_message("\n".join(msg_lines))

    # F. 更新历史数据文件
    if top_concepts:
        history_data[today_str] = [x[0] for x in top_concepts]
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history_data, f)

if __name__ == "__main__":
    run_task()