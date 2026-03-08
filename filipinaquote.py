import requests
from lxml import html
from datetime import datetime
import pandas as pd
import os
import config

def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1]+":"+__file__+":ALERT:" +text, 'parse_mode': 'markdown'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
def get_stock_data(ticker):
    # Dictionary mapping ticker symbols to company codes
    ticker_to_code = {
        "LTG.PS": "12"   # LT Group
    }
    url = f"https://edge.pse.com.ph/companyPage/stockData.do?cmpy_id={ticker_to_code[ticker]}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    result = {"ticker": ticker}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise error for bad status codes
        tree = html.fromstring(response.content)
        price_nodes = tree.xpath('//th[text()="Last Traded Price"]/following-sibling::td[1]/text()')
        result["last_trade_price"] = float(price_nodes[0].strip()) if price_nodes else "Not found"
        as_of_nodes = tree.xpath('//span[contains(text(), "As of")]/text()')
        result["asof_date"] = as_of_nodes[0].strip() if as_of_nodes else "Not found"
        result['asof_date'] = datetime.strptime(result['asof_date'].replace('As of ', ''), '%b %d, %Y %I:%M %p')        
        prev_close_nodes = tree.xpath('//th[contains(text(), "Previous Close and Date")]/following-sibling::td[1]/text()')
        prev_close_raw = prev_close_nodes[0].strip() if prev_close_nodes else "Not found"
        result["previous_close"] = float(prev_close_raw.split('(')[0].strip())
        result["previous_date"] = prev_close_raw.split('(')[1].strip(' )') if '(' in prev_close_raw else ""
        result["previous_date"] = datetime.strptime(result['previous_date'], '%b %d, %Y')
    except Exception as e:
        sendTelegram(f"failed for ticker:{ticker}, {e}")
    return result

def savedict(stock_data):
    filename = "filipinaquotecsv/filipinaquote.csv"
    dfold = pd.read_csv(filename)  if os.path.exists(filename) else pd.DataFrame()
    df = pd.DataFrame([stock_data])
    dfout = pd.concat([dfold,df])
    print(f"Found {filename} with {len(dfold)}, saving with {len(dfout)} rows.")
    dfout.drop_duplicates(["ticker","previous_date","asof_date"]).to_csv(filename,index=False)
    
# Example usage:
if __name__ == "__main__":
    stock_data = get_stock_data("LTG.PS")
    print(stock_data)
    savedict(stock_data)
