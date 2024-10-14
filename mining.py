import requests,json,os
from lxml.html.soupparser import fromstring
from pathlib import Path
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config

#btcaddress = '38zPevxjY7bNxtRMnpbs8rF5tMnpTCWtNt'
btcaddress = "1MeNUCC6buJUaj5L2PAiZtSso44xdVUn7C"

def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1]+":"+__file__+":ALERT:" +text, 'parse_mode': 'HTML'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
def getmyhashrate(hashratedic):
    try:
        threehouravghash = hashratedic['3 hrs']
        if threehouravghash[-5:]!=" Th/s":
            raise Exception("could not get hashrate from %s" % (threehouravghash))
        threehouravghash = float(threehouravghash[:-5])
    except:
        raise Exception("could not get hashrate from %s" % str(hashratedic))
    if threehouravghash==0:
        sendTelegram("ERR: no hashrate")
    return threehouravghash*1e9
def getmybtcpayout(payoutsnap):
    try:
        payout = payoutsnap[1]
        if payout[-4:]!=" BTC":
            raise Exception("could not get balance from %s" % (payoutsnap))
        payout = float(payout[:-4])
    except:
        raise Exception("could not get balance from %s" % str(payoutsnap))
    return payout

def checkmining(btcaddress):
    url = "https://api.blockchair.com/bitcoin/stats"
    x = requests.get(url)
    if x.status_code!=200:
        raise Exception("ERR:%s %d" % (url,x.status_code))
    answer = x.json()
    del x
    url = "https://ocean.xyz/stats/%s" % btcaddress
    x = requests.get(url)
    if x.status_code!=200:
        raise Exception("ERR:%s %d" % (url,x.status_code))
    tree = fromstring(x.text)
    del x
    payoutsnap = tree.xpath('//div[@id="payoutsnap-statcards"]/div/span/text()')
    hashratetable = tree.xpath('//tbody[@id="hashrates-tablerows"]/tr/td/text()')
    hashratedic = {}
    for i in range(len(hashratetable)//3):
        hashratedic[hashratetable[3*i]] = hashratetable[3*i+1]
    eleckWhprice = 0.12
    block_reward=100*2**(-answer['data']['blocks']//210000)
    totalhashperbtc  = float(answer['data']['hashrate_24h'])/24/6*block_reward
    totalhashperusd  = totalhashperbtc/answer['data']['market_price_usd']
    mydailyhash = getmyhashrate(hashratedic)*24*3600
    usdperday   = mydailyhash/totalhashperusd
    btcperday   = mydailyhash/totalhashperbtc
    usdelecperday = 0.104*eleckWhprice*24
    totalhashperblock = float(answer['data']['hashrate_24h'])/24/6
    myhashperblock    = getmyhashrate(hashratedic)*60*10
    oddsperblock      = totalhashperblock/myhashperblock
    soloblockyear     = oddsperblock/365/24/6
    poolbalance = getmybtcpayout(payoutsnap)
    balances = "pool balance=%.8fBTC (%.2f days of mining) theo from hash=%.8fBTC/day" % (poolbalance,poolbalance/btcperday,btcperday)
    rates = "reward/yr=%.2fUSD (%.8fBTC) elec/yr=%.2f (%.2fUSD/kWh) solo=%.1f years/block" % (usdperday*365,btcperday*365,usdelecperday*365,eleckWhprice,soloblockyear)
    sendTelegram(balances+"\n"+rates)
    print(balances+"\n"+rates)

if __name__ == "__main__":
    try:
        checkmining(btcaddress)
    except Exception as e:
        sendTelegram("ERR: "+str(e))
        print("ERR: "+str(e))
