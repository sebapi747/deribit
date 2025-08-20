import requests,json,os,csv,time
import datetime as dt
from lxml.html.soupparser import fromstring
from pathlib import Path
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
import urllib3
# Disable all warnings (use cautiously)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#btcaddress = '38zPevxjY7bNxtRMnpbs8rF5tMnpTCWtNt'
#btcaddress = "1MeNUCC6buJUaj5L2PAiZtSso44xdVUn7C"
btcaddress = "bc1qgcvc74jydsuz89675d9se5v6klmwl564qdmnzu"

def sendTelegram(text):
    f = __file__
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1]+":"+f+":ALERT:" +text, 'parse_mode': 'markdown'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
def discover_bitaxe_hosts_with_ips():
    os.system("nmap -sn 192.168.1.0/24 > nmap-out.txt 2>&1")
    time.sleep(2)
    hosts = []
    try:
        with open("nmap-out.txt", 'r') as f:
            for line in f:
                if 'Nmap scan report for' in line:
                    parts = line.strip().split()
                    hostname = parts[4] if len(parts) > 4 else None
                    ip = parts[-1].strip('()')
                    
                    if hostname and ('bitaxe' in hostname.lower() or 'suprahex' in hostname.lower()):
                        hosts.append((hostname, ip))
    except FileNotFoundError:
        print("Error reading nmap output")
    return hosts
def restart_bitaxe(hostname="bitaxe"):
    response = requests.get(f"http://{hostname}/api/restart", timeout=10)
    return response.status_code
    
def get_bitaxe_config(ip):
    try:
        # Query ASIC config
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'MiningMonitor/1.0'
        }
        asic_response = requests.get(f"http://{ip}/api/system/asic", headers=headers, timeout=10)
        asic_data = None
        if asic_response.status_code == 200 and asic_response.content:
            try:
                asic_data = asic_response.json()
            except json.JSONDecodeError:
                # Not JSON - log what we actually got
                content_preview = asic_response.text[:1250] if asic_response.text else "Empty"
                sendTelegram(f"ASIC API returned non-JSON: {content_preview}")
        
        # Query system info
        system_response = requests.get(f"http://{ip}/api/system/info", headers=headers, timeout=10)
        system_data = None
        if system_response.status_code == 200 and system_response.content:
            try:
                system_data = system_response.json()
            except json.JSONDecodeError:
                content_preview = system_response.text[:1250] if system_response.text else "Empty"
                sendTelegram(f"System API returned non-JSON: {content_preview}")
        
        config_str = f"ASIC Config: {str(asic_data)}\nSystem Info: {str(system_data)}"
        sendTelegram(f"Pre-restart config:\n{config_str}")
        return asic_data, system_data            
    except requests.exceptions.RequestException as e:
        sendTelegram(f"Network error querying {ip}: {str(e)}")
        return None, None
    except Exception as e:
        sendTelegram(f"Unexpected error querying config: {str(e)}")
        return None, None
    
def oceanmyhashrate(hashratedic):
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
def oceanmybtcpayout(payoutsnap):
    try:
        payout = payoutsnap[1]
        if payout[-4:]!=" BTC":
            raise Exception("could not get balance from %s" % (payoutsnap))
        payout = float(payout[:-4])
    except:
        raise Exception("could not get balance from %s" % str(payoutsnap))
    return payout
def oceanmining(btcaddress):
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
    return oceanmyhashrate(hashratedic), oceanmybtcpayout(payoutsnap)
        
def poolio(btcaddress):
    url = "https://public-pool.io:40557/api/client/%s" % btcaddress
    x = requests.get(url,verify=False)
    if x.status_code!=200:
        raise Exception("ERR:%s %d" % (url,x.status_code))
    myjson = x.json()
    hashrate = 0
    for w in myjson['workers']:
        hashrate += float(w['hashRate'])
    return hashrate,0

def checkmining(btcaddress):
    hostsip = discover_bitaxe_hosts_with_ips()
    url = "https://api.blockchair.com/bitcoin/stats"
    x = requests.get(url)
    if x.status_code!=200:
        raise Exception("ERR:%s %d" % (url,x.status_code))
    answer = x.json()
    myhashrate,poolbalance = poolio(btcaddress)
    if myhashrate == 0:
        get_bitaxe_config("bitaxe")
        restart_status = restart_bitaxe()
        sendTelegram(f"🔧 Restart sent to bitaxe (HTTP {restart_status})")
    eleckWhprice = 0.053
    block_reward=100*2**(-answer['data']['blocks']//210000)
    totalhashperbtc  = float(answer['data']['hashrate_24h'])*60*10/block_reward
    totalhashperusd  = totalhashperbtc/answer['data']['market_price_usd']
    mydailyhash = myhashrate*24*3600
    usdperday   = mydailyhash/totalhashperusd
    btcperday   = mydailyhash/totalhashperbtc
    usdelecperday = 0.104*eleckWhprice*24
    totalhashperblock = float(answer['data']['hashrate_24h'])*60*10
    myhashperblock    = myhashrate*60*10
    oddsperblock      = myhashperblock/totalhashperblock+1e-200
    soloblockyear     = 1/(365*24*6*oddsperblock)
    #poolbalance = getmybtcpayout(payoutsnap)
    balances = "bitaxe hosts: %s\nbtc=%.0f hash=%.8fBTC/day" % (str(hostsip),answer['data']['market_price_usd'],btcperday)
    rates = "reward/yr=%.2fUSD (%.8fBTC) elec/yr=%.2f (%.2fUSD/kWh) solo=%.1f years/block" % (usdperday*365,btcperday*365,usdelecperday*365,eleckWhprice,soloblockyear)
    sendTelegram(balances+"\n"+rates+'\n[public-pool.io](https://web.public-pool.io/#/app/%s)' % btcaddress)
    print(balances+"\n"+rates)
    dic = {"btcperday":btcperday,'usdperday':usdperday,"myhashrate":myhashrate,"tutc":dt.datetime.utcnow()}
    filename =  "%s/bitaxe/bitaxe.csv" % (config.dirname)
    fileexists = os.path.isfile(filename)
    with open(filename, 'a') as f:
        w = csv.writer(f)
        if fileexists == False:
            w.writerow(dic.keys())
        w.writerow(dic.values())


if __name__ == "__main__":
    try:
        checkmining(btcaddress)
    except (Exception,ZeroDivisionError) as e:
        sendTelegram(f"ERR:{str(e)}")
        raise
