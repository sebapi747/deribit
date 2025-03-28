import json
import sys
from datetime import datetime,timedelta
import pytz
import pandas as pd

ticker_suffix_info = { 
    # UTC Open: 21:00
    'NZ': ('17:45', '10:00', 'Pacific/Auckland', [5, 6]),      # New Zealand (NZX)
    # UTC Open: 23:00 (Australia/Sydney, etc.)
    'AX': ('17:00', '10:00', 'Australia/Sydney', [5, 6]),      # Australia (ASX)  
    # UTC Open: 00:00 (Asia/Tokyo, Asia/Seoul, etc.)
    'KS': ('15:30', '09:00', 'Asia/Seoul', [5, 6]),            # South Korea (Korea)
    'T':  ('15:00', '09:00', 'Asia/Tokyo', [5, 6]),            # Japan (Tokyo)

    # UTC Open: 01:00
    'SI': ('17:00', '09:00', 'Asia/Singapore', [5, 6]),        # Singapore (SGX)
    'KL': ('17:00', '09:00', 'Asia/Kuala_Lumpur', [5, 6]),     # Malaysia (Kuala Lumpur)
    'SS': ('15:00', '09:00', 'Asia/Shanghai', [5, 6]),         # China (Shanghai)
    'SZ': ('15:00', '09:00', 'Asia/Shanghai', [5, 6]),         # China (Shenzhen)
    'TW': ('13:30', '09:00', 'Asia/Taipei', [5, 6]),           # Taiwan (Taiwan Stock Exchange)
    # UTC Open: 01:30
    'HK': ('16:00', '09:30', 'Asia/Hong_Kong', [5, 6]),        # Hong Kong (HKEX)
    # UTC Open: 02:00
    'JK': ('15:00', '09:00', 'Asia/Jakarta', [5, 6]),          # Indonesia (Jakarta)
    'VN': ('14:30', '09:00', 'Asia/Ho_Chi_Minh', [5, 6]),      # Vietnam (Ho Chi Minh)
    # UTC Open: 03:00
    'BK': ('17:00', '10:00', 'Asia/Bangkok', [5, 6]),          # Thailand (Bangkok)
    # UTC Open: 15:30
    'NS': ('15:30', '09:15', 'Asia/Kolkata', [5, 6]),          # India (National Stock Exchange)

    # UTC Open: 06:00
    'KW': ('14:00', '09:00', 'Asia/Kuwait', [4, 5]),           # Kuwait (Kuwait Stock Exchange)
    'QA': ('13:30', '09:00', 'Asia/Qatar', [4, 5]),            # Qatar (Qatar Stock Exchange)
    'AE': ('14:00', '10:00', 'Asia/Dubai', [4, 5]),            # UAE (Abu Dhabi Securities Exchange)

    # UTC Open: 07:00
    'HE': ('17:00', '09:00', 'Europe/Helsinki', [5, 6]),       # Finland (Helsinki)
    'VS': ('16:00', '09:00', 'Europe/Vilnius', [5, 6]),        # Lithuania (Vilnius)
    'JO': ('16:00', '09:00', 'Africa/Johannesburg', [5, 6]),   # South Africa (Johannesburg)
    'SR': ('15:00', '10:00', 'Asia/Riyadh', [4, 5]),           # Saudi Arabia (Tadawul)

    # UTC Open: 07:45
    'RO': ('16:45', '09:45', 'Europe/Bucharest', [5, 6]),      # Romania (Bucharest)

    # UTC Open: 08:00
    'L': ('16:30', '08:00', 'Europe/London', [5, 6]),          # UK (London)
    'DE': ('17:30', '09:00', 'Europe/Berlin', [5, 6]),         # Germany (Deutsche Börse)
    'PA': ('17:30', '09:00', 'Europe/Paris', [5, 6]),          # France (Euronext Paris)
    'AS': ('17:30', '09:00', 'Europe/Amsterdam', [5, 6]),      # Netherlands (Euronext Amsterdam)
    'MC': ('17:30', '09:00', 'Europe/Madrid', [5, 6]),         # Spain (Madrid)
    'MI': ('17:30', '09:00', 'Europe/Rome', [5, 6]),           # Italy (Milan)
    'OL': ('16:30', '09:00', 'Europe/Oslo', [5, 6]),           # Norway (Oslo)
    'TA': ('17:00', '10:00', 'Asia/Jerusalem', [5, 6]),        # Israel (Tel Aviv)
    'SW': ('17:30', '09:00', 'Europe/Stockholm', [5, 6]),      # Sweden (OMX Stockholm)
    'ST': ('17:30', '09:00', 'Europe/Stockholm', [5, 6]),      # Sweden (OMX Stockholm)
    'WA': ('17:00', '09:00', 'Europe/Warsaw', [5, 6]),         # Poland (Warsaw)
    'VI': ('17:30', '09:00', 'Europe/Vienna', [5, 6]),         # Austria (Vienna)
    'CO': ('16:25', '09:00', 'Europe/Copenhagen', [5, 6]),     # Denmark (Copenhagen)
    'PR': ('15:30', '09:00', 'Europe/Prague', [5, 6]),         # Czech Republic (Prague)
    'IS': ('15:30', '09:00', 'Europe/Istanbul', [5, 6]),       # Turkey (Borsa Istanbul)
    # UTC Open: 08:30
    'AT': ('17:20', '10:30', 'Europe/Athens', [5, 6]),         # Greece (Athens)
    # UTC Open: 09:00
    'LS': ('17:30', '09:00', 'Europe/Lisbon', [5, 6]),         # Portugal (Euronext Lisbon)
    'BR': ('17:00', '10:00', 'Europe/Brussels', [5, 6]),       # Belgium (Euronext Brussels)
    'IC': ('15:30', '09:00', 'Atlantic/Reykjavik', [5, 6]),    # Iceland (Iceland Stock Exchange)
    # UTC Open: 12:00
    'SN': ('16:00', '09:00', 'America/Santiago', [5, 6]),      # Chile (Santiago)

    # UTC Open: 13:00
    'SA': ('17:00', '10:00', 'America/Sao_Paulo', [5, 6]),     # Brazil (B3 São Paulo)
    # UTC Open: 13:30
    '': ('16:00', '09:30', 'America/New_York', [5, 6]),        # US (NYSE)
    'TO': ('16:00', '09:30', 'America/Toronto', [5, 6]),       # Canada (Toronto)
    'V': ('16:00', '09:30', 'America/Toronto', [5, 6]),        # Canada (TSX Venture)
    # UTC Open: 14:30
    'MX': ('15:00', '08:30', 'America/Mexico_City', [5, 6]),   # Mexico (Mexican Stock Exchange)
    'CL': ('16:00', '09:30', 'America/Bogota', [5, 6]),        # Colombia (Bogota)
}
   
def nextopenday(current_dow,closed_dow,shift):
    nextday = (current_dow+shift)%7
    if nextday not in closed_dow:
        return shift
    direction = 1 if shift>0 else -1
    return nextopenday(current_dow,closed_dow,shift+direction)
    
def get_market_status(suffix):
    close_time, open_time, timezone, closed_dow = ticker_suffix_info[suffix]
    utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    tz = pytz.timezone(timezone)
    local_now = utc_now.astimezone(tz)
    open_hour, open_minute = map(int, open_time.split(':'))
    close_hour, close_minute = map(int, close_time.split(':'))
    current_hour = local_now.hour
    current_minute = local_now.minute
    current_dow = local_now.weekday()  # 0=Mon, 6=Sun
    if current_dow in closed_dow:
        is_open = False
        dtprev = local_now.replace(hour=close_hour, minute=close_minute) + timedelta(days=nextopenday(current_dow,closed_dow,-1))
        dtnext = local_now.replace(hour=open_hour, minute=open_minute) + timedelta(days=nextopenday(current_dow,closed_dow,1))
    else:
        current_minutes = current_hour * 60 + current_minute
        open_minutes = open_hour * 60 + open_minute
        close_minutes = close_hour * 60 + close_minute        
        is_open = open_minutes <= current_minutes < close_minutes
        if is_open:
            dtprev = local_now.replace(hour=open_hour, minute=open_minute)
            dtnext = local_now.replace(hour=close_hour, minute=close_minute)
        else:
            if current_minutes>close_minutes:
                dtprev = local_now.replace(hour=close_hour, minute=close_minute)
            else:
                dtprev = local_now.replace(hour=close_hour, minute=close_minute) +timedelta(days=nextopenday(current_dow,closed_dow,-1))
            if current_minutes<open_minutes:
                dtnext = local_now.replace(hour=open_hour, minute=open_minute)
            else:
                dtnext = local_now.replace(hour=open_hour, minute=open_minute) +timedelta(days=nextopenday(current_dow,closed_dow,1))   
    hktz = pytz.timezone("Asia/Hong_Kong")
    return {"suffix":suffix,'isopen': is_open, 'dtprev': dtprev.astimezone(hktz).replace(microsecond=0), 'dtnext': dtnext.astimezone(hktz).replace(microsecond=0)}

def get_suffixes(strings):
    suffixes = {}
    for s in strings:
        s = str(s)
        if '.' in s:
            suffix = s.rsplit('.', 1)[-1]
        else:
            suffix = ""
        suffixes[suffix] = suffixes.get(suffix,[])+[s]
    return suffixes

def getfilesuffixes(json_filename):
    with open(json_filename, 'r') as f:
        data = json.load(f)
    suffixes = get_suffixes(data)
    return suffixes

def check_markets(json_filename):
    suffixes = ticker_suffix_info.keys()
    market_status = []
    for suffix in suffixes:
        market_status.append(get_market_status(suffix))
    df = pd.DataFrame(market_status).set_index("suffix")
    tickersbysuffix = getfilesuffixes(json_filename)
    return df.join(pd.DataFrame({"count":[len(v) for v in tickersbysuffix.values()]},index=tickersbysuffix.keys()),how="outer").sort_values(["isopen","dtnext"],ascending=[False,True])

def check_recent_markets():
    dtnow = datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone("Asia/Hong_Kong"))
    suffixes = ticker_suffix_info.keys()
    market_status = []
    for suffix in suffixes:
        market_status.append(get_market_status(suffix))
    df = pd.DataFrame(market_status).set_index("suffix")
    return df.loc[df["isopen"] & (df["dtprev"]<dtnow-timedelta(minutes=20)) & (df["dtprev"]>dtnow-timedelta(minutes=70))].index


# Example usage:
"""print(market_status)
print(ismarketopenforticker("AAPL"))      # NYSE, US
print(ismarketopenforticker("RY.TO"))     # Toronto, Canada
print(ismarketopenforticker("BHP.AX"))    # Australia
print(ismarketopenforticker("700.HK"))    # Australia"""
json_filename = sys.argv[1]
market_status = check_markets(json_filename)
print(market_status)
filesbysuffix = getfilesuffixes(json_filename)
suffix = check_recent_markets()
files = []
for s in suffix:
	files.append(filesbysuffix[s])
print(files)
