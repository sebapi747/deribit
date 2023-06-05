import requests
import datetime as dt
import pandas as pd
import time, os
from pathlib import Path
import sqlite3
dbfile = "sql/ib.db"

''' ---------------------------------------------------------------------------------------------
 SQLite utils
'''
class DBObj(object):
    def __init__(self):
        self.db = sqlite3.connect(dbfile)
g = DBObj()

def insert_df_to_table(df, tablename, cols):
    df[cols].to_sql(name=tablename+'_tmp', con=g.db, if_exists='replace',index=False)
    sql = 'insert or replace into '+tablename+' ('+','.join(cols)+') select '+','.join(cols)+' from '+tablename+'_tmp'
    g.db.execute(sql)
    g.db.commit()
    g.db.execute('drop table '+tablename+'_tmp')


strdate = str(dt.datetime.utcnow())[0:10]
dirname = "data/%s/yahoo" % strdate
Path(dirname).mkdir(parents=True, exist_ok=True)

def ccyfile(ccy):
	return "%s/%s.csv" % (dirname, ccy)

def insertfx(ccy):
    filename = ccyfile(ccy)
    df = pd.read_csv(filename)
    df.rename(columns = {'Date':'dt', 'Close': 'spot'}, inplace = True)
    df['ccy'] = ccy
    print("INFO: inserting %d rows in %s fx table" % (len(df),ccy))
    insert_df_to_table(df, "fx", ["ccy","dt","spot"])

def insertallfx():
    ccydf = pd.read_csv("ccylist.csv")
    for ccy in ccydf['ccy']:
        if os.path.isfile(ccyfile(ccy))==False:
            continue
        insertfx(ccy)

def getfxdata():
    ccydf = pd.read_csv("ccylist.csv")
    for ccy in ccydf['ccy']:
        filename = ccyfile(ccy)
        if ccy=="USD" or os.path.isfile(filename):
            continue
        headers = {'accept':'*/*', 'user-agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Raspbian Chromium/78.0.3904.108 Chrome/78.0.3904.108 Safari/537.36'}
        timestamp = int(dt.datetime.now().timestamp()-60*16) # 16 minutes ago
        tstart = 1070236800-60*60*24*365*20
        query = "https://query1.finance.yahoo.com/v7/finance/download/%sUSD=X?period1=%d&period2=%d&interval=1mo&events=history&includeAdjustedClose=true" % (ccy, tstart, timestamp)
        x = requests.get(query, headers=headers)
        print(ccy, x.status_code, filename)
        file = open(filename,"w") 
        file.write(x.text)
        file.close()
        insertfx(ccy)
        time.sleep(0.5)
        
if __name__ == "__main__":
    getfxdata()
    insertallfx()
