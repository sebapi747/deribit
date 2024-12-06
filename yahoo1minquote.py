import os,json,time,sqlite3
import datetime as dt
import pandas as pd
import random
import requests
import numpy as np
import pytz
import re
import time
import matplotlib.pyplot as plt
import matplotlib as mpl
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
dirname = config.dirname 
localtz = pytz.timezone("Asia/Hong_Kong")
utctz   = pytz.timezone("UTC")

# 0:mon,1:tue,2:wed,3:tue,4:fri,5:sat,6:sun
if dt.datetime.utcnow().weekday()>=6:
    print("INFO: closed on sun")
    exit()
    
def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1] +":"+__file__+":"+text, 'parse_mode': 'markdown'}  
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
def get_jsonfilename(ticker):
    return "json/%s.json" % (ticker)
def get_json_data(ticker):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + ticker
    filename = get_jsonfilename(ticker)
    errfilename = filename.replace(".json",".err")
    if os.path.exists(filename):
        filehours = (dt.datetime.now().timestamp()-os.path.getmtime(filename))/3600
        print("INFO: %s found" % filename, end="\r")
        if filehours<0.5:
            with open(filename,"r") as f:
                return json.load(f)
    if os.path.exists(errfilename):
        filehours = (dt.datetime.now().timestamp()-os.path.getmtime(errfilename))/3600
        print("INFO: %s found" % filename)
        if filehours<2:
            raise Exception("ERR: %s occurred less than 2 hours ago" % errfilename)
    sleeptime = random.uniform(1,2)
    print("INFO: %s in %.2fsec" % (url,sleeptime))
    time.sleep(sleeptime)
    os.system("rm -f %s" % filename)
    os.system("curl --silent %s > %s" % (url,filename))
    try:
        if not os.path.exists(filename):
            raise Exception("ERR: could not fetch %s" % url)
        with open(filename,"r") as f:
            jsondata = json.load(f)
        if jsondata['chart'].get('error') is not None:
            raise Exception("ERR: %s %s" % (ticker, jsondata['chart']['error']['description']))
    except Exception as e:
        with open(errfilename, "w") as f:
            f.write(str(e))
        raise
    return jsondata

def convertdate(d):
    return dt.datetime.fromtimestamp(d).astimezone(localtz).astimezone(utctz)
def processjsontopandas(jsondata):
    jsonmeta = jsondata['chart']['result'][0]['meta']
    #echgtz  = pytz.timezone(jsonmeta['exchangeTimezoneName'])
    result  = jsondata['chart']['result'][0]
    if result.get('timestamp') is not None and len(result['indicators']['quote'])>0:
        dates = [convertdate(d) for d in result['timestamp']]
        out = {'utcdate':dates}
        for c in ["open","low","high","close","volume"]:
            out[c] = result['indicators']['quote'][0][c]
    else:
        meta = result['meta']
        with open(get_jsonfilename(meta['symbol']).replace(".json",".err"), "w") as f:
            json.dump(meta,f)
        raise Exception("ERR: %s json does not have time series" % meta['symbol'])
    df = pd.DataFrame(out).dropna()
    df['symbol'] = jsonmeta['symbol']
    return df
    
def schema(dbfilename):
    if not os.path.exists(dbfilename):
        print("WARN: missing file %s" % dbfilename)
        #os.system("touch %s" % dbfilename)
        with sqlite3.connect(dbfilename) as con:
            con.execute('''
            create table quoteminutebar (
            symbol text,
            utcdate datetime,
            open numeric,
            low numeric,
            high numeric,
            close numeric,
            volume numeric,
            primary key(symbol, utcdate)
            )''')

def insert_df_to_table(df, tablename, cols,con):
    df[cols].to_sql(name=tablename+'_tmp', con=con, if_exists='replace',index=False)
    sql = 'insert or replace into '+tablename+' ('+','.join(cols)+') select '+','.join(cols)+' from '+tablename+'_tmp'
    con.execute(sql)
    con.execute('drop table '+tablename+'_tmp')
    
def getandinsertfutpandas(ticker,dbfilename):
    df = processjsontopandas(get_json_data(ticker))
    print("INFO: %s inserting %d" % (ticker,len(df)),end="\r")
    with sqlite3.connect(dbfilename) as con:
        insert_df_to_table(df, "quoteminutebar", df.columns,con)

def inserttickersymbols(ticker):
    err = ""
    dbfilename = "%s/yahoo1minquote/%s.db" % (dirname,ticker)
    schema(dbfilename)
    with sqlite3.connect(dbfilename) as con:
        nbbefore = len(pd.read_sql("select 1 from quoteminutebar", con=con))
    try:
        getandinsertfutpandas(ticker,dbfilename)
    except Exception as e:
        err += "\n%s" % str(e)
    with sqlite3.connect(dbfilename) as con:
        nbafter = len(pd.read_sql("select 1 from quoteminutebar", con=con))
    print(err)
    print("INFO: %s: had %d quotes now %d" % (ticker,nbbefore,nbafter))
    return ticker,nbbefore,nbafter,err

'''
gooddf = pd.read_csv(dirname+"gooddf.csv")
with sqlite3.connect("sql/ib.db") as con:
    yimap = pd.read_sql("select yahooticker,finchatticker,finchatexchg from yahoo_roic_ib_map where finchatticker not like 'MISX-%' and finchatticker is not null",con=con)
tickers = yimap.loc[yimap["finchatticker"].isin(gooddf["ticker"]),"yahooticker"].tolist()
json.dumps(tickers)
json.dumps(list(set(gooddf['ccy'])))
'''
def insertalltickers():
    tickers = ["AAPL", "NVDA", "GOOGL", "META", "LLY", "MA", "COST", "MC.PA", "ASML.AS", "600519.SS", "SAP.DE", "AZN.L", "ADBE", "OR.PA", "ABT", "ITX.MC", "CMCSA", "BKNG", "SYK", "LMT", "NKE", "MDT", "CB", "CDI.PA", "AI.PA", "HCA", "6861.T", "ELV", "LRCX", "SCCO", "KLAC", "CFR.SW", "RACE", "APH", "000858.SZ", "MSI", "MRK.DE", "NOC", "ORLY", "2454.TW", "RSG", "FTNT", "7974.T", "TRV", "PSA", "URI", "7010.SR", "GWW", "WALMEX.MX", "PAYX", "BMW.DE", "0388.HK", "DELTA.BK", "MSCI", "LEN", "300760.SZ", "PWR", "EXPN.L", "FAST", "MPWR", "EW", "WEGE3.SA", "FMG.AX", "VRSK", "IDXX", "CTSH", "CBRE", "SYY", "LULU", "CCEP.AS", "2328.HK", "601225.SS", "MLM", "CAP.PA", "VMC", "AHT.L", "2308.TW", "AVB", "HNR1.DE", "HUM", "GRMN", "KER.PA", "FEMSAUBD.MX", "MTD", "VER.VI", "ANSS", "LISN.SW", "2333.HK", "PUB.PA", "0669.HK", "600025.SS", "CHD", "STMN.SW", "CHKP", "WRB", "3711.TW", "CINF", "600031.SS", "EME", "WST", "TER", "STLD", "TLX.DE", "CEZ.PR", "600019.SS", "GEBN.SW", "ESS", "LH", "VRSN", "MAA", "MANH", "CPALL.BK", "ULTA", "RPM", "EG", "600886.SS", "OC", "AMCR", "2338.HK", "UHS", "UI", "2280.SR", "AC.MX", "0003.HK", "UDR", "GGG", "BDMS.BK", "JKHY", "BIM.PA", "PME.AX", "ELS", "CPT", "VACN.SW", "NVT", "HLMA.L", "TXRH", "PNDORA.CO", "SFM", "CACI", "TFII.TO", "AOS", "RAA.DE", "BWXT", "TECH", "3045.TW", "WING", "EVR", "3092.T", "CPU.AX", "AUTO.L", "MEDP", "3231.TW", "HII", "600845.SS", "GL", "EHC", "1193.HK", "BKW.SW", "PRI", "MEL.NZ", "1929.HK", "ROCK-B.CO", "0358.HK", "8869.KL", "ORK.OL", "SEIC", "AMRT.JK", "FGR.PA", "FN", "AYI", "DCI", "GAPB.MX", "X.TO", "SHL.AX", "TPX", "8210.SR", "ASURB.MX", "BEAN.SW", "2688.HK", "AAK.ST", "603369.SS", "FCN", "LIVEPOLC-1.MX", "3808.HK", "2379.TW", "RADL3.SA", "BFAM", "SSD", "ADRO.JK", "RLI", "FHZN.SW", "600570.SS", "603198.SS", "600426.SS", "GMXT.MX", "3998.HK", "4704.T", "3653.TW", "G", "GNTX", "HL.L", "BH.BK", "BEZ.L", "LSTR", "RMV.L", "SPIE.PA", "PUM.DE", "EXLS", "900948.SS", "600968.SS", "OPAP.AT", "600161.SS", "BMI", "REY.MI", "MCY.NZ", "SFSN.SW", "HER.MI", "4030.SR", "EXPO", "3443.TW", "BME.L", "FPE3.DE", "CPIN.JK", "2449.TW", "4002.SR", "6532.T", "601216.SS", "LANC", "0868.HK", "RYN", "ISA.CL", "FELE", "9684.T", "COLM", "TNET", "QLYS", "NWL.AX", "GL9.IR", "0270.HK", "FRAGUAB.MX", "GEMS.JK", "4190.SR", "600398.SS", "ITGR", "QFLS.QA", "600132.SS", "6965.T", "SOP.PA", "600754.SS", "TOM.OL", "600873.SS", "603899.SS", "IPAR", "HVN.AX", "IDCC", "4200.SR", "DOO.TO", "AZM.MI", "KFY", "BDX.WA", "CWK.L", "BEM.BK", "0881.HK", "UNF", "NHI", "600060.SS", "1503.TW", "SAM", "ITP.PA", "CVCO", "PMV.AX", "SAX.DE", "BETS-B.ST", "SHOO", "600612.SS", "CBZ", "OMAB.MX", "NSP", "600007.SS", "VRLA.PA", "HESM", "VIRP.PA", "7649.T", "CMOCTEZ.MX", "6239.TW", "600702.SS", "DNLM.L", "0995.HK", "THULE.ST", "ECV.DE", "FHI", "VIS.MC", "8454.TW", "LSG.OL", "JUN3.DE", "MOVE.SW", "TBIG.JK", "IOSP", "2049.TW", "0548.HK", "3141.T", "TITC.AT", "CEY.L", "2669.HK", "KIND-SDB.ST", "601965.SS", "LOUP.PA", "BCHN.SW", "2089.KL", "2313.TW", "KARN.SW", "GCC.MX", "CBG.BK", "MGNS.L", "EXX.JO", "0811.HK", "AOF.DE", "AIN", "PTBA.JK", "DMP.AX", "300298.SZ", "601126.SS", "4005.SR", "7564.T", "3005.TW", "HILS.L", "600575.SS", "EVTC", "SXI", "600285.SS", "2404.TW", "LACOMERUBC.MX", "APE.AX", "MYRG", "SCHO.CO", "CDA.AX", "SIMO", "KTY.WA", "XTB.WA", "002690.SZ", "6028.T", "VAIAS.HE", "TABAK.PR", "ENAEX.SN", "TEP.L", "OFG", "6951.T", "DAN.MI", "000012.SZ", "1277.HK", "AKRA.JK", "AUSS.OL", "PETS.L", "4260.SR", "0636.HK", "WMK", "6139.TW", "EB5.SI", "601882.SS", "ALESK.PA", "APOG", "600409.SS", "9759.T", "PFV.DE", "MKPI.JK", "3255.KL", "SYSR.ST", "CEM.MI", "600269.SS", "DUE.DE", "ACP.WA", "OEM-B.ST", "BRAV.ST", "HLAN.TA", "600273.SS", "6432.T", "603919.SS", "9939.TW", "603100.SS", "HBH.DE", "7476.T", "MONY.L", "BCH.BK", "CAF.MC", "2458.TW", "CSGS", "MCRI", "WOSG.L", "SLCE3.SA", "7734.T", "DMLP", "7419.T", "CRAI", "0719.HK", "2127.T", "AFG.OL", "NRO.PA", "DOM.WA", "GRNG.ST", "TROAX.ST", "STG.CO", "MTRX.TA", "BDT.TO", "ANHYT.IS", "3376.TW", "SKIS-B.ST", "600557.SS", "6257.TW", "ELG.DE", "9627.T", "7730.T", "2597.TW", "8210.TW", "600987.SS", "600125.SS", "2299.HK", "3306.HK", "WAC.DE", "VETO.PA", "ZV.MI", "MEGA.BK", "MUM.DE", "TEL1L.VS", "6214.TW", "BDGI.TO", "MIDI.JK", "LAPD.TA", "8154.T", "ONE.TA", "8996.TW", "2850.TW", "CHG.BK", "2455.TW", "ELN.MI", "REG1V.HE", "HARVIA.HE", "NEU.WA", "600035.SS", "603855.SS", "MAVI.IS", "4292.SR", "3046.T", "AX1.AX", "9979.HK", "LAS-A.TO", "HERDEZ.MX", "IGIC", "1052.HK", "MBUU", "2520.TW", "O5RU.SI", "FOI-B.ST", "6269.TW", "6866.T", "2124.T", "DCM.VN", "9672.T", "9757.T", "YSN.DE", "3014.TW", "2320.SR", "1523.HK", "9960.T", "2190.SR", "KOMN.SW", "SBT.PA", "CMB.MI", "OLVAS.HE", "6490.T", "4919.T", "SWTQ.SW", "ASE.WA", "4849.T", "8081.TW", "AUB.PA", "1879.T", "PLPC", "603970.SS", "S61.SI", "9911.TW", "JANTS.IS", "KID.OL", "SKR.BK", "1475.HK", "SCANFL.HE", "558.SI", "4215.T", "MGIC", "BAHN-B.ST", "TMAS.JK", "5027.KL", "JIN.AX", "9930.TW", "ODC", "1785.HK", "600681.SS", "KRI.AT", "ABS.WA", "WEHB.BR", "ITIC", "TDRN.TA", "6777.T", "3733.T", "1830.HK", "2480.TW", "8057.T", "CYDSASAA.MX", "7100.KL", "9621.T", "0098.HK", "0746.HK", "3964.T", "PCR.WA", "CWCO", "1268.HK", "IXX.DE", "6914.T", "SNT.WA", "4298.T", "6859.T", "6099.T", "6183.TW", "4641.T", "FQT.DE", "FORTH.BK", "6381.T", "CLTN.SW", "CIX", "4481.T", "1720.TW", "PLSN.TA", "ALC.MC", "3431.T", "AOJ-B.CO", "4765.T", "PERR.PA", "QISI.QA", "4674.T", "0331.HK", "3454.TW", "ARD.TA", "MPX", "INF.PA", "NPAPER.ST", "RLGT", "6525.TW", "AEF.AX", "VP.L", "2368.HK", "MRB.WA", "LOGO.IS", "4338.SR", "LYL.AX", "MEDI.OL", "2397.TW", "VOX.WA", "3341.T", "4571.TW", "VICI.JK", "UZU.DE", "1799.T", "BBW.SI", "RPI-UN.TO", "CMDR.TA", "LANNA.BK", "UVAN.BK", "3901.T", "IVU.DE", "7035.KL", "3130.TW", "OLTH.AT", "ORS.MI", "7921.T", "EQUI.MI", "5271.KL", "1748.HK", "QC7.SI", "AH.BK", "RJH.BK", "HYDRA.AS", "7609.T", "5285.TW", "EML", "6224.TW", "BEC.MI", "PWS.MI", "FID.AX", "ELMD", "MTI.BK", "3798.T", "MED", "ESCA", "7949.T", "BAN.MI", "6144.T", "SHVA.TA", "FRO.WA", "3921.T", "7059.T", "AMB.WA", "OLY.TO", "9172.KL", "4557.TW", "CGS.L", "DCR.WA", "6262.KL", "SAMG", "8125.KL", "4771.T", "2491.T", "0240.HK", "SUR.JO", "REC.L", "ARAD.TA", "DNG.TO", "XRF.AX", "3771.T", "PSSI.JK", "PAYT.TA", "TOG.BK", "APT.WA", "1752.TW", "1127.HK", "2062.KL", "1586.HK", "SWA.DE", "3902.T", "9644.T", "WAT.AX", "JTPE.JK", "DVLA.JK", "6246.T", "4396.T", "3763.T", "TTR1.DE", "3676.T", "WTN.WA", "6061.T", "FIRE.ST", "DUNCANFOX.SN", "9702.T", "1730.TW", "YATAS.IS", "EMCO.TA", "0012.KL", "BCY.SI", "6086.T", "2816.T", "7472.T", "LIX.VN", "VLS.AX", "AI.BK", "7178.KL", "2176.HK", "8443.KL", "RAIL.ST", "NELY.JK", "GABR.CO", "7134.KL", "G5EN.ST", "5147.KL", "SIILI.HE", "WFCF", "BBL.AX", "MAK.WA", "HIT.AX", "SNV.JO", "SON.WA", "LHIS.TA", "CNG.VN", "7029.KL", "TMILL.BK", "0040.KL", "569.SI", "GEBKA.AT", "FERIAOSOR.SN", "IFI.WA", "8079.KL", "SPVI.BK", "EAST.JK", "IRO.V", "EDI.WA", "AZN", "CCEP", "TYMN.L", "1701.TW", "ASX", "ASR", "7705.T", "3094.T", "2924.T", "4783.T", "3804.T", "9428.T", "6161.T", "7711.T", "8769.T", "3359.T", "3172.T", "3695.T", "7865.T", "2303.T", "7081.T", "RACE.MI", "OJM.MI", "SAP", "VIE.VI", "601633.SS", "601598.SS", "AW-UN.TO", "TITC.BR", "FQT.VI", "KME.AX","SPY","GLD","AGG","USO"]
    ccys = ["AUD", "BRL", "CAD", "CHF", "CLP", "CNY", "COP", "CZK", "DKK", "EUR", "GBP", "HKD", "IDR", "ILS", "JPY", "MXN", "MYR", "NOK", "NZD", "PLN", "QAR", "SAR", "SEK", "SGD", "THB", "TRY", "TWD", "USD", "VND", "ZAR"]
    errors = ""
    out = "\n|ticker|before|after|\n|---|---:|---:|\n"
    for ticker in tickers:
        ticker,nbbefore,nbafter,err = inserttickersymbols(ticker)
        out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
        errors += err
    for ccy in ccys:
        ticker = ccy+'=X'
        ticker,nbbefore,nbafter,err = inserttickersymbols(ticker)
        out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
        errors += err
    sendTelegram(out+errors)

if __name__ == "__main__":
    insertalltickers()
