import os
os.chdir('/home/seb/work/deribit')
import yahoo1minquote
import yahoomkttime
tickers = yahoomkttime.check_open_tickers(["posticker.json","goodtickers.json"])
out,errors = "",""
for i,ticker in enumerate(tickers):
    print(i,len(tickers),ticker)
    ticker,nbbefore,nbafter,err = yahoo1minquote.inserttickersymbols(ticker,sleeptime=0.2)
    out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
    errors += err
print(errors)
print(out)

