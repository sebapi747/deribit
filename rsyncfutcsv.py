import os
import config
remotedir = config.remotedir
dirname = config.dirname
outdir = dirname + "/pics/"
os.system('rsync -avzhe ssh %s %s' % (dirname+"/futcsv", remotedir))
