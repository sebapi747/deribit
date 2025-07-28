import glob
import os
import pandas as pd
import requests
import datetime as dt
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
remotedir = config.remotedir
outdir = config.dirname + "/meteopng"

def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}
def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1] +":"+__file__+":"+text, 'parse_mode': 'markdown'}  
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
def plot_city_weather(csv_file):
    # Extract city name from file name
    city_name = os.path.basename(csv_file).replace('_weather.csv', '').replace('_', ' ').title()
    
    # Read CSV data
    df = pd.read_csv(csv_file)
    lastdate = df['Date'].array[-1]
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Create figure with 3 subplots in a row
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 5))
    
    # Plot 1: Temperature and Humidity
    ax1.plot(df['Date'], df['Max_Temperature_C'], color='#8B0000', label='Max Temp (°C)')
    ax1.plot(df['Date'], df['Min_Temperature_C'], color='#8B0000', linestyle='--', label='Min Temp (°C)')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Temperature (°C)', color='#8B0000')
    ax1.tick_params(axis='y', labelcolor='#8B0000')
    
    ax1b = ax1.twinx()
    ax1b.plot(df['Date'], df['Mean_Humidity_Pct'], color='#00B7EB', label='Humidity (%)')
    ax1b.set_ylabel('Humidity (%)', color='#00B7EB')
    ax1b.tick_params(axis='y', labelcolor='#00B7EB')
    
    ax1.legend(loc='upper left')
    ax1b.legend(loc='upper right')
    
    # Plot 2: Precipitation and Pressure
    ax2.bar(df['Date'], df['Precipitation_mm'], color='#87CEEB', label='Precipitation (mm)')
    ax2.set_xlabel(f'Date (last: {lastdate})')
    ax2.set_ylabel('Precipitation (mm)', color='#87CEEB')
    ax2.tick_params(axis='y', labelcolor='#87CEEB')
    
    ax2b = ax2.twinx()
    ax2b.plot(df['Date'], df['Mean_Pressure_hPa'], color='#708090', label='Pressure (hPa)')
    ax2b.set_ylabel('Pressure (hPa)', color='#708090')
    ax2b.tick_params(axis='y', labelcolor='#708090')
    
    ax2.legend(loc='upper left')
    ax2b.legend(loc='upper right')
    
    # Plot 3: PM10 and PM2.5
    ax3.plot(df['Date'], df['Mean_PM10_ugm3'], color='#B0B0B0', label='PM10 (µg/m³)')
    ax3.plot(df['Date'], df['Mean_PM25_ugm3'], color='#D1B000', linestyle='--', label='PM2.5 (µg/m³)')
    ax3.set_xlabel('Date')
    ax3.set_ylabel('Particle Concentration (µg/m³)')
    ax3.legend(loc='upper left')
    
    # Format dates on x-axis for all plots
    date_formatter = DateFormatter('%Y-%m-%d')
    for ax in [ax1, ax2, ax3]:
        ax.xaxis.set_major_formatter(date_formatter)
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    
    # Add super title and adjust layout
    plt.suptitle(f'{city_name} Weather Data', fontsize=16)
    plt.tight_layout()
    
    # Create output directory if it doesn't exist
    os.makedirs('meteopng', exist_ok=True)
    
    # Save plot
    output_file = f'meteopng/{city_name.replace(" ", "_").lower()}.png'
    plt.savefig(output_file, bbox_inches='tight', metadata=get_metadata())
    plt.close()

# Main script
if __name__ == "__main__":
    for csv_file in glob.glob('meteocsv/*.csv'):
        print("processing ",csv_file)
        plot_city_weather(csv_file)
    cmd = 'rsync -avzhe ssh -L %s %s' % (outdir, remotedir)
    print(cmd)
    os.system(cmd)
    sendTelegram("updated [weather](https://www.markowitzoptimizer.pro/static/pics/deribit/meteopng/citywheather.html)")
    
