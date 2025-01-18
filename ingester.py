import yfinance as yf
import pandas_market_calendars as mcal
import clickhouse_connect
from datetime import datetime
import time

nyse = mcal.get_calendar("NYSE")
tickers = ['AAPL', 'NVDA', 'MSFT']
#Check if market is open right now
date = datetime.now().strftime("%Y-%m-%d")
def connect(host, port, username, password):
    connected = False
    while (connected == False):
        try:
            client = clickhouse_connect.get_client(host=host,port=port,username=username,password=password)
            connected = True
            return client
        except Exception as e:
            print(f"Connection Failed: {e}")
            connected = False
            print("retrying in 10 seconds...")
            time.sleep(10)
def execute(tick, client):
    if ((nyse.schedule(start_date=date, end_date=date).empty==False)):
        if (date == nyse.schedule(start_date = date, end_date = date).iloc[0]['market_open']):
            print("Market Open")
        elif (date == nyse.schedule(start_date = date, end_date = date).iloc[0]['market_close']):
            print("Market Closed")
        #If open get last available price of Ticker List
        ticker  = yf.Ticker(tick)
        data = ticker.history(period="1d")
        if data.empty:
            print("No Data")
        latest_price = data['Close'].iloc[-1]
        data = [datetime.now(),tick,latest_price]
        client.insert('Stock_Data', [data], column_names=['timestamp','name','price'])
        print(f"Latest price for {tick} is: ${latest_price}")

client = connect('localhost','8123','default','')
print(f"Connected to Clickhouse server, starting ingester, ticker ist: {tickers}")
            
while True:
    client = connect('localhost','8123','default','')
    if ((nyse.schedule(start_date=date, end_date=date).empty==False)):
        for tick in tickers:
            execute(tick, client)
    time.sleep(60)

