from datetime import datetime
import pandas as pd
import numpy as np

import requests
# monkeypatch using standard python json module
import json
pd.io.json._json.loads = lambda s, *a, **kw: json.loads(s)

# monkeypatch using faster simplejson module
import simplejson
pd.io.json._json.loads = lambda s, *a, **kw: simplejson.loads(s)

# normalising (unnesting) at the same time (for nested jsons)
pd.io.json._json.loads = lambda s, *a, **kw: pd.json_normalize(simplejson.loads(s))

key = "FILL IN YOUR OWN POLYGON API KEY HERE"

def ts_to_datetime(ts) -> str:
    return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M')

#########################################################        
# FUNCTION
#########################################################        

def f_getVWAP(_ticker, _datefrom, _dateto):
  
    url_ = 'https://api.polygon.io/v2/aggs/ticker/'+_ticker+'/range/1/day/'+_datefrom+'/'+_dateto+'?adjusted=true&sort=asc&limit=500&apiKey='+key
    
    data = json.loads(requests.get(url_).text)
    
    df = pd.json_normalize(data, record_path=['results'])
    
    df['Ticker'] = _ticker
    
    df.rename(columns = {'o':'Open',
                         'h':'High',
                         'l':'Low',
                         'c':'Close',
                         'v':'Volume',
                         'vw':'VWAP',
                         'n':'Trades',
                         't':'DateTimeUnix'}, inplace=True)
    
    df = df[['Volume', 'VWAP', 'Open', 'Close', 'High', 'Low', 'DateTimeUnix', 'Trades','Ticker']]
        
    df['DatetimeUTC'] = pd.to_datetime(df["DateTimeUnix"].apply(lambda x: ts_to_datetime(x)))
    df['DatetimeEst'] = pd.to_datetime(df['DatetimeUTC'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    df['Date'] = pd.to_datetime(df['DatetimeEst']).dt.strftime('%Y-%m-%d')
    
    df['rAvgVolume'] = df['Volume'].rolling(10).mean().shift(1).fillna(0)
    
    # a significant volume day is defined as a day with at least 1M volume and more than 10 times the averages volume over the past 10 days
    df['IsSignificantVolume'] = np.where(((df['Volume'] > df['rAvgVolume'] * 10) & (df['Volume'] > 1000000)), 1, 0)
    
    # calculating the day with the highest ranked relative significant volume day
    df["IsHighest"] = df[df['IsSignificantVolume']==1]["Volume"].rank(ascending=False)
    df["IsHighest"] = np.where(df["IsHighest"]==1,1,0)
    
    df['HighestDate'] = np.where(df['IsHighest'] == 1, df['Date'], np.nan)
    df['HighestDate'].ffill(inplace=True)

    df['HighestVolume'] = np.where(df['IsHighest'] == 1, df['Volume'], np.nan)
    df['HighestVolume'].ffill(inplace=True)
    
    # i would say this is the traditional vwap boulevard
    df['HighestVWAP'] = np.where(df['IsHighest'] == 1, df['VWAP'], np.nan)
    df['HighestVWAP'].ffill(inplace=True)
    
    # calculating the most recent day with a relative significant volume day
    df["IsMostRecent"] = df[df['IsSignificantVolume']==1]["Date"].rank(ascending=False)
    df["IsMostRecent"] = np.where(df["IsMostRecent"]==1,1,0)
    
    df['MostRecentDate'] = np.where(df['IsMostRecent'] == 1, df['Date'], np.nan)
    df['MostRecentDate'].ffill(inplace=True)

    df['MostRecentVolume'] = np.where(df['IsMostRecent'] == 1, df['Volume'], np.nan)
    df['MostRecentVolume'].ffill(inplace=True)
    
    # this is the most recent vwap boulevard
    df['MostRecentVWAP'] = np.where(df['IsMostRecent'] == 1, df['VWAP'], np.nan)
    df['MostRecentVWAP'].ffill(inplace=True)

    return df.tail(1)

df = f_getVWAP('VVOS', '2022-02-27', '2023-02-27')
