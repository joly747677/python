import numpy as np
import pandas as pd
import pymysql
import os
from datetime import datetime
import datetime as dt
import requests
import time


db = pymysql.connect(host='172.17.0.1',
                     user='root',
                     password='123456',
                     database='joly',
                     autocommit=True)
cursor = db.cursor()        



headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.52',

'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
'Accept-Encoding': 'gzip, deflate',
'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
'Cache-Control': 'max-age=0',
'Host': 'push2his.eastmoney.com',
'Proxy-Connection':'keep-alive'
}



def println(data):
    print(data,flush=True)

def js(code):
    println(code)
    try:
        data=getAllData(code)
    except  Exception as re:
        return None  
    data['ma5']=data['close'].rolling(5).mean()
    data['ma10']=data['close'].rolling(10).mean()
    data['ema10']=data['close'].ewm(span=10,adjust=False).mean()
    data['ema23']=data['close'].ewm(span=23,adjust=False).mean()
    data['dif']=data['ema10']-data['ema23']
    data['dea']=data['dif'].ewm(span=8,adjust=False).mean()
    data['macd']=(data['dif']-data['dea'])*2
    data['macd3']=data['macd'].rolling(3).mean()
    data['q']=(data['macd']-data['macd3'])
    data['q2']=(data['q'].rolling(2).mean())*2
    data['p']=data['q2']-data['q']
    data['bian']= data['q']*data['p']
    date=getLastTime(code)
    for i in data.iloc:
        time=datetime.strptime(i['time'],'%Y-%m-%d')
        if int(time.strftime('%Y%m%d'))>=int(date) :
            insertHistory(code,i)

def getLastTime(code):
    sql="select time from stock_history where code='"+code+"' order by time desc limit 1"
    cursor.execute(sql)
    da= cursor.fetchone()
    if da==None:
        return '20211029'
    else:
        tomorrow = da[0] + dt.timedelta(days=1)
        return tomorrow.strftime('%Y%m%d')

def deleteHistory(code):
    sql="delete from  stock_history where code='"+code+"'"
    cursor.execute(sql)  

def insertHistory(code,data):
    macd3=data['macd3']
    if(macd3==None or str(macd3)=='nan'):
        macd3=0
    ma10=data['ma10']
    if(ma10==None or str(ma10)=='nan'):
        ma10=0
        
    bian=data['bian']
    if(bian<0):
        bian=1
    else:
        bian=0
        
        
    sql="INSERT INTO `joly`.`stock_history`(`code`, `time`, `open`, `close`, `hight`, `low`, `dif`, `macd`, `jyl`, `jyze`, `zf`, `zdf`, `zde`, `hsl`,`macd3`,`ma10`,`change`) VALUES \
    ('"+code+"', '"+data['time']+"', "+str(data['open'])+", "+str(data['close'])+", "+str(data['hight'])+",\
    "+str(data['low'])+", "+str(data['dif'])+", "+str(data['macd'])+", "+str(data['jyl'])+", "+str(data['jyze'])+",\
    "+str(data['zf'])+", "+str(data['zdf'])+", "+str(data['zde'])+", "+str(data['hsl'])+","+str(macd3)+","+str(ma10)+","+str(bian)+");"
    cursor.execute(sql)
    db.commit()


def getStockList():
    #sql="select code from stock_base_info a where code='601258'"
    sql="select code from stock_base_info a where 1=1\
	AND ( NOT ( ( `a`.`code` LIKE '68%' ) ) ) \
	AND ( NOT ( ( `a`.`code` LIKE '8%' ) ) ) \
	AND ( NOT ( ( `a`.`name` LIKE '%ST%' ) ) ) and jg>0 \
    and code not in(select code from stock_history where time=CURRENT_DATE())\
    order by jg desc"
    cursor.execute(sql)
    return cursor.fetchall()



def getAllData(code):
    date='20211029'
    if(code.startswith('6') or code=='000001'):
        url="http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&beg="+date+"&end=20500101&rtntype=6&secid=1."+code+"&klt=101&fqt=1"
    else:
        url="http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&beg="+date+"&end=20500101&rtntype=6&secid=0."+code+"&klt=101&fqt=1"
    res = requests.get(url,headers=headers)
    klines=res.json()['data']['klines']
    data=list()
    g=list()
    for de in klines:
        data.append(de.split(','))
    df = pd.DataFrame(data,columns=['time','open','close','hight','low','jyl','jyze','zf','zdf','zde','hsl'])
    df[['open','close','hight','low','jyl','jyze','zf','zdf','zde','hsl']] = df[['open','close','hight','low','jyl','jyze','zf','zdf','zde','hsl']].astype(float)
    return df

    

myStocks=getStockList()
for stock in myStocks:
    js(stock[0])

cursor.close()    
db.close()   