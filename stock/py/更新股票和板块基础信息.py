#更新股票和板块基础信息
import datetime
import os
import time

import akshare as ak
import efinance as ef
import numpy as np
import pandas as pd
import pymysql
import requests
from efinance.common import get_realtime_quotes_by_fs

headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.52',

'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
'Accept-Encoding': 'gzip, deflate',
'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
'Cache-Control': 'max-age=0',
'Host': 'push2his.eastmoney.com',
'Proxy-Connection':'keep-alive'
}

db = pymysql.connect(host='172.17.0.1',
                     user='root',
                     password='123456',
                     database='joly',
                     autocommit=True)
cursor = db.cursor()  

def println(date):
    print(date,flush=True)


#判断是否包含中文
def check_contain_chinese(check_str):
    for ch in check_str:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True     
    return False
    

#判断一个unicode是否是英文字母
def check_all_alphabet(check_str):
    if check_str.isnumeric():
        return False
    for uchar in check_str:
        #if not((u'\u0041' <= uchar<=u'\u005a') or (u'\u0061'<= uchar<=u'\u007a')):
        if uchar=='\\' or uchar=='[':
            return False
    return True

#更新股票基本信息
def insertStockBaseInfo():
    df=ef.stock.get_realtime_quotes().set_index('股票代码', drop=False)
    for row2 in df.iloc:
        ltsz=row2['流通市值']
        if ltsz=='-':
            ltsz=0
        zsz=row2['总市值']
        if zsz=='-':
            zsz=0
        jg=row2['最新价']
        if jg=='-':
            jg=0
        zdf=row2['涨跌幅']
        if zdf=='-':
            zdf=0
        sql="insert into stock_base_info(code,name,zsz,ltsz,jg,zdf) VALUES('"+row2['股票代码']+"','"+row2['股票名称']+"',"+str(zsz)+","+str(ltsz)+","+str(jg)+","+str(zdf)+") \
        ON DUPLICATE KEY UPDATE name = '"+row2['股票名称']+"',zsz="+str(zsz)+",ltsz="+str(ltsz)+",jg="+str(jg)+",zdf="+str(zdf)
        cursor.execute(sql)
        

#更新通信达股票板块信息
def insertStockConceptFromTxd():
    path = '/program/txd/T0002/hq_cache/block_gn.dat'
    path = '/opt/project/stock/data/block_gn.dat'
    println(path)
    f=open(r''+path+'',encoding='utf-8')
    sentimentlist = []
    source='通信达'
    for line in f:
        s = line.strip().split('\t')
        sentimentlist.append(s)
    f.close()
    data=' '.join('%s' %id for id in sentimentlist)
    stock=[]
    concept=''
    for i in data.split(' '):
        if(len(i)>0):
            i=i.strip("']").strip('?')
            if(check_contain_chinese(i) or (check_all_alphabet(i) and len(i)>1)):
                concept=i
                println(concept)
                isupdate=updateConceptSource(concept,'txd')
                db.commit()
                l=getConceptCode(concept,source);
            elif i.isnumeric():
                insertCodeConcept(concept,i,source,l)
    sql="update stock_concept_info c,stock_base_info b set c.name=b.name where c.code=b.code"
    cursor.execute(sql)
    updateAllConcept()


#更新东方财富股票板块信息     
def insertStockConceptFromDfcf():
    concept = '概念板块'
    source='东方财富'
    url='http://push2his.eastmoney.com/api/qt/clist/get?pn=1&pz=1000000&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m%3A90%2Bt%3A3%2Bf%3A%2150&fields=f12%2Cf14%2Cf3%2Cf2%2Cf15%2Cf16%2Cf17%2Cf4%2Cf8%2Cf10%2Cf9%2Cf5%2Cf6%2Cf18%2Cf20%2Cf21%2Cf13%2Cf124%2Cf297'
    res = requests.get(url)
    js=res.json()['data']['diff']
    df = pd.json_normalize(js)
    order=['f12','f14','f3','f2','f15','f16','f17','f4','f8','f10','f9','f5','f6','f18','f20','f21','f13','f124','f297']
    df=df[order]
    data=df.rename(columns={'f12':'股票代码',
    'f14':'股票名称',
    'f3':'涨跌幅',
    'f2':'最新价',
    'f15':'最高',
    'f16':'最低',
    'f17':'今开',
    'f4':'涨跌额',
    'f8':'换手率',
    'f10':'量比',
    'f9':'动态市盈率',
    'f5':'成交量',
    'f6':'成交额',
    'f18':'昨日收盘',
    'f20':'总市值',
    'f21':'流通市值',
    'f13':'市场类型',
    'f124':'更新时间',
    'f297':'最新交易日'})    
    for row in data.iloc:
        name=row['股票名称']
        conceptCode=row['股票代码']
        println(name)
        isupdate=updateConceptSource(name,'dfcf')
        l=getConceptCode(name,source)
        try:
            df = get_realtime_quotes_by_fs(f'b:{conceptCode}').rename(columns={
            '代码': '股票代码',
            '名称': '股票名称',
            })    
            for row2 in df.iloc:
                insertCodeConcept(name,row2['股票代码'],source,l)
        except  Exception as re:
            println(re)       
    sql="update stock_concept_info c,stock_base_info b set c.name=b.name where c.code=b.code"
    cursor.execute(sql)           
    updateAllConcept()
  
#更新同花顺股票板块信息       
def insertStockConceptFromThs():
    source='同花顺'
    data = ak.stock_board_concept_name_ths()
    for row in data.iloc():
        name=row['概念名称']
        println(name)
        isupdate=updateConceptSource(name,'ths')
        l=getConceptCode(name,source)
        try:
            df = ak.stock_board_concept_cons_ths(symbol=name).rename(columns={
                '代码': '股票代码',
                '名称': '股票名称',
            })
            println(df)
            for row2 in df.iloc:
                insertCodeConcept(name,row2['股票代码'],source,l)
        except  Exception as re:
            println(re)
    sql="update stock_concept_info c,stock_base_info b set c.name=b.name where c.code=b.code"
    cursor.execute(sql)           
    updateAllConcept()

def updateAllConcept():
    sql="update stock_concept_info c,concept_source s set c.concept=s.concept where c.concept=s.ths or c.concept=s.txd or c.concept=s.dfcf"
    cursor.execute(sql)


def updateConceptSource(name,field):
    sql="select concept from concept_source where "+field+"='"+name+"'" 
    cursor.execute(sql)
    concept= cursor.fetchone()
    if(concept ==None):
        sql="INSERT INTO concept_source ( concept, "+field+" ) value('"+name+"','"+name+"')ON DUPLICATE KEY UPDATE "+field+" = '"+name+"'"
        cursor.execute(sql)
        return True
    else:
        return False
        
        
def insertCodeConcept(concept,code,source,codeList):
    if(codeList !=None and concept+"_"+code in codeList):
        return False
    else:
        try:
            sql="INSERT INTO `stock_concept_info`(`concept`, `code`,`source` ) VALUES ('"+concept+"', '"+code+"','"+source+"');"
            cursor.execute(sql)
            db.commit()
        except  Exception as re:
            println(codeList)
        return True
        
def getConceptCode(concept,source):
    if(source=='东方财富'):
        sql="select CONCAT_WS('_',c.dfcf,s.code) from stock_concept_info s left join concept_source c on s.concept=c.concept where s.source='东方财富' and c.dfcf='"+concept+"'"
    if(source=='同花顺'):
        sql="select CONCAT_WS('_',c.ths,s.code) from stock_concept_info s left join concept_source c on s.concept=c.concept where s.source='同花顺' and c.ths='"+concept+"'"
    if(source=='通信达'):
        sql="select CONCAT_WS('_',c.txd,s.code) from stock_concept_info s left join concept_source c on s.concept=c.concept where s.source='通信达' and c.txd='"+concept+"'"
    cursor.execute(sql)
    codes= cursor.fetchall()
    l=list()
    for c in codes:
        l.append(c[0])
    return l
    
   

def updateRmlb():
    sql="select code from rmlb where code not like '3%'" 
    cursor.execute(sql)
    codes= cursor.fetchall()
    for cod in codes:
        code=cod[0]
        if(code.startswith('6') or code=='000001'):
            url="http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1&fields2=f53,f59&beg=20220101&end=20500101&rtntype=6&secid=1."+code+"&klt=101&fqt=1"
        else:
            url="http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1&fields2=f53,f59&beg=20220101&end=20500101&rtntype=6&secid=0."+code+"&klt=101&fqt=1"
        res = requests.get(url)
        js=res.json()['data']['klines']
        data=list()
        for de in js:
            data.append(de.split(','))
        df = pd.DataFrame(data,columns=['close','df'],dtype=float)
        
        mxlb = 0
        zt = 0
        te=df['df']
        for t in te.iloc:
            if(t>9.7):
                zt=zt+1
                if(zt>mxlb):
                    mxlb=zt
            else:
                zt=0
        println(code+" "+str(mxlb))
        sql="update rmlb set mxlb="+str(mxlb)+" where code = '"+code+"'"
        cursor.execute(sql)


println("开始更新股票基本信息")
#insertStockBaseInfo()
println("开始更新通信达信息")
#insertStockConceptFromTxd()
println("开始更新东方财富信息")
#insertStockConceptFromDfcf()
println("开始更新同花顺信息")
insertStockConceptFromThs()


#更新连板信息
#updateRmlb()

db.commit()
db.close() 