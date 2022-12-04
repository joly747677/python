import numpy as np
import pandas as pd
import pymysql
import time
import os
from datetime import datetime
import requests


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


    
#更新连板情况
def get_stock_lb_df():
    zxjyr=allhq.iloc[0]['最新交易日']
    time=datetime.strptime(str(zxjyr),'%Y%m%d')
    zxjyr=time.strftime('%Y-%m-%d')
    update_stock_lb_zr(zxjyr)
    update_stock_lb_d()
    for row2 in allhq.iloc:
        if(row2['涨跌幅']!='-' and (float(row2['涨跌幅'])>9.8 or (float(row2['涨跌幅'])>4.8 and ('ST' in row2['股票名称'])))):
            #更新连板
            update_stock_lb_jr(row2['股票代码'],zxjyr)
        if(row2['涨跌幅']!='-' and (float(row2['涨跌幅'])<(-9.8) or (float(row2['涨跌幅'])<(-4.8) and ('ST' in row2['股票名称'])))):
            #更新连板
            update_stock_lb_dt(row2['股票代码'])
    if(datetime.now().hour>7):
        sql = "update rmlb set mxlb=level where level>mxlb or mxlb is null"
        cursor.execute(sql)
    db.commit()
    
#判断是否更新昨日连板
def update_stock_lb_zr(zxjyr):
    sql="select min(date) from rmlb"
    cursor.execute(sql)
    date= cursor.fetchone()
    time=datetime.strptime(zxjyr,'%Y-%m-%d')
    n=int(time.strftime('%Y%m%d'))
    ls=int(date[0].strftime('%Y%m%d'))
    if n>ls:
        sql = "update rmlb set zrLevel=0;"
        cursor.execute(sql)
        sql = "update rmlb set zrLevel=level where level>0;"
        cursor.execute(sql)

#更新股票断板
def update_stock_lb_d():
    sql = "update rmlb set level=0,date=now(),isDt=False"
    cursor.execute(sql)
    sql = "update rmlb set lastDate=null where level>0"
    cursor.execute(sql)
    

def getMyStockList():
    sql="select code,name from my_stock where isHave=True"
    cursor.execute(sql)
    return cursor.fetchall()
    
def getData2(code,ismy,ismm):
    row2=allCode.get(code)
    jg=row2[2]
    op=row2[5]
    #获取实时均价
    cje=row2[11]
    cjl=row2[10]
    if cjl=='-' or cjl==0:
        return
    jj=round(cje/cjl/100,3)
    df=row2[1]
    name=row2[0]
    if(len(name)==3):
        name=name+'  '
    getQs(code,jg,name,jj,ismy,df,op,ismm)
        
        
def getAllHq():
    url="http://push2his.eastmoney.com/api/qt/clist/get?pn=1&pz=1000000&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m%3A0%2Bt%3A6%2Cm%3A0%2Bt%3A80%2Cm%3A1%2Bt%3A2%2Cm%3A1%2Bt%3A23%2Cm%3A0%2Bt%3A81%2Bs%3A2048&fields=f12,f14,f3,f2,f15,f16,f17,f4,f8,f10,f9,f5,f6,f18,f20,f21,f13,f124,f297"
    res = requests.get(url,headers=headers)
    js=res.json()['data']['diff']
    df = pd.json_normalize(js)
    order=['f12','f14','f3','f2','f15','f16','f17','f4','f8','f10','f9','f5','f6','f18','f20','f21','f13','f124','f297']
    df=df[order]
    df=df.rename(columns={'f12':'股票代码',
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
    return df

def getAllData(code):
    if( code=='000001' and code in alldata):
        del alldata[code]
    if not (code in alldata):
        ##f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61
        ##'time','open','close','hight','low','jyl','jyze','zf','zdf','zde','hsl'
        url="http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1&fields2=f51%2Cf52%2Cf53%2Cf59&beg=19000101&end=20500101&rtntype=6&secid=";
        if(code.startswith('6') or code=='000001'):
            url=url+"1."+code+"&klt=101&fqt=1"
        else:
            url=url+"0."+code+"&klt=101&fqt=1"
        res = requests.get(url,headers=headers)
        js=res.json()['data']['klines']
        data=list()
        for de in js:
            data.append(de.split(','))
        df = pd.DataFrame(data,columns=['time','open','close','df'])
        df[['open','close','df']] = df[['open','close','df']].astype(float)
        alldata.setdefault(code,df)
    return alldata.get(code)
    
    
def getStatus(ma5,ma10,ztMa10):
    c=( ma5 - ma10 ) / ( ma5 + ma10 ) * 400;
    if(ma10>ztMa10):
        if(c <- 1.618):
            return 'heng'
        else:    
            return 'up'
    elif (ma10 < ztMa10):
        if(c > 1.618):
            return 'heng'
        else:
            return 'down'
    return 'heng'


#获取当前趋势
def getQs(code,jg,name,jj,ismy,df,op,ismm):
    try:
        data=getAllData(code)
    except  Exception as re:
        println(re)
        return None       
    ind=len(data)-1
    if(jg!=None):
        data.loc[ind,'close']=jg
    if(df!=None):
        data.loc[ind,'df']=df
    
    data['ma5']=data['close'].rolling(5).mean()
    data['ma10']=data['close'].rolling(10).mean()
    data['ema10']=data['close'].ewm(span=10,adjust=False).mean()
    data['ema23']=data['close'].ewm(span=23,adjust=False).mean()
    data['dif']=data['ema10']-data['ema23']
    data['dea']=data['dif'].ewm(span=8,adjust=False).mean()
    data['macd']=(data['dif']-data['dea'])*2
    data['macd3']=data['macd'].rolling(3).mean()
    
    
    ma10=data['ma10'][ind]
    dif=data['dif'][ind]
    #昨天10日均线
    ztMa10=data['ma10'][ind-1]
    macd=data['macd'][ind]
    macd3=data['macd3'][ind]
    ma5=data['ma5'][ind]
    #昨日macd<macd3
    zrMacd=data['macd'][ind-1]
    zrMacd3=data['macd3'][ind-1]
    qs=getStatus(ma5,ma10,ztMa10)    
    jg=data.loc[ind,'close']
    df=data.loc[ind,'df']
    t2=data.loc[ind-1,'close']
    t3=data.loc[ind-2,'close']
    t4=data.loc[ind-3,'close']
    if qs!='heng' and jg!=0:
        c=(abs(jg-t2)+ abs(t2-t3)+abs(t3-t4))*200/jg
        if(c<1.5):
            qs='heng'  
    le=jsZsyh(name,jg,code,macd,macd3,df,dif,ismy,op,zrMacd,zrMacd3)
    
    if(ismy):
        qc(code,name,jg,df,data)
    
    if(ismm):
        jsdqjg(jg,jj,name,qs,code,df,le)
    
    return qs
    
    
    

#如果价格低于最近一次阳线的开盘价建议清仓
def qc(code,name,jg,zdf,data):
    if(zdf>0 or datetime.now().hour<7):
        return
    ind=len(data)
    i=2
    while i<ind:
        nd=ind-i
        df=data.loc[nd,'df']
        if(df>0):
            op=data.loc[nd,'open']
            if(op>jg):
                time=data.loc[nd,'time']
                println(code+name+'价格'+str(jg)+'低于'+time+'开盘价'+str(op)+'建议清仓')
            return
        i=i+1
    
    
    
    
    
    
#判断最近是否涨跌停过
def isZt(data,n):
    te=data.loc[len(data)-n:len(data),'df']
    for t in te.iloc:
        if(t>9.7 or t<-9.7):
            return True
    return False
    
#计算区间  
def jsZsyh(name,jg,code,macd,macd3,df,dif,ismy,op,zrMacd,zrMacd3):
    concept=stockbk(code)
    pf=getLevel(code)
    le=0
    if dif>0:
        if macd>0:
            q='A'
            le=le+2
        else:
            q='B'
            le=le-1
    else:
        if macd>0:
            q='D'
            le=le+1
        else:
            q='C'
            le=le-2

    tt=False
    if(macd>macd3):
        zr=zrMacd-zrMacd3
        jr=macd-macd3
        if zr<0:
            t='上车'
            le=le+1
            tt=True
        else:
            if(jr>zr):
                t='持仓'
                le=le+2
                tt=True
            else:
                t='减仓'
                le=le-1
    else:
        t='清仓'
        le=le-3
    if(le>=0):
        t=t+str((le+1)*2000)
    else:
        t=t+"  卖卖卖卖卖"
    
    if(tt or ismy):
        println(code+name+'【'+q+'区                                '+t+'】现价【'+str(jg)+'】【'+str(df)+'】'+'【评分:'+str(pf)+'】'+','.join('%s' %a for a in concept))


    


#当前股票属于哪个板块            
def stockbk(code):
    sql="select distinct concept from  stock_concept where code ='"+code+"' and concept in ('"+"','".join(bk)+"')"
    cursor.execute(sql)
    concept= cursor.fetchall()

    l=list()
    for c in concept:
        l.append(c[0])
    if(len(bk)==10):
        rule = {bk[0]:0,bk[1]:1,bk[2]:2,bk[3]:3,bk[4]:4,bk[5]:5,bk[6]:6,bk[7]:7,bk[8]:8,bk[9]:9}
        l=sorted(l, key=lambda x:rule[x])
    return l

def getLevel(code):
    sql="SELECT sum( `t`.`ztlevel` ) + ( ( sum( ( `t`.`zdf` * `t`.`ltsz` ) ) / sum( `t`.`ltsz` ) ) / 3 ) AS `level` \
        FROM `stock_concept` `s` LEFT JOIN `top_concept` `t` ON `s`.`concept` = `t`.`concept` \
        where code ='"+code+"'"
    cursor.execute(sql)
    pf= cursor.fetchone()
    if(pf==None or pf[0]==None):
        return 0
    else:
        return ('%.2f' % pf[0])
    
#计算当日区间    
def jsdqjg( jg,jj,name,qs,code,df,le):
    jj=float(jj)
    j1 = jj * 1.01809;
    j2 = jj * 1.01191;
    j3 = jj * 0.98809;
    j4 = jj * 0.98191;
    if ((float(jg) >= j1 and df<9.95 )) :
        println(code+name+'【'+qs+'******************************当日必卖】现价【'+str(jg)+'】【'+str(df)+'】')
    if (float(jg) <= j4 ):
        println(code+name+'【'+qs+'******************************当日必买】现价【'+str(jg)+'】【'+str(df)+'】')
        
def println(data):
    print(data,flush=True)
        
             
       
def getStockList(isMy):
    if(isMy):
        #sql="select * from top where code in (select code from my_stock) and \
#CODE IN ( SELECT CODE FROM stock_concept WHERE concept IN ( select concept from (SELECT * FROM top_concept where al<100 limit 1 )a) ) "
        sql="select code,name from my_stock where isHave=2"
    else:
        sql="select code,name from top limit 20"
    #sql="select code,name from my_stock where code in (select code from stock_concept_info where concept in ('"+concept[0]+"'))"
    #sql="SELECT CODE,NAME FROM top WHERE CODE IN (SELECT CODE FROM	stock_concept WHERE	 concept IN ( '"+"','".join(concept)+"')) limit 40"
        #AND NAME NOT LIKE '%ST%' 	AND CODE NOT LIKE '3%' and code not like '68%' \
        #AND CODE NOT IN ( SELECT CODE FROM stock_concept WHERE concept IN ( '转债标的' ) ) \
       #ORDER BY LEVEL DESC"
    cursor.execute(sql)
    return cursor.fetchall()
    
#最近半年涨停最多的股票    
def getLongTou():
    sql="select b.*,a.* from (select code,count(*) as con,min(close) as mi from stock_history where hight/low>1.1 and time>DATE_SUB(CURDATE( ),INTERVAL 4 month) group by code ) a left join stock_base_info b on a.code=b.code  where a.code not like '3%'order by con desc limit 30"
    cursor.execute(sql)
    return cursor.fetchall()
    
    
#获取连板最多10大板块    
def get_stock_bk_lb():
    sql="select concept from top_concept limit 10"
    cursor.execute(sql)
    concept= cursor.fetchall()
    l=list()
    for c in concept:
        l.append(c[0])
    return l
 
def get_dt_count(code):
    sql="select count(*) from rmlb where isdt=true and code in ( select code from stock_concept where concept in(\
        select concept from stock_concept where code ='"+code+"')\
        AND concept NOT IN ( SELECT CONCEPT FROM ( SELECT concept FROM top_concept LIMIT 10 ) a ))  "
    cursor.execute(sql)
    return cursor.fetchone()
    
    
def insertStockBaseInfo():
    h=datetime.now().hour
    m=datetime.now().minute
    if(h==1 and m<30):
        sql='update stock_base_info set zdf=0'
        cursor.execute(sql)
    #if(h<1 or (h==1 and m<30) or h>7):
    #    return
    for row2 in allhq.iloc:
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
        
    
    
#更新股票今日连板
def update_stock_lb_jr(code,zxjyr):
    sql = "INSERT INTO rmlb ( NAME, CODE, LEVEL,lastDate) (SELECT NAME,CODE,1,now() FROM stock_base_info \
            WHERE CODE ='"+code+"' limit 1) ON DUPLICATE KEY UPDATE LEVEL= zrLevel+1,lastDate='"+zxjyr+"'"
    cursor.execute(sql)
 
#更新今日跌停
def update_stock_lb_dt(code):
    sql = "INSERT INTO rmlb ( NAME, CODE, isDt ) (SELECT NAME,CODE,TRUE FROM stock_base_info \
            WHERE CODE ='"+code+"' limit 1) ON DUPLICATE KEY UPDATE isDt= TRUE"
    cursor.execute(sql)

def run(stocks,ismy,ismm):
    for stock in stocks:
        getData2(stock[0],ismy,ismm)


bk=[]
allhq=pd.DataFrame()
alldata=dict()
allCode=dict()
while True: 
    println(datetime.now())
    getQs('000001',None,'上证指数',None,True,None,None,False)
    println('*******************上证指数仓位控制建议****************************')
    out=True
    while out:
        try:
            allhq=getAllHq()
            out=False
        except  Exception as re:
            println(re)
            out=True
    allCode = allhq.set_index('股票代码').T.to_dict('list')    
    get_stock_lb_df()
    insertStockBaseInfo()
    #获取净流入最多10大板块
    bk=get_stock_bk_lb()
    myStocks=getMyStockList()    
    run(myStocks,True,True)
    #println('最近清仓')
    #stocks=getStockList(True)
    #run(stocks,False,False)
    println('龙头股:')
    stocks=getLongTou()
    run(stocks,False,False)
    println('热门概念:'+','.join(bk))
    stocks=getStockList(False)
    run(stocks,False,False)
    println("严格按照提示操作，上车 持仓，早买午卖，减仓清仓，早卖午买.加速阶段持仓，反转阶段做t")
    
cursor.close()   
db.close()   