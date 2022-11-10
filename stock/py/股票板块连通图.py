import networkx as nx
import matplotlib.pyplot as plt
import pymysql

db = pymysql.connect(host='172.17.0.1',
                     user='root',
                     password='123456',
                     database='joly',
                     autocommit=True)
cursor = db.cursor()


def getNode():
    sql="select distinct a from (\
select a,b from ( SELECT a.concept as a,b.concept as b FROM stock_concept a\
	left  JOIN stock_concept b ON a.CODE = b.CODE  and a.concept!=b.concept\
    WHERE a.CODE IN ( SELECT CODE FROM rmlb WHERE LEVEL > 0 ) ) tem   where b is not null group by a,b having count(*)>1 order by count(*) desc )x"
    cursor.execute(sql)
    return cursor.fetchall()

def getLink():
    sql= "select A,B from ( select x.*,case when @a!=a or @i=con then @i:=con else 0 end as level,@a:=a from (\
    select a,b,count(*) as con from ( SELECT a.concept as a,b.concept as b FROM stock_concept a\
    left  JOIN stock_concept b ON a.CODE = b.CODE  and a.concept!=b.concept\
    WHERE a.CODE IN ( SELECT CODE FROM rmlb WHERE LEVEL > 0 )  ) tem where b is not null group \
    by a,b having count(*)>1 order by a,count(*) desc )x,(select @a:=null,@c:=0)b )x where level>0 and a not in (\
    select A from (\
    select x.*,case when @a!=a or @i=con then @i:=con else 0 end as level,@a:=a from (\
    select a,b,count(*) as con from ( SELECT a.concept as a,b.concept as b FROM stock_concept a\
    left  JOIN stock_concept b ON a.CODE = b.CODE  and a.concept!=b.concept\
    WHERE a.CODE IN ( SELECT CODE FROM rmlb WHERE LEVEL > 0 )  ) tem where b \
    is not null group by a,b having count(*)>1 order by a,count(*) desc )x,(select @a:=null,@c:=0)b)x WHERE LEVEL>0 group by a having count(*)>1)"
    cursor.execute(sql)
    return cursor.fetchall()

        
def println(data):
    print(data,flush=True)

def subgraph():
    G = nx.Graph()
    pointList=getNode()
    linkList=getLink()
    # 转化为图结构
    for node in pointList:
        #println(node[0])
        G.add_node(node[0])
    for link in linkList:
        #println(link)
        G.add_edge(link[0], link[1])
    # 打印连通子图
    for c in nx.connected_components(G):
       # 输出连通子图
        print("连通子图：", c)

subgraph()