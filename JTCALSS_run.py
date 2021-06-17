#导入模块包
import warnings
warnings.filterwarnings('ignore')
import random
import pandas as pd
import time
import multiprocessing
import networkx as nx
import os
import numpy as np
import timeit

#获取关系、目标客户数据
#目标客户
rela_data = pd.read_csv('demo_data_final.csv')
node_data = pd.read_csv('node_data.csv')
global cust_list
cust_list = node_data['cust_id'].unique()
print('目标客户数', len(cust_list))
    
#构造有向图
def graph_(rela_data):
    Graph = nx.DiGraph()
    for indexs in rela_data.index:
        Graph.add_weighted_edges_from([tuple(rela_data.loc[indexs].values)])
    return Graph
global Graph
Graph = graph_(rela_data[['start_company', 'end_company', 'weight']].drop_duplicates())
print('图中节点数目', Graph.number_of_nodes())
print('图中关系数目', Graph.number_of_edges())

#目标客户与Graph分析
def analyze_node():
    s = []
    for i in cust_list[:]:
        if Graph.has_node(i):
            whether = 1
            count = len(nx.single_source_shortest_path_length(Graph,source=i,cutoff=5))
        else:
            whether = 0
            count = 0
        s.append([i, whether, count])
    zero = pd.DataFrame(s)
    zero.columns = ['name', 'whether', 'count']
    print('在图中无控股', len(zero.loc[zero['count']==1]))
    print('不在图中', len(zero.loc[zero['whether']==0]))


def DistanceMatrix(Graph, point, n_step):
    if Graph.has_node(point): 
        results = nx.single_source_shortest_path_length(Graph, source=point, cutoff= n_step)   
    nodelist = list(results.keys())
    if len(nodelist) > 30000: 
        print("subgraph type",len(nodelist)," It is big" )
        return len(nodelist)    
    if len(nodelist) <= 1: 
        print("subgraph type",len(nodelist)," It is small" )
        return len(nodelist)      
    #获取距离矩阵
    dm = nx.to_pandas_adjacency(Graph.subgraph(nodelist))
    return dm

def LinkPredict(dm, point, n_step):
    start = pd.DataFrame(0, index=[point], columns=dm.columns)
    start.loc[point,point] = 1
    #获取间接控股比例矩阵
    def sum_involution(ma, n_step):
        #衰减参数
        C = 1
        mab = ma
        result = ma
        for _ in range(n_step-1):
            ma = round(ma.dot(mab), 6)
            np.fill_diagonal(ma.values,0,wrap=True)
            result = result + C*ma
        return result            
    mm = sum_involution(dm, n_step)
    #获取point的间接持股比例
    start = start.dot(mm).T.astype('float').round(8)
    start.drop(point, axis=0, inplace=True)
    start.sort_values(by=point, inplace=True, ascending=False)
    return [v for k,v in start.to_dict().items()].pop()

#投资比例大于1的数据文件清理
if os.path.isfile('result.csv'):
    os.remove('result.csv')


#全局变量，在并行处理中会使用
global no_list
global result_file_path
result_file_path = 'result.csv'
no_list = []
def LinkResult(point):
    #判断目标客户是否在Graph中
    if Graph.has_node(point):
        print('----',point)
        try:
            #获取距离矩阵
            dm = DistanceMatrix(Graph, point, n_step=5)
            #获取目标客户对关联客户的（间接）持股比例
            Link = LinkPredict(dm, point, 5)
            #持股比例数据处理
            if len(Link) > 0:
                #目标客户、关联客户、（间接）持股比例
                Link = pd.DataFrame([(point,r,v) for r,v in Link.items()],columns = ['start','target','link'])
                Link['rank'] = Link['link'].rank(method='min',ascending=False).astype('int')
                #增加一个排名
                Link['rank'] = [int(q) for q in Link['rank']]
                #结果报错，追加的形式，脚本运行前要提前删除
                Link.to_csv(result_file_path, encoding='utf-8', index=False, header=0, mode='a')
        except Exception as e:
            print(point, e)
            Link = pd.DataFrame([[point, '', '', dm]], columns = ['start', 'target', 'link', 'rank'])
            Link.to_csv(result_file_path, encoding='utf-8', index=False, header=0, mode='a')
    else:
        no_list.append(point)
# revise to parallel
items = cust_list[:5]
p = multiprocessing.Pool(32)
start = timeit.default_timer()
b = p.map(LinkResult, items)
p.close()
p.join()
end = timeit.default_timer()
print('multi processing time:', str(end-start), 's')