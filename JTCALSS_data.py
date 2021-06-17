#导入模块包
import warnings
warnings.filterwarnings('ignore')
import random
import pandas as pd
import multiprocessing
import timeit
from faker import Faker
fake = Faker("zh-CN")
import os

#投资比例大于1的数据文件清理
if os.path.isfile('exchange.csv'):
    os.remove('exchange.csv')
    
#投资关系数据清理
if os.path.isfile('demo_data_final.csv'):
    os.remove('demo_data_final.csv')

#目标客户数据
if os.path.isfile('node_data.csv'):
    os.remove('node_data.csv')
    
#生成控股比例数据
def demo_data_(edge_num):
    s = []
    for i in range(edge_num):
        #投资公司、被投资公司、投资比例、投资时间
        s.append([fake.company(), fake.company(), random.random(), fake.date(pattern="%Y-%m-%d", end_datetime=None)])
    demo_data = pd.DataFrame(s, columns=['start_company', 'end_company', 'weight', 'data_date'])
    print("-----demo_data describe-----")
    print(demo_data.info())
    print("-----demo_data head---------")
    print(demo_data.head())
    return demo_data

#判断DataFrame两列是否相等
def if_same(a, b):
    if a==b:
        return 1
    else:
        return 0

#demeo数据处理        
def rela_data_(demo_data):
    print('原始数据记录数', len(demo_data))
    #去除自投资
    demo_data['bool'] = demo_data.apply(lambda x: if_same(x['start_company'], x['end_company']), axis=1)
    demo_data = demo_data.loc[demo_data['bool'] != 1]
    #去除非空
    demo_data = demo_data[(demo_data['start_company'] != '')&(demo_data['end_company'] != '')]
    #按照日期排序删除重复start_company、end_company项
    demo_data = demo_data.sort_values(by=['start_company', 'end_company', 'data_date'], ascending=False).drop_duplicates(keep='first', subset=['start_company', 'end_company']).reset_index()

    #删除多条大于0.5且保留最新值
    demo_data = pd.concat([demo_data.loc[demo_data['weight'] <= 0.5], demo_data.loc[demo_data['weight'] > 0.5].sort_values(by=['end_company', 'data_date'], ascending=False).drop_duplicates(keep='first', subset=['end_company', 'weight'])]).reset_index()[['start_company', 'end_company', 'weight', 'data_date']]
    
    #此时的demo_data_init用来归一化操作
    global demo_data_init
    demo_data_init = demo_data.copy()

    #持股比例求和
    demo_data_sum = demo_data[['end_company', 'weight']].groupby(['end_company']).sum()
    #持股比例大于1的index
    more_one_index = demo_data_sum.loc[demo_data_sum['weight']>1].index.unique()
    print('持股比例大于1的index', len(more_one_index))
    
    #并行处理持股比例大于1的数据归一化
    #liunx中可以执行，windows上执行报错
    items = more_one_index[:]
    p = multiprocessing.Pool(32)
    start = timeit.default_timer()
    b = p.map(do_something, items)
    p.close()
    p.join()
    end = timeit.default_timer()
    print('multi processing time:', str(end-start), 's')
    #持股比例大于1后的归一化结果
    base_more_one = pd.read_csv('exchange.csv', header=None)
    base_more_one.columns = ['start_company', 'end_company', 'weight', 'data_date']

    #持股比例不大于1的index
    low_one_index = demo_data_sum.loc[demo_data_sum['weight']<=1].index
    base_low_one = pd.merge(demo_data, pd.DataFrame(low_one_index), on = ['end_company'], how = 'inner')
    demo_data_final = pd.concat([base_low_one, base_more_one]).reset_index()[['start_company', 'end_company', 'weight', 'data_date']].drop_duplicates()
    print('数据处理后记录数', len(demo_data_final))
    demo_data_final.to_csv('demo_data_final.csv', index = False)
    return demo_data_final
    
#并行处理函数
def do_something(i):
    #大于1的pd
    exchange = demo_data_init.loc[demo_data_init['end_company'] == i].sort_values(by=['end_company', 'data_date'], ascending=False)
    #fundedratio
    weight_sum = sum(exchange['weight'])
    exchange['weight'] = exchange['weight']/weight_sum
    exchange.to_csv('exchange.csv', encoding = 'utf-8', index = False, header = 0, mode = 'a')
    print('-----End of The',i,'-----')
#节点数据
def node_data_(node_num):
    cust_list = [fake.company() for i in range(node_num)]
    node_data = pd.DataFrame(cust_list, columns=['cust_id']).drop_duplicates()
    print('节点数目', len(node_data['cust_id'].unique()))
    node_data.to_csv('node_data.csv', index = False)
    
if __name__ == '__main__':
    #edge_num样本关系条数
    demo_data = demo_data_(edge_num=10000)
    rela_data_(demo_data)
    #node_num样本节点条数
    node_data_(node_num=5000)