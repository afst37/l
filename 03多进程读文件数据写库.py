import itertools
from collections import Counter
import  pandas as pd
import numpy as np
import json
import time
from multiprocessing import Pool,Process,Queue
import psycopg2
from sqlalchemy import create_engine
from io import StringIO
import os


def  duwenjian(file):
    #time0 = time.perf_counter() ##函数开始时间
    print('程序开始读文件%s'%file)
    #print(time0)
    datas = []
    datas0 = []
    # 读取文件的内容并做格式化
    #data1 = pd.read_table(r"E:\\test\010120200413220000", header=None)  ##因为做为函数调用，将读文件步骤调整到主程序
    data1 = pd.read_table(file, header=None)  # 因为做多线程调用，所以将打开文件放回函数
    for i in range(len(data1[0])):
        datas_temp = json.loads(data1[0][i])
        datas.append(json.loads(data1[0][i]))
        # print('i:' + str(i))
        # print('datas_temp:'+str(datas_temp))
        # print('datas[i]'+str(datas[i]))
        for datas_temp in datas[i]:
            # print('i-1:'+str(i-1))
            # print(datas_temp['caNo'])
            # print(datas_temp['businessTypeNo'])
            # print(datas_temp['data'])
            # 丢弃data列数据
            datas0.append(
                [(datas_temp['caNo']), (datas_temp['dalTime']), datas_temp['businessTypeNo'], datas_temp['deviceId'],
                 datas_temp['mac'], datas_temp['outIp'], datas_temp['inIp'], datas_temp['projectNo'], datas_temp['recordTime'],
                 datas_temp['regionId'], datas_temp['systemTypeNo']])

    # 将列表转化为pandas的格式，方便输出特定数
    datas1 = pd.DataFrame(datas0)
    print('测试数据的格式正确性')
    #print(datas1.iloc[0:3, 2:4]) #测试数据的格式正确性
    # datas1.to_csv('output.csv', sep=',',index=False, header=False) #测试数据的格式正确性
    # 数据格式化时间time1
    output = StringIO()
    datas1.to_csv(output, sep=',', index=False, header=False)
    #在用copy_from向数据库导入数据之前一定要将游标复位
    output.seek(0)
    # 用copy_from向数据库导入数据
    cur.copy_from(output, 'big_data3', sep=',', null='\\N', columns=(
    'cano', 'daltime', 'businesstypeno', 'deviceid', 'mac', 'outip', 'inip', 'projectno', 'recordtime', 'regionid',
    'systemtypeno'))
    conn.commit()
    print('导入%s数据成功' %file)


if __name__ == '__main__':

    time00 = time.perf_counter()
    print('程序开始时间：'+str(time00))
    path = os.path.abspath('.') # 当前文件夹目录下的\temp目录
    print(path)
    files = os.listdir(path+'/temp')  # \temp目录得到文件夹下的所有文件名称

    pool = Pool () #设置进程数
    conn = psycopg2.connect(host='172.16.11.34', port=5432, database='big', user='username', password='passwd')
    cur = conn.cursor()
    for file in files:  # 遍历文件夹
        if not os.path.isdir(file):  # 判断是否是文件夹，不是文件夹才打开
            filename = path +'/temp' + "/" + file  # 打开文件
            pool.apply_async(duwenjian, (filename,))
    pool.close() #关闭进程池
    pool.join()

    time03 = time.perf_counter()
    cur.close()
    conn.close()
    print('程序结束时间：' + str(time03))
    print('程序总运行时间：' + str(time03-time00))




