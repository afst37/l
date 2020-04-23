import itertools
from collections import Counter
import pandas as pd
import numpy as np
import json
import time

import psycopg2
from sqlalchemy import create_engine
from io import StringIO
import os


def duwenjianxieku():
    # time0 = time.perf_counter()
    # print(time0)
    datas = []
    datas0 = []
    # 读取文件的内容并做格式化
    # data1 = pd.read_table(r"E:\\test\010120200413220000", header=None)  ##因为做为函数调用，将读文件步骤调整到主程序
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
                 datas_temp['mac'], datas_temp['outIp'], datas_temp['inIp'], datas_temp['projectNo'],
                 datas_temp['recordTime'],
                 datas_temp['regionId'], datas_temp['systemTypeNo']])

    # 将列表转化为pandas的格式，方便输出特定数
    datas1 = pd.DataFrame(datas0)
    # print('测试数据的格式正确性')
    # print(datas1.iloc[0:3, 2:4]) #测试数据的格式正确性
    output = StringIO()

    datas1.to_csv(output, sep=',', index=False, header=False)
    # datas1.to_csv('output.csv', sep=',',index=False, header=False)
    output1 = output.getvalue()
    # 数据格式化时间time1
    # time1 = time.perf_counter()
    # print('数据格式化所需时间time1：')
    # print(time1 - time0)

    '''
    将打开关闭数据库程序部分转移到主程序，以节约程序运行时间

    conn = psycopg2.connect(host='172.16.11.34', port=5432, database='big', user='username', password='passwd')
    cur = conn.cursor()
    print("Opened database successfully")


    '''
    # 在用copy_from向数据库导入数据之前一定要将游标复位
    output.seek(0)
    # 用copy_from向数据库导入数据
    cur.copy_from(output, 'big_data3', sep=',', null='\\N', columns=(
        'cano', 'daltime', 'businesstypeno', 'deviceid', 'mac', 'outip', 'inip', 'projectno', 'recordtime', 'regionid',
        'systemtypeno'))
    time2 = time.perf_counter()
    # print('数据格式化所需时间time2：')
    # print(time2 - time1)
    # print('done copying')
    conn.commit()
    print('导入%s数据成功' % file)
    '''
    cur.close()
    conn.close()
    print('导入数据成功！')
    '''
    return


if __name__ == '__main__':
    time00 = time.perf_counter()
    print('程序开始时间：' + str(time00))
    path = os.path.abspath('.')  # 当前文件夹目录下的\temp目录
    print(path)
    files = os.listdir(path + '/temp')  # \temp目录得到文件夹下的所有文件名称
    s = []
    conn = psycopg2.connect(host='172.16.11.34', port=5432, database='big', user='username', password='passwd')
    cur = conn.cursor()
    for file in files:  # 遍历文件夹
        if not os.path.isdir(file):  # 判断是否是文件夹，不是文件夹才打开
            data1 = pd.read_table(path + '/temp' + "/" + file, header=None)  # 打开文件
            duwenjianxieku()
    time01 = time.perf_counter()
    cur.close()
    conn.close()
    print('程序结束时间：' + str(time01))
    print('程序总运行时间：' + str(time01 - time00))




