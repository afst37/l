import itertools
from collections import Counter
import  pandas as pd
import numpy as np
import json
import time
from multiprocessing import Pool,Queue,Process
import multiprocessing
import psycopg2
from sqlalchemy import create_engine
from io import StringIO
import os


def  duwenjian(q,file):
    print('开始读取%s文件'%file)
    datas = []
    datas0 = []
    # 读取文件的内容并做格式化
    #data1 = pd.read_table(r"E:\\test\010120200413220000", header=None)  ##因为做为函数调用，将读文件步骤调整到主程序
    data1 = pd.read_table(file, header=None)  # 因为做多线程调用，所以将打开文件放回函数
    for i in range(len(data1[0])):
        datas_temp = json.loads(data1[0][i])
        datas.append(json.loads(data1[0][i]))

        for datas_temp in datas[i]:
            datas0.append(
                [(datas_temp['caNo']), (datas_temp['dalTime']), datas_temp['businessTypeNo'], datas_temp['deviceId'],
                 datas_temp['mac'], datas_temp['outIp'], datas_temp['inIp'], datas_temp['projectNo'], datas_temp['recordTime'],
                 datas_temp['regionId'], datas_temp['systemTypeNo']])

    datas1 = pd.DataFrame(datas0)
    output = StringIO()
    #转换数据为csv格式的StringIO
    datas1.to_csv(output, sep=',', index=False, header=False)
    q.put(output)
    print('处理%s数据成功' % file)

def  xieku(q):
    while True: ##函数形成死循环
        #print('写库程序开始')
        if not q.empty():
            conn = psycopg2.connect(host='172.16.11.34', port=5432, database='big', user='username', password='passwd')
            cur = conn.cursor()
            output=q.get()
            # 在用copy_from向数据库导入数据之前一定要将游标复位
            output.seek(0)
            # 用copy_from向数据库导入数据
            cur.copy_from(output, 'big_data2', sep=',', null='\\N', columns=( 'cano', 'daltime', 'businesstypeno',
                 'deviceid', 'mac', 'outip', 'inip', 'projectno', 'recordtime', 'regionid','systemtypeno'))
            conn.commit()
            print('导入数据成功')
            q.task_done()
            #time.sleep(0.5)
    print('退出导入数据')

if __name__ == '__main__':
    manager = multiprocessing.Manager()
    que=manager.Queue()
    time00 = time.perf_counter()
    print('程序开始时间：'+str(time00))
    path = os.path.abspath('.') # 当前文件夹目录下的\temp目录
    print(path)
    files = os.listdir(path+'/temp')  # \temp目录得到文件夹下的所有文件名称
    #设置多进程处理文件数据的格式化
    pool = Pool () #设置进程数
    conn = psycopg2.connect(host='172.16.11.34', port=5432, database='big', user='username', password='passwd')
    cur = conn.cursor()
    #xk=pool.apply_async(xieku,args=(que,))
    xk = Process(target=xieku, args=(que,))
    xk.start()
    for file in files:  # 遍历文件夹
        if not os.path.isdir(file):  # 判断是否是文件夹，不是文件夹才打开
            filename = path +'/temp' + "/" + file  # 打开文件
            pool.apply_async(duwenjian, args=(que,filename,))
            #xk.start()

    pool.close() #关闭进程池
    pool.join()
    que.join()

    xk.terminate()
    time01 = time.perf_counter()
    cur.close()
    conn.close()
    #print('程序结束时间：' + str(time01))
    print('程序总运行时间：' + str(time01-time00))
