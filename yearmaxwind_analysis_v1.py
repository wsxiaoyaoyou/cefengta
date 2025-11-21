import datetime
import math
import pandas as pd
import pymysql
import numpy as np
import json
import subprocess
import sys

def cal_c1_c2(N):
    if N < 10:
        c1 = 0.9497 + (1.02057 - 0.9497) * (N - 10) / (15 - 10)
        c2 =0.4952 + (0.5182 - 0.4952) * (N - 10) / (15 - 10)
    elif (N >= 10) & (N < 1000):
        N_known = np.array([10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 250, 500, 1000])
        c1_known = np.array([0.9497, 1.02057, 1.06283, 1.09145, 1.11238, 1.12847, 1.14132, 1.15185, 1.16066, 1.17485, 1.18538, 1.19385, 1.20649, 1.20649, 1.24292, 1.2588, 1.26851])
        c2_known = np.array([0.4952, 0.5182, 0.52355, 0.53066, 0.53522, 0.54034, 0.54332, 0.5463, 0.54853, 0.55208, 0.55477, 0.55688, 0.5586, 0.56002, 0.56002, 0.5724, 0.5745])
        # 使用numpy的interp函数进行线性插值
        c1 = np.interp(N, N_known, c1_known)
        c2 = np.interp(N, N_known, c2_known)
    else:
        c1 = 1.28255
        c2 = 0.57722
    return c1, c2


def cal_genweation(data, wind_name, start_time, end_time):
    time_name = 'Date_Time'
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan)
    data[wind_name] = data[wind_name].astype('float')
    T = pd.DataFrame({time_name: pd.date_range(start=start_time, end=end_time, freq='10T', closed='left')})
    T[time_name] = T[time_name].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    data = pd.merge(data, T, on=time_name, how='outer').sort_values(time_name)
    data.drop_duplicates(subset=time_name, keep='first', inplace=True)
    data.reset_index(inplace=True)
    day = (datetime.datetime.strptime(end_time.split(' ')[0], '%Y-%m-%d') - datetime.datetime.strptime(start_time.split(' ')[0], '%Y-%m-%d')).days
    N = int(day/5)
    c1, c2 = cal_c1_c2(N)
    wind_list = []
    for i in range(0, N*5*24*6, int(5*24*6)):
        wind_list.append(np.nanmax(data.loc[i:i + 24*5*6 - 1, wind_name]))
    mean = np.nanmean(wind_list)
    std = np.nanstd(wind_list)
    a = c1 / std
    u = mean - c2 / a
    vmax = u - 1 / a * math.log(math.log((50*N / (50*N-1))))
    return np.around(vmax, 3)


def read_data_from_sql(cefengta_id, start_time, end_time, wind_name):
    host = 'localhost'
    port = 3306
    user = 'root' #用户名
    password = '123456' # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute("SELECT Date_Time, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (wind_name, cefengta_id, start_time, end_time))
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list
    cursor.close()
    conn.close()
    return data

def yearmaxwind_analysis_v1(cefengta_id,can_name,start_time,end_time,savename):
    # 读取功率曲线
    wind_name = can_name.split('m')[0] + '_WS_AVG'
    cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
    result = cal_genweation(cefengta_data, wind_name, start_time, end_time)
    result_json = {}
    result_json['50yearmaxwind'] = result
    json_str = json.dumps(result_json, indent=4)
    with open(savename, 'w') as f:
        f.write(json_str)


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings("ignore")
    # cefengta_id = 'M003470'
    # can_name = '100m风速'
    # start_time = '2022-05-16'
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/50yearmaxwind.json'
    # 50yearmaxwind_analysis
    # 塔名，通道名称，时间起点， 结果存储名称路径
    # python3 yearmaxwind_analysis_v1.py M003470 100m风速 2022-05-16_00:00 2022-07-16_00:00 /home/xiaowu/share/202311/测风塔系统/接口/50yearmaxwind.json
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        can_name = sys.argv[2]
        start_time = sys.argv[3].replace('_', ' ')
        end_time = sys.argv[4].replace('_', ' ')
        savename = sys.argv[5]

        # 读取功率曲线
        wind_name = can_name.split('m')[0] + '_WS_AVG'
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
        result = cal_genweation(cefengta_data, wind_name, start_time, end_time)
        result_json = {}
        result_json['50yearmaxwind'] = result
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)



