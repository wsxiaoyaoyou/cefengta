import pandas as pd
import pymysql
import numpy as np
from scipy import interpolate
import json
import subprocess
import sys


def wind_to_power_powercure(fwind_power, wind_f, ws_in, ws_out, time_fz):
    if wind_f > ws_out or wind_f < ws_in:
        p = 0
    else:
        power_new = fwind_power(wind_f)
        p = power_new * time_fz / 60
    return np.nansum(p)


def cal_genweation(data, wind_name, can, powercure, zhejian):
    time_name = 'Date_Time'
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    fwind_power = interpolate.interp1d(powercure['wind'], powercure['power'])
    data['gen'] = data[wind_name].apply(lambda x: wind_to_power_powercure(fwind_power, x, np.nanmin(powercure['wind']), np.nanmax(powercure['wind']), 10) * zhejian)
    if can == '日':
        data['time'] = data[time_name].apply(lambda x: x[:10])
    elif can == '月':
        data['time'] = data[time_name].apply(lambda x: x[:7])
    elif can == '年':
        data['time'] = data[time_name].apply(lambda x: x[:4])
    result = data.groupby('time')[['gen']].sum()
    result['gen'] = result['gen'].apply(lambda x: np.around(x/1000, 3))
    result.reset_index(inplace=True)
    return result


def read_data_from_sql(cefengta_id, start_time, end_time, wind_name):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
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

def read_powercure_from_sql(powercure_type):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute("SELECT wind, power FROM cefengta.powercure where type ='%s';" % powercure_type)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list
    cursor.close()
    conn.close()
    return data

def generation_analysis(cefengta_id,can_name,start_time,end_time,powercure_type,zhejian,key_value,savename):
    wind_name = can_name.split('m')[0] + '_WS_AVG'
    powercure = read_powercure_from_sql(powercure_type)
    powercure = powercure.replace('None', np.nan).astype('float')
    powercure.sort_values(by='wind', ascending=True, inplace=True)
    cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
    result = cal_genweation(cefengta_data, wind_name, key_value, powercure, zhejian)
    result_json = {}
    result_json['Generation_unit'] = 'MWh'
    result_json['Date_Time'] = result['time'].tolist()
    result_json['Generation'] = result['gen'].tolist()
    json_str = json.dumps(result_json, indent=4)
    with open(savename, 'w') as f:
        f.write(json_str)


if __name__ == '__main__':
    import warnings

    warnings.filterwarnings("ignore")
    # cefengta_id = 'M003470'
    # can_name = '30m风速'
    # start_time = '2022-05-16'
    # end_time = '2022-09-12'
    # powercure_type = '6.25_193'
    # zhejian = 1.0
    # key_value = '月'
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/generation.json'
    # generation_analysis
    # 塔名，通道名称，时间起点，时间终点，功率曲线，折减系数，日/月/年
    # python3 generation_analysis.py M003470 30m风速 2022-05-16 2022-09-12 6.25_193 1.0 日 /home/xiaowu/share/202311/测风塔系统/接口/generation.json
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        can_name = sys.argv[2]
        start_time = sys.argv[3]
        end_time = sys.argv[4]
        powercure_type = sys.argv[5]
        zhejian = float(sys.argv[6])
        key_value = sys.argv[7]
        savename = sys.argv[8]

        # 读取功率曲线
        wind_name = can_name.split('m')[0] + '_WS_AVG'
        powercure = read_powercure_from_sql(powercure_type)
        powercure = powercure.replace('None', np.nan).astype('float')
        powercure.sort_values(by='wind', ascending=True, inplace=True)
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
        result = cal_genweation(cefengta_data, wind_name, key_value, powercure, zhejian)
        result_json = {}
        result_json['Generation_unit'] = 'MWh'
        result_json['Date_Time'] = result['time'].tolist()
        result_json['Generation'] = result['gen'].tolist()
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)



