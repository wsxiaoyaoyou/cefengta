import numpy as np
import pandas as pd
import pymysql
from scipy.optimize import curve_fit
import json
import subprocess
import sys
import math


def cal_sh_ceng(v1, v2, z1, z2):
    try:
        s = math.log(float(v1) / float(v2)) / math.log(z1 / z2)
    except:
        s = np.nan
    return s

def cal_shear(data, ceng_list):
    ceng_list.sort(reverse=False)
    ceng = np.array(ceng_list)
    wind_mean = []
    for id in ceng_list:
        wind_mean.append(np.nanmean(data[data[str(id) + '_WS_AVG'] != ' '][str(id) + '_WS_AVG'].replace('None', np.nan).astype('float')))
    wind = np.array(wind_mean)
    power_func = lambda x, c, a: c * np.power(x, a)
    params, cov = curve_fit(power_func, wind, ceng, maxfev=1000)
    # wind = params[0] * np.power(ceng_list, params[1])
    wind_list = []
    for i in wind:
        wind_list.append(np.around(i, 3))
    result = {}
    # height wind从小到大
    result['height'] = ceng_list
    result['wind'] = wind_list
    result['%.3f' % params[0] + '*x^' '%.3f' % params[1]] = {'c': [np.around(params[0], 3)],
                                                             'a': [np.around(params[1], 3)]}
    result['c'] = np.around(params[0], 3)
    result['a'] = np.around(params[1], 3)
    ceng_list1 = ceng_list.copy()
    ceng_list1.sort(reverse=True)
    # 加一列height,
    data_shear = pd.DataFrame(index=[str(x) for x in ceng_list1[1:]], columns=[str(x) for x in ceng_list1[:-1]])
    for index_i in data_shear.index:
        for col_i in data_shear.columns:
            if int(index_i) < int(col_i):
                data_shear.loc[index_i, col_i] = np.nanmean(data.apply(
                    lambda x: cal_sh_ceng(x[index_i + '_WS_AVG'], x[col_i + '_WS_AVG'], int(index_i), int(col_i)),
                    axis=1))
    data_shear['height'] = data_shear.index.values
    items = data_shear.to_json(orient="records", force_ascii=False)
    items = json.loads(items)
    result['shear_ceng'] = items
    return result


def read_data_from_sql(cefengta_id, start_time, end_time, wind_name):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT Date_Time %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (wind_name, cefengta_id, start_time, end_time))
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list
    cursor.close()
    conn.close()
    return data


def read_height_wind(cefengta_id):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT SHEARY FROM cefengta.dynamic_information where ID='%s';" % cefengta_id)
    # # 获取查询结果
    # 获取表头
    values = cursor.fetchall()[0][0]
    cursor.close()
    conn.close()
    return values

def shear_analysis(cefengta_id,start_time,end_time,savename,height="null"):
    if height!="null":
        height = height.replace('_', ',')
    else:
        height = read_height_wind(cefengta_id)
    wind_name = ''
    ceng_list = []
    for ceng_id in height.split(','):
        wind_name = wind_name + ' ,' + ceng_id + '_WS_AVG'
        ceng_list.append(int(ceng_id))
    cefengta = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
    result_json = cal_shear(cefengta, ceng_list)
    json_str = json.dumps(result_json, indent=4)
    with open(savename, 'w') as f:
        f.write(json_str)


if __name__ == '__main__':
    # cefengta_id = 'M003470'
    # start_time = '2022-05'
    # end_time = '2022-09'
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/shear.json'
    # 塔名，时间起点，时间终点
    # python3 shear_analysis.py M003470 2022-05 2022-09 /home/xiaowu/share/202311/测风塔系统/接口/shear.json
    #
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        start_time = sys.argv[2]
        end_time = sys.argv[3]
        savename = sys.argv[4]
        if len(sys.argv) > 5:
            height = sys.argv[5]
            height = height.replace('_', ',')
        else:
            height = read_height_wind(cefengta_id)
        wind_name = ''
        ceng_list = []
        for ceng_id in height.split(','):
            wind_name = wind_name + ' ,' + ceng_id + '_WS_AVG'
            ceng_list.append(int(ceng_id))
        cefengta = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
        result_json = cal_shear(cefengta, ceng_list)
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)

