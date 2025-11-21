import pandas as pd
import pymysql
import numpy as np
import sys
from scipy.optimize import curve_fit
import subprocess


def linear_func(x, a, b):
    return a*x+b


def cal_scatter(data, can1, can2):
    # time_name = 'Date_Time'
    data = data[data[can1] != ' ']
    data[can1] = data[can1].replace('None', np.nan).astype('float')
    data = data[data[can2] != ' ']
    data[can2] = data[can2].replace('None', np.nan).astype('float')
    result_json = {}
    result_json['type1'] = data[can1].values.tolist()
    result_json['type2'] = data[can2].values.tolist()
    data = data.dropna(subset=[can1])
    data = data.dropna(subset=[can2])
    popt, pcov = curve_fit(linear_func, data[can1], data[can2])
    a = popt[0]
    b = popt[1]
    x_line = np.array([x for x in np.arange(0, int(np.nanmax(data[can1])+1))])
    y_line = [np.around(x, 2) for x in linear_func(x_line, a, b)]
    calc_ydata = [linear_func(i, a, b) for i in data[can1]]
    res_ydata = np.array(data[can2]) - np.array(calc_ydata)
    ss_res = np.sum(res_ydata ** 2)
    ss_tot = np.sum((data[can2] - np.mean(data[can2])) ** 2)
    r_squared = 1 - (ss_res / ss_tot)
    result_json['x_line'] = x_line.tolist()
    result_json['y_line'] = y_line
    result_json['r_squared'] = np.around(r_squared, 2)
    return result_json


def read_data_from_sql(cefengta_id, start_time, end_time, can1, can2):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if can1 == can2:
        cursor.execute(
            "SELECT Date_Time, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (
            can1, cefengta_id, start_time, end_time))
    else:
        cursor.execute(
            "SELECT Date_Time, %s, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (
            can1, can2, cefengta_id, start_time, end_time))
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list
    cursor.close()
    conn.close()
    return data

def scatter_analysis(cefengta_id,start_time,end_time,can_name1,can_name2,savename):
    # 读取功率曲线
    if '风速' in can_name1:
        can1 = can_name1.split('m')[0] + '_WS_AVG'
        can2 = can_name2.split('m')[0] + '_WS_AVG'
    else:
        can1 = can_name1.split('m')[0] + '_WD_AVG'
        can2 = can_name2.split('m')[0] + '_WD_AVG'
    cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, can1, can2)
    result_json = cal_scatter(cefengta_data, can1, can2)
    import simplejson
    json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
    with open(savename, 'w') as f:
        f.write(json_str)

if __name__ == '__main__':
    import warnings
    warnings.filterwarnings("ignore")
    # cefengta_id = 'M003470'
    # start_time = '2022-05-16'
    # end_time = '2022-09-16'
    # can_name = '30m风速'
    # can_name = '50m风速'
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/50yearmaxwind.json'
    # scatter_analysis
    # 塔名，时间起点，时间终点, 通道名称1，通道名称2，结果存储名称路径
    # python3 scatter_analysis.py M003470  2022-05-16 2022-09-16 30m风速 50m风速 /home/xiaowu/share/202311/测风塔系统/接口/scatter.json
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        start_time = sys.argv[2]
        end_time = sys.argv[3]
        can_name1 = sys.argv[4]
        can_name2 = sys.argv[5]
        savename = sys.argv[6]

        # 读取功率曲线
        if '风速' in can_name1:
            can1 = can_name1.split('m')[0] + '_WS_AVG'
            can2 = can_name2.split('m')[0] + '_WS_AVG'
        else:
            can1 = can_name1.split('m')[0] + '_WD_AVG'
            can2 = can_name2.split('m')[0] + '_WD_AVG'
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, can1, can2)
        result_json = cal_scatter(cefengta_data, can1, can2)
        import simplejson
        json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
        with open(savename, 'w') as f:
            f.write(json_str)



