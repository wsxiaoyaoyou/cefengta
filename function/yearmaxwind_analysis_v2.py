import math
import pandas as pd
import pymysql
import numpy as np
import json
import subprocess
import sys
import scipy.stats as s
from scipy.special import gamma


def hist_weibull(data, wind_name):
    '''
    计算威布尔函数
    :param data: pd.Dataframe
    :param wind_name: 风速列名
    :return: shape, scale, Data['wind'].values, Data['weibull'].values
    形状参数和尺度参数，用于画图的风速和威布尔概率密度值
    '''
    params = s.exponweib.fit(data[wind_name], floc=0, f0=1)
    shape = params[1]
    return shape, np.nanmean(data[wind_name])


def ewts_ii_exact(V_ave, k, Tr=50, n=23037):
    """
    Exact variation of the EWTS II algorithm to estimate the extreme wind speed.

    Parameters:
    V_ave : float
        is the average wind speed
    k : float
        is the Weibull k factor
    Tr : float
        is the return period in years (50 if calculating 50-yr extreme values)
    n : float
        n is the number of independent events per year (23,037 for 10-min time steps and 1-yr extrema)

    Returns:
    V_ref : float
        The estimated extreme wind speed for the given return period
    """
    V_ref = V_ave / gamma(1 + 1 / k) * np.power(-math.log(1-math.exp(math.log(1-1/Tr)/n)), 1/k)
    return V_ref


def cal_genweation(data, wind_name):
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan)
    data[wind_name] = data[wind_name].astype('float')
    data = data.dropna(subset=[wind_name])
    k, V_ave = hist_weibull(data, wind_name)
    vmax = ewts_ii_exact(V_ave, k, Tr=50, n=23037)
    return np.around(vmax, 3)


def read_data_from_sql(cefengta_id, start_time, end_time, wind_name):
    host = 'localhost'
    port = 3306
    database = 'cefengta'
    user = 'root'  # 用户名
    password = '123456'  # 密码
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


def yearmaxwind_analysis_v2(cefengta_id,can_name,start_time,end_time,savename):
    # 读取功率曲线
    wind_name = can_name.split('m')[0] + '_WS_AVG'
    cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)

    result = cal_genweation(cefengta_data, wind_name)
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
    # python3 yearmaxwind_analysis_v2.py M003470 100m风速 2022-05-16 2023-05-16 /home/xiaowu/share/202311/测风塔系统/接口/50yearmaxwind.json
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        can_name = sys.argv[2]
        start_time = sys.argv[3]
        end_time = sys.argv[4]
        savename = sys.argv[5]
        # 读取功率曲线
        wind_name = can_name.split('m')[0] + '_WS_AVG'
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)

        result = cal_genweation(cefengta_data, wind_name)
        result_json = {}
        result_json['50yearmaxwind'] = result
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)



