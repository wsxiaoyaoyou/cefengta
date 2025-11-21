import numpy as np
import pandas as pd
import pymysql
import scipy.stats as s
import json
import subprocess
import sys


def weibull(u, shape, scale):
    return (shape / scale) * (u / scale) ** (shape - 1) * np.exp(-(u / scale) ** shape)


def cal_fenbu(data,wind_name,freq_bin):
    wind = []
    result = []
    for i in np.linspace(0, 25, int(25/freq_bin + 1)):
        try:
            if i == 25:
                wind.append(i+freq_bin/2)
                result.append(np.around(len(data[(data[wind_name] >= i)]) / len(data) * 100, 3))
            else:
                wind.append(i+freq_bin/2)
                result.append(np.around(len(data[(data[wind_name] >= i) & (data[wind_name] < i + freq_bin)]) / len(data)*100, 3))
        except:
            wind.append(i+freq_bin/2)
            result.append(np.nan)
    return wind, result

def hist_weibull(data, wind_name, freq_bin):
    '''
    计算威布尔函数
    :param data: pd.Dataframe
    :param wind_name: 风速列名
    :return: shape, scale, Data['wind'].values, Data['weibull'].values
    形状参数和尺度参数，用于画图的风速和威布尔概率密度值
    '''
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    data = data[data[wind_name] > 0]
    wind, result = cal_fenbu(data,wind_name, freq_bin)
    params = s.exponweib.fit(data[wind_name], floc=0, f0=1)
    shape = params[1]
    scale = params[3]
    Data = pd.DataFrame()
    Data['wind'] = [i + freq_bin/2 for i in np.linspace(0, 25, int(25/freq_bin + 1))]
    # Data['weibull'] = Data['wind'].apply(lambda x: weibull(x, shape, scale))
    Data['weibull'] = Data['wind'].apply(lambda x: np.around(s.exponweib.pdf(x, *params) *100, 3))
    Result = {}
    Result['k'] = np.around(shape, 3)
    Result['c'] = np.around(scale, 3)
    result_weibull = {}
    result_weibull['wind'] = wind
    result_weibull['bin'] = result
    Result['weibull_bin'] = result_weibull
    result_cure = {}
    result_cure['wind'] = Data['wind'].to_list()
    result_cure['curve'] = Data['weibull'].to_list()
    Result['pdf_curve'] = result_cure
    # from matplotlib import pyplot as plt
    #
    # plt.bar(wind, result, width=freq_bin)
    # plt.plot(Data['wind'], Data['weibull'])
    # plt.show()
    return Result


def read_data_from_sql(cefengta_id, start_time, end_time, wind_name):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
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

def weibull_analysis(cefengta_id,can_name,start_time,end_time,freq_bin,savename):
    freq_bin = float(freq_bin)
    wind_name = can_name.split('m')[0] + '_WS_AVG'
    cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
    result_json = hist_weibull(cefengta_data, wind_name, freq_bin)
    json_str = json.dumps(result_json, indent=4)
    with open(savename, 'w') as f:
        f.write(json_str)



if __name__ == '__main__':
    import warnings

    warnings.filterwarnings("ignore")
    # cefengta_id = 'M003470'
    # start_time = '2022-05'
    # end_time = '2022-09'
    # can_name = '30m风速'
    # freq_bin = 1
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/weibull.json'
    # 塔名，通道名称，时间起点，时间终点，间隔参数
    # python3 weibull_analysis.py M003470 30m风速 2022-05 2022-09 1 /home/xiaowu/share/202311/测风塔系统/接口/weibull.json
    #
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):

        cefengta_id = sys.argv[1]
        can_name = sys.argv[2]
        start_time = sys.argv[3]
        end_time = sys.argv[4]
        freq_bin = float(sys.argv[5])
        savename = sys.argv[6]
        wind_name = can_name.split('m')[0] + '_WS_AVG'
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
        result_json = hist_weibull(cefengta_data, wind_name, freq_bin)
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)
