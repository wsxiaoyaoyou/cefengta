import numpy as np
import pandas as pd
import pymysql
import json
import subprocess
import sys

def cal_wind_bin(data, wind_name):
    '''
    计算风速频率分布
    :param data: pd.Dataframe
    :param wind_name: 风速列名
    :return: 【测量高度，风速频率】
    [30.0, 3.514, 4.619, 6.007, 7.991, 9.857, 10.967, 11.585, 11.084, 10.01, 7.831, 5.927, 3.852, 2.588, 1.547, 0.981, 0.627, 0.424, 0.242, 0.133, 0.122, 0.053, 0.025, 0.009, 0.003, 0.003, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    '''
    result = {}
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    for i in np.linspace(0.5, 25, 50):
        try:
            if i == 25:
                result[str(i)] = np.around(len(data[(data[wind_name] >= i - 0.5)]) / len(data) * 100, 3)
            else:
                result[str(i)] = np.around(len(data[(data[wind_name] >= i - 0.5) & (data[wind_name] < i)]) / len(data)*100, 3)
        except:
            result[str(i)] = np.nan
    return result


def cal_WPD_bin(data, wind_name):
    '''
    计算风能频率分布
    :param data:pd.Dataframe
    :param wind_name:风速列名
    :return:【测量高度，各个风速区间风能频率】
    [30, 0.003, 0.032, 0.178, 0.626, 1.625, 3.273, 5.664, 8.291, 10.849, 11.862, 12.087, 10.303, 8.892, 6.713, 5.244, 4.099, 3.331, 2.29, 1.492, 1.579, 0.809, 0.437, 0.174, 0.069, 0.079, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    '''
    result = {}
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    WPD_zong = np.nansum(data[wind_name].astype('float') ** 3)
    for i in np.linspace(0.5, 25, 50):
        try:
            if i == 25:
                # print(i, len(data[(data[str(cengid) + 'WSAVG'] >= i-0.5) & (data[str(cengid) + 'WSAVG'] <= i)]))
                # result.append(np.around(len(data[(data[str(cengid) + 'WSAVG'] >= i-0.5) & (data[str(cengid) + 'WSAVG'] <= i)]) / len(data)*100, 3))
                result[str(i)] = np.around(np.nansum(data[(data[wind_name] >= i - 0.5)][wind_name] ** 3) / WPD_zong *100, 3)
            else:
                result[str(i)] = np.around(np.nansum(data[(data[wind_name] >= i - 0.5) & (data[wind_name] < i)][wind_name] ** 3) / WPD_zong *100, 3)
        except:
            result[str(i)] = np.nan
    return result

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

def frequency_analysis(cefengta_id,can_name,start_time,end_time,savename):
    wind_name = can_name.split('m')[0] + '_WS_AVG'
    result_json = {}
    cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
    result_wind = cal_wind_bin(cefengta_data, wind_name)
    result_json['wind'] = result_wind
    result_WPD = cal_WPD_bin(cefengta_data, wind_name)
    result_json['WPD'] = result_WPD
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
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/frequency.json'

    # 塔名，通道名称，时间起点，时间终点
    # python3 frequency_analysis.py M003470 30m风速 2022-05 2022-09 /home/xiaowu/share/202311/测风塔系统/接口/frequency.json
    #
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        can_name = sys.argv[2]
        start_time = sys.argv[3]
        end_time = sys.argv[4]
        savename = sys.argv[5]
        wind_name = can_name.split('m')[0] + '_WS_AVG'
        result_json = {}
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
        result_wind = cal_wind_bin(cefengta_data, wind_name)
        result_json['wind'] = result_wind
        result_WPD = cal_WPD_bin(cefengta_data, wind_name)
        result_json['WPD'] = result_WPD
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)