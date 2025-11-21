import numpy as np
import pandas as pd
import pymysql
import json
import subprocess
import sys


def cal_dir_WPD_bin(data, wind_name, dir_name, bin_key):
    '''
    计算风能频率玫瑰图
    :param data:pd.Dataframe
    :param wind_name:风速列名
    :param dir_name:风向列名
    :param bin_key:区间个数
    :return:{年：【测量高度，各个风向区间风能频率】}{1月：【测量高度，各个风向区间风能频率】}
    [30.0, 0.547, 5.649, 16.107, 20.576, 9.311, 3.291, 0.779, 0.638, 2.55, 11.451, 16.827, 7.499, 3.585, 0.392, 0.276, 0.521]
    '''
    result = []
    data = data[data[dir_name] != ' ']
    data = data[data[wind_name] != ' ']
    data[dir_name] = data[dir_name].replace('None', np.nan).astype('float')
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    WPD_zong = np.nansum(data[wind_name] ** 3)
    for i in np.linspace(360 / bin_key, 360, bin_key):
        try:
            if i == 360/bin_key:
                result.append(np.around(np.nansum(data[(data[dir_name] <= i-360/bin_key/2) | (data[dir_name] > 360 - 360 / bin_key / 2)][wind_name] ** 3) / WPD_zong * 100,3))
            else:
                result.append(np.around(np.nansum(data[(data[dir_name] > i-360/bin_key/2*3) & (data[dir_name] <= i-360/bin_key/2)][wind_name] ** 3) / WPD_zong * 100, 3))
        except:
            result.append(np.nan)
    return result


def cal_dir_WPD_bin_year_yue(data, wind_name, dir_name, bin_key, year_yue):
    result_json = {}
    if year_yue == '年况':
        result_dir = cal_dir_WPD_bin(data, wind_name, dir_name, bin_key)
        result_json['year'] = result_dir
    elif year_yue == '月况':
        data['month'] = data['Date_Time'].apply(lambda x:x[5:7])
        for i in range(1, 13):
            data_month = data[data['month'] == '%02d' % i]
            if len(data_month) > 0:
                result_dir = cal_dir_WPD_bin(data, wind_name, dir_name, bin_key)
                result_json['%02d' % i] = result_dir
            else:
                result_json['%02d' % i] = []
    return result_json


def cal_dir_bin(data, dir_name, bin_key):
    '''
    计算风向频率玫瑰图
    :param data: pd.Dataframe
    :param dir_name: 风向列名 0-360°
    :param bin_key:区间个数
    :return:【测量高度，各个风向区间频率】
    [30.0, 1.574, 5.796, 14.679, 14.166, 6.616, 2.805, 2.168, 2.197, 3.739, 10.53, 14.14, 9.388, 8.504, 1.917, 0.833, 0.949]
    '''
    result = []
    data = data[data[dir_name] != ' ']
    data[dir_name] = data[dir_name].replace('None', np.nan).astype('float')
    for i in np.linspace(360 / bin_key, 360, bin_key):
        try:
            if i == 360/bin_key:
                result.append(np.around(len(data[(data[dir_name] <= i-360/bin_key/2) | (data[dir_name] > 360 - 360 / bin_key / 2)]) / len(data) * 100, 3))
            else:
                result.append(np.around(len(data[(data[dir_name] > i-360/bin_key/2*3) & (data[dir_name] <= i-360/bin_key/2)]) / len(data)*100, 3))
        except:
            result.append(np.nan)
    return result


def cal_dir_bin_year_yue(data, dir_name, bin_key, year_yue):
    result_json = {}
    if year_yue == '年况':
        result_dir = cal_dir_bin(data, dir_name, bin_key)
        result_json['year'] = result_dir
    elif year_yue == '月况':
        data['month'] = data['Date_Time'].apply(lambda x:x[5:7])
        for i in range(1, 13):
            data_month = data[data['month'] == '%02d' % i]
            if len(data_month) > 0:
                result_dir = cal_dir_bin(data_month, dir_name, bin_key)
                result_json['%02d' % i] = result_dir
            else:
                result_json['%02d' % i] = []
    return result_json



def read_data_from_sql(cefengta_id, start_time, end_time, wind_name, dir_name):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'

    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT Date_Time, %s, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (dir_name, wind_name, cefengta_id, start_time, end_time))
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list
    cursor.close()
    conn.close()
    return data

def rose_analysis(cefengta_id,can_name_1,can_name_2,start_time,end_time,year_yue,key_value,bin_key,savename):
    bin_key = int(bin_key)
    wind_name = can_name_1.split('m')[0] + '_WS_AVG'
    dir_name = can_name_2.split('m')[0] + '_WD_AVG'
    cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name, dir_name)
    # 风能
    if key_value == '风能':
        result_json = cal_dir_WPD_bin_year_yue(cefengta_data, wind_name, dir_name, bin_key, year_yue)
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)

    # 风向
    elif key_value == '风向':
        result_json = cal_dir_bin_year_yue(cefengta_data, dir_name, bin_key, year_yue)
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)


if __name__ == '__main__':
    import warnings

    warnings.filterwarnings("ignore")

    # 塔名，通道名称1，通道名称2，时间起点，时间终点，月况 / 年况，风向 / 风能，扇区数
    # python3 rose_analysis.py M003470 30m风速 30m风向 2022-05 2022-09 月况 风向 16 /home/xiaowu/share/202311/测风塔系统/接口/rose_month.json
    #
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        can_name_1 = sys.argv[2]
        can_name_2 = sys.argv[3]
        start_time = sys.argv[4]
        end_time = sys.argv[5]
        year_yue = sys.argv[6]
        key_value = sys.argv[7]
        bin_key = int(sys.argv[8])
        savename = sys.argv[9]

        wind_name = can_name_1.split('m')[0] + '_WS_AVG'
        dir_name = can_name_2.split('m')[0] + '_WD_AVG'
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name, dir_name)
        # 风能
        if key_value == '风能':
            result_json = cal_dir_WPD_bin_year_yue(cefengta_data, wind_name, dir_name, bin_key, year_yue)
            json_str = json.dumps(result_json, indent=4)
            with open(savename, 'w') as f:
                f.write(json_str)

        # 风向
        elif key_value == '风向':
            result_json = cal_dir_bin_year_yue(cefengta_data, dir_name, bin_key, year_yue)
            json_str = json.dumps(result_json, indent=4)
            with open(savename, 'w') as f:
                f.write(json_str)
