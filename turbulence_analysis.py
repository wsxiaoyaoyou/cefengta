import numpy as np
import pandas as pd
import pymysql
import json
import subprocess
import sys
import simplejson

def cal_category_IEC(data_value):
    if data_value > 0.16:
        return 'S'
    elif (data_value > 0.14) & (data_value <= 0.16):
        return 'A'
    elif (data_value > 0.12) & (data_value <= 0.14):
        return 'B'
    elif (data_value >= 0) & (data_value <= 0.12):
        return 'C'
    else:
        return np.nan


def cal_turbulence_height(data, wind_name, wind_std_name):
    '''
    计算单层高度的湍流
    :param data: pd.Dataframe
    :param wind_name: 风速列名——均值
    :param wind_std_name: 风速列名——标准差
    :return:
    '''
    result = {}
    result['Data_Points'] = len(data[wind_name])
    result['Mean_TI'] = np.around(np.nanmean(data[wind_std_name] / data[wind_name]), 2)
    result['Standard_Deviation_of_TI'] = np.around(np.nanstd(data[wind_std_name] / data[wind_name]), 2)
    result['Representative_TI'] = np.around(result['Mean_TI'] + 1.28 * result['Standard_Deviation_of_TI'], 2)
    result['Peak_TI'] = np.around(np.nanmax(data[wind_std_name] / data[wind_name]), 2)
    data_15 = data[(data[wind_name] >= 14.5) & (data[wind_name] < 15.5)]
    result['Data_Points_15m/s'] = len(data_15[wind_name])
    result['Mean_TI_15m/s'] = np.around(np.nanmean(data_15[wind_std_name] / data_15[wind_name]), 2)
    result['Standard_Deviation_of_TI_15m/s'] = np.around(np.nanstd(data_15[wind_std_name] / data_15[wind_name]), 2)
    result['Representative_TI_15m/s'] = np.around(result['Mean_TI_15m/s'] + 1.28 * result['Standard_Deviation_of_TI_15m/s'], 2)
    result['IEC_3ed_Turbulence_Category_15m/s'] = cal_category_IEC(result['Mean_TI_15m/s'])
    return result

def cal_turbulence_height_can(data, wind_name, wind_std_name):
    wind_name_list = []
    for wind_name_i in wind_name.split(','):
        if wind_name_i != ' ':
            wind_name_i = wind_name_i.replace(' ', '')
            data = data[data[wind_name_i] != ' ']
            data[wind_name_i] = data[wind_name_i].replace('None', np.nan).astype('float')
            wind_name_list.append(wind_name_i)
    wind_std_name_list = []
    for wind_std_name_i in wind_std_name.split(','):
        if wind_std_name_i != ' ':
            wind_std_name_i = wind_std_name_i.replace(' ', '')
            data = data[data[wind_std_name_i] != ' ']
            data[wind_std_name_i] = data[wind_std_name_i].replace('None', np.nan).astype('float')
            wind_std_name_list.append(wind_std_name_i)
    # 开始计算表格
    result_json_height = {}
    height_line = []
    height_line_mean_TI = []
    for height_i in range(0, len(wind_name_list)):
        result_json_height[wind_name_list[height_i].split('_')[0] + 'm'] = cal_turbulence_height(data, wind_name_list[
            height_i], wind_std_name_list[height_i])
        height_line.append(wind_name_list[height_i].split('_')[0] + 'm')
        height_line_mean_TI.append(result_json_height[wind_name_list[height_i].split('_')[0] + 'm']['Mean_TI'])
    result_line = {}
    result_line['Height_Above_Ground'] = height_line
    result_line['Mean_Turbulence_Intensity'] = height_line_mean_TI
    result_json_height['line'] = result_line
    return result_json_height


def cal_turbulence_wind(data, wind_name, wind_std_name, result, data_len):
    '''
    计算单层高度的湍流
    :param data: pd.Dataframe
    :param wind_name: 风速列名——均值
    :param wind_std_name: 风速列名——标准差
    :return:
    '''
    if len(data) > 0:
        result['Data_Points'] = len(data[wind_name])
        result['Bin_Frequency'] = np.around(result['Data_Points'] / data_len * 100, 2)
        result['Mean_TI'] = np.around(np.nanmean(data[wind_std_name] / data[wind_name]), 2)
        result['Standard_Deviation_of_TI'] = np.around(np.nanstd(data[wind_std_name] / data[wind_name]), 2)
        result['Representative_TI'] = np.around(result['Mean_TI'] + 1.28 * result['Standard_Deviation_of_TI'], 2)
        result['Peak_TI'] = np.around(np.nanmax(data[wind_std_name] / data[wind_name]), 2)
    else:
        result['Data_Points'] = np.nan
        result['Bin_Frequency'] = np.nan
        result['Mean_TI'] = np.nan
        result['Standard_Deviation_of_TI'] = np.nan
        result['Representative_TI'] = np.nan
        result['Peak_TI'] = np.nan
    return result

def cal_turbulence_wind_can(data, wind_name, wind_std_name):
    result_json_wind = {}
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    data = data[data[wind_std_name] != ' ']
    data[wind_std_name] = data[wind_std_name].replace('None', np.nan).astype('float')
    data_len = len(data)
    wind_line = []
    wind_line_mean_ti = []
    wind_line_representtative_ti = []
    data_bin = data[(data[wind_name] >= 0) & (data[wind_name] < 0.5)]
    result = {}
    result['Bin_Midpoint'] = 0.3
    result['Bin_Endpoint_Lower'] = 0.0
    result['Bin_Endpoint_Upper'] = 0.5
    result = cal_turbulence_wind(data_bin, wind_name, wind_std_name, result, data_len)
    result_json_wind[str(1)] = result
    wind_line.append(result['Bin_Midpoint'])
    wind_line_mean_ti.append(result['Mean_TI'])
    wind_line_representtative_ti.append(result['Representative_TI'])
    # print(result_json_wind)
    for bin_i in range(1, 26):
        result = {}
        result['Bin_Midpoint'] = float(bin_i)
        result['Bin_Endpoint_Lower'] = bin_i - 0.5
        result['Bin_Endpoint_Upper'] = bin_i + 0.5
        data_bin = data[(data[wind_name] >= bin_i - 0.5) & (data[wind_name] < bin_i + 0.5)]
        # print(data_bin)
        result = cal_turbulence_wind(data_bin, wind_name, wind_std_name, result, data_len)
        result_json_wind[str(bin_i + 1)] = result
        wind_line.append(result['Bin_Midpoint'])
        wind_line_mean_ti.append(result['Mean_TI'])
        wind_line_representtative_ti.append(result['Representative_TI'])
    result_line = {}
    result_line['Wind_Speed'] = wind_line
    result_line['Repressentative_TI'] = wind_line_representtative_ti
    result_line['Mean_TI'] = wind_line_mean_ti
    result_json_wind['line'] = result_line
    return result_json_wind


def cal_turbulence_month_day(data, wind_name, wind_std_name):
    '''
    计算单层高度的湍流
    :param data: pd.Dataframe
    :param wind_name: 风速列名——均值
    :param wind_std_name: 风速列名——标准差
    :return:
    '''
    result = {}
    if len(data) > 0:
        result['Data_Points'] = len(data[wind_name])
        result['Mean_TI'] = np.around(np.nanmean(data[wind_std_name] / data[wind_name]), 2)
        result['Standard_Deviation_of_TI'] = np.around(np.nanstd(data[wind_std_name] / data[wind_name]), 2)
        result['Representative_TI'] = np.around(result['Mean_TI'] + 1.28 * result['Standard_Deviation_of_TI'], 2)
        result['Peak_TI'] = np.around(np.nanmax(data[wind_std_name] / data[wind_name]), 2)
    else:
        result['Data_Points'] = np.nan
        result['Mean_TI'] = np.nan
        result['Standard_Deviation_of_TI'] = np.nan
        result['Representative_TI'] = np.nan
        result['Peak_TI'] = np.nan
    return result

def cal_turbulence_month_can(data, wind_name, wind_std_name, time_name):
    data['time'] = data[time_name].apply(lambda x: x[5:7])
    # print(data['time'])
    result_json_month = {}
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    data = data[data[wind_std_name] != ' ']
    data[wind_std_name] = data[wind_std_name].replace('None', np.nan).astype('float')
    for month_i in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
        data_month = data[data['time'] == month_i]
        result = cal_turbulence_month_day(data_month, wind_name, wind_std_name)
        result_json_month[month_i] = result
    return result_json_month



def cal_turbulence_day_can(data, wind_name, wind_std_name, time_name):
    data['time'] = data[time_name].apply(lambda x: x[11:13])
    result_json_day = {}
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    data = data[data[wind_std_name] != ' ']
    data[wind_std_name] = data[wind_std_name].replace('None', np.nan).astype('float')
    for day_i in ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']:
        data_month = data[data['time'] == day_i]
        result = cal_turbulence_month_day(data_month, wind_name, wind_std_name)
        result_json_day[day_i + ':00-' + '%02d' % (int(day_i) + 1) + ':00'] = result
    return result_json_day

def cal_turbulence_dir(data, wind_name, wind_std_name, result):
    '''
    计算单层高度的湍流
    :param data: pd.Dataframe
    :param wind_name: 风速列名——均值
    :param wind_std_name: 风速列名——标准差
    :return:
    '''
    if len(data) > 0:
        result['Data_Points'] = len(data[wind_name])
        result['Mean_TI'] = np.around(np.nanmean(data[wind_std_name] / data[wind_name]), 2)
        result['Standard_Deviation_of_TI'] = np.around(np.nanstd(data[wind_std_name] / data[wind_name]), 2)
        result['Representative_TI'] = np.around(result['Mean_TI'] + 1.28 * result['Standard_Deviation_of_TI'], 2)
        result['Peak_TI'] = np.around(np.nanmax(data[wind_std_name] / data[wind_name]), 2)
    else:
        result['Data_Points'] = np.nan
        result['Mean_TI'] = np.nan
        result['Standard_Deviation_of_TI'] = np.nan
        result['Representative_TI'] = np.nan
        result['Peak_TI'] = np.nan
    return result


def cal_turbulence_dir_can(data, wind_name, wind_std_name, dir_name, value_int):
    result_json_dir = {}
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    data = data[data[wind_std_name] != ' ']
    data[wind_std_name] = data[wind_std_name].replace('None', np.nan).astype('float')
    data = data[data[dir_name] != ' ']
    data[dir_name] = data[dir_name].replace('None', np.nan).astype('float')
    c = 1
    for i in np.linspace(360 / value_int, 360, value_int):
        result = {}
        result['Sector_Midpoint'] = i - 360 / value_int
        if i == 360/value_int:
            data_dir = data[(data[dir_name] <= i-360/value_int/2) | (data[dir_name] > 360 - 360 / value_int / 2)]
            result['Sector_Range'] = [360 - 360 / value_int / 2, i-360/value_int/2]
        else:
            data_dir = data[(data[dir_name] > i-360/value_int/2*3) & (data[dir_name] <= i-360/value_int/2)]
            result['Sector_Range'] = [i-360/value_int/2*3, i-360/value_int/2]
        result = cal_turbulence_dir(data_dir, wind_name, wind_std_name, result)
        result_json_dir[str(c)] = result
        c = c + 1
    return result_json_dir


def cal_turbulence_can(data, wind_name, wind_std_name, key_value, value_int=None):
    time_name = 'Date_Time'
    if key_value == '高度':
        result_json_height = cal_turbulence_height_can(data, wind_name, wind_std_name)
        return result_json_height
    elif key_value == '风速':
        result_json_wind = cal_turbulence_wind_can(data, wind_name, wind_std_name)
        return result_json_wind
    elif key_value == '月':
        result_json_month = cal_turbulence_month_can(data, wind_name, wind_std_name, time_name)
        return result_json_month
    elif key_value == '日':
        result_json_day = cal_turbulence_day_can(data, wind_name, wind_std_name, time_name)
        return result_json_day
    else:
        result_json_dir = cal_turbulence_dir_can(data, wind_name, wind_std_name, key_value, value_int)
        return result_json_dir





def read_data_from_sql(cefengta_id, start_time, end_time, wind_name, wind_std_name, dir_name=None):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if dir_name == None:
        try:
            cursor.execute(
                "SELECT Date_Time, %s, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (wind_name, wind_std_name, cefengta_id, start_time, end_time))
        except:
            cursor.execute(
                "SELECT Date_Time %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (
                wind_name+wind_std_name, cefengta_id, start_time, end_time))
    else:
        cursor.execute("SELECT Date_Time, %s, %s, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (wind_name, wind_std_name, dir_name, cefengta_id, start_time, end_time))
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

def turbulence_analysis(cefengta_id,can_name,start_time,end_time,key_value,savename,value_int = 16):
    wind_name = can_name.split('m')[0] + '_WS_AVG'
    wind_std_name = can_name.split('m')[0] + '_WS_SD'
    if key_value in ['风速', '月', '日']:
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name, wind_std_name)
    elif key_value == '高度':
        height = read_height_wind(cefengta_id)
        wind_name = ''
        wind_std_name = ''
        ceng_list = []
        for ceng_id in height.split(','):
            wind_name = wind_name + ' ,' + ceng_id + '_WS_AVG'
            wind_std_name = wind_std_name + ' ,' + ceng_id + '_WS_SD'
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name, wind_std_name)
    else:
        dir_name = key_value.split('m')[0] + '_WD_AVG'
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name, wind_std_name, dir_name)
    if '风向' in key_value:
        dir_name = key_value.split('m')[0] + '_WD_AVG'
        result_json = cal_turbulence_can(cefengta_data, wind_name, wind_std_name, dir_name, value_int)
    else:
        result_json = cal_turbulence_can(cefengta_data, wind_name, wind_std_name, key_value)

    json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
    with open(savename, 'w') as f:
        f.write(json_str)


if __name__ == '__main__':
    import warnings

    warnings.filterwarnings("ignore")
    # cefengta_id = 'M003470'
    # can_name = '30m风速'
    # start_time = '2022-05'
    # end_time = '2022-09'
    # # key_value = '高度'
    # # savename = '/home/xiaowu/share/202311/测风塔系统/接口/turbulence_高度.json'
    # # key_value = '风速'
    # # savename = '/home/xiaowu/share/202311/测风塔系统/接口/turbulence_风速.json'
    # # key_value = '月'
    # # savename = '/home/xiaowu/share/202311/测风塔系统/接口/turbulence_月.json'
    # # key_value = '日'
    # # savename = '/home/xiaowu/share/202311/测风塔系统/接口/turbulence_日.json'
    # key_value = '120m风向'
    # value_int = 16
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/turbulence_风向.json'

    # 塔名，通道名称，时间起点，时间终点，高度/风速/风向/月/日，结果存储名称路径，缺省参数
    # python3 turbulence_analysis.py M003470 30m风速 2022-05-16 2022-09-16 高度 /home/xiaowu/share/202311/测风塔系统/接口/turbulence.json
    # python3 turbulence_analysis.py M003470 30m风速 2022-05-16 2022-09-16 120m风向 /home/xiaowu/share/202311/测风塔系统/接口/turbulence.json 16
    #
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):

        cefengta_id = sys.argv[1]
        can_name = sys.argv[2]
        start_time = sys.argv[3]
        end_time = sys.argv[4]
        key_value = sys.argv[5]
        savename = sys.argv[6]
        if len(sys.argv) > 7:
            value_int = int(sys.argv[7])
        else:
            value_int = 16
        # 开始计算
        wind_name = can_name.split('m')[0] + '_WS_AVG'
        wind_std_name = can_name.split('m')[0] + '_WS_SD'
        if key_value in ['风速', '月', '日']:
            cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name, wind_std_name)
        elif key_value == '高度':
            height = read_height_wind(cefengta_id)
            wind_name = ''
            wind_std_name = ''
            ceng_list = []
            for ceng_id in height.split(','):
                wind_name = wind_name + ' ,' + ceng_id + '_WS_AVG'
                wind_std_name = wind_std_name + ' ,' + ceng_id + '_WS_SD'
            cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name, wind_std_name)
        else:
            dir_name = key_value.split('m')[0] + '_WD_AVG'
            cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name, wind_std_name, dir_name)
        if '风向' in key_value:
            dir_name = key_value.split('m')[0] + '_WD_AVG'
            result_json = cal_turbulence_can(cefengta_data, wind_name, wind_std_name, dir_name, value_int)
        else:
            result_json = cal_turbulence_can(cefengta_data, wind_name, wind_std_name, key_value)


        json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
        with open(savename, 'w') as f:
            f.write(json_str)


