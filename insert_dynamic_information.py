import pandas as pd
import pymysql
from scipy.optimize import curve_fit
import numpy as np
import sys
import subprocess
host = 'localhost'
port = 3306
user = 'root' #用户名
password = '123456' # 密码
database = 'cefengta'
import warnings

warnings.filterwarnings("ignore")
def cal_dir_bin(data, dir_name):
    '''
    计算风向频率玫瑰图
    :param data: pd.Dataframe
    :param dir_name: 风向列名 0-360°
    :return:【测量高度，各个风向区间频率】
    [30.0, 1.574, 5.796, 14.679, 14.166, 6.616, 2.805, 2.168, 2.197, 3.739, 10.53, 14.14, 9.388, 8.504, 1.917, 0.833, 0.949]
    '''
    result = []
    result.append(int(dir_name.split('_')[0]))
    data = data[data[dir_name] != ' ']
    data[dir_name] = data[dir_name].replace('None', np.nan)
    data[dir_name] = data[dir_name].astype('float')
    # for i in np.linspace(22.5, 360, 16):
    for i in np.linspace(11.25, 348.75, 16):
        try:
            if i == 11.25:
                result.append(np.around(len(data[(data[dir_name] <= i) | (data[dir_name] > 348.75)]) / len(data) * 100, 3))
            else:
                result.append(np.around(len(data[(data[dir_name] > i - 22.5) & (data[dir_name] <= i)]) / len(data)*100, 3))
        except:
            result.append(np.nan)
    return result
def cal_rho(data, tem_name, P_name):

    '''
    R = 287 气体常数(287J/kg·K) 根绝温度和压力计算空气密度
    :param data: pd.Dataframe
    :param tem_name: 温度列名 温度单位 ℃
    :param P_name: 压力列名 压力单位 Pa
    :return: 空气密度
    '''

    R = 287
    data = data[data[tem_name] != ' ']
    data = data[data[P_name] != ' ']
    data[tem_name] = data[tem_name].replace('None', np.nan)
    data[P_name] = data[P_name].replace('None', np.nan)
    T = np.nanmean(data[tem_name].astype('float')) + 273.15
    P = np.nanmean(data[P_name].astype('float'))
    rho = P / R / T
    return rho

def cal_wind_ceng(data, wind_name_list):
    '''
    计算个高度层平均风速
    :param data: pd.Dataframe
    :param wind_name_list: 风速列表，从小到大排列
    :return: 【风速平均值】， 【层高】
    '''
    result = []
    wind_name = []
    colname = []
    for col_i in wind_name_list:
        colname.append(str(col_i) + '_WS_AVG')
    cal_wind = data[colname]
    for wind_ceng in wind_name_list:
        wind_name_ceng = str(wind_ceng) + '_WS_AVG'
        cal_wind = cal_wind[cal_wind[wind_name_ceng] != ' ']
        cal_wind[wind_name_ceng] = cal_wind[wind_name_ceng].replace('None', np.nan)
        cal_wind[wind_name_ceng] = cal_wind[wind_name_ceng].astype('float')
    for wind_ceng in wind_name_list:
        wind_name_ceng = str(wind_ceng) + '_WS_AVG'
        if np.around(np.nanmean(cal_wind[wind_name_ceng]), 2) > 0:
            wind_name.append(wind_ceng)
            result.append(np.around(np.nanmean(cal_wind[wind_name_ceng]), 2))
    return result, wind_name


def cal_WPD_bin(data, wind_name):
    '''
    计算风能频率分布
    :param data:pd.Dataframe
    :param wind_name:风速列名
    :return:【测量高度，各个风速区间风能频率】
    [30, 0.003, 0.032, 0.178, 0.626, 1.625, 3.273, 5.664, 8.291, 10.849, 11.862, 12.087, 10.303, 8.892, 6.713, 5.244, 4.099, 3.331, 2.29, 1.492, 1.579, 0.809, 0.437, 0.174, 0.069, 0.079, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    '''
    result = []
    result.append(int(wind_name.split('_')[0]))
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan)
    data[wind_name] = data[wind_name].astype('float')
    WPD_zong = np.nansum(data[wind_name].astype('float') ** 3)
    for i in np.linspace(0.5, 25, 50):
        try:
            if i == 25:
                # print(i, len(data[(data[str(cengid) + 'WSAVG'] >= i-0.5) & (data[str(cengid) + 'WSAVG'] <= i)]))
                # result.append(np.around(len(data[(data[str(cengid) + 'WSAVG'] >= i-0.5) & (data[str(cengid) + 'WSAVG'] <= i)]) / len(data)*100, 3))
                result.append(np.around(np.nansum(data[(data[wind_name] >= i - 0.5)][wind_name] ** 3) / WPD_zong *100, 3))
            else:
                result.append(np.around(np.nansum(data[(data[wind_name] >= i - 0.5) & (data[wind_name] < i)][wind_name] ** 3) / WPD_zong *100, 3))
        except:
            result.append(np.nan)
    return result


def cal_wind_yue(data, wind_name, time_name):
    '''
    计算月均风速
    :param data: pd.Dataframe
    :param wind_name: 风速列名
    :param time_name: 时间列名
    :return: 【测量高度，各个月份风速平均值】
    [30.0, 3.06, 2.99, 3.33, 3.18, 3.15, 3.72, 4.1, 3.06, 3.57, 3.96, 2.76, 2.98]
    '''
    result = []
    result.append(int(wind_name.split('_')[0]))
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan)
    data[wind_name] = data[wind_name].astype('float')
    try:
        data['time'] = data[time_name].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S").split('-')[1])
    except:
        data['time'] = data[time_name].apply(lambda x: x.split('-')[1])
    for i in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
        try:
            result.append(np.around(np.nanmean(data[data['time'] == i][wind_name]), 2))
        except:
            result.append(np.nan)
    return result

def cal_wind_day(data, wind_name, time_name):
    '''
    计算日均风速
    :param data: pd.Dataframe
    :param wind_name: 风速列名
    :param time_name: 时间列名
    :return: 【测量高度，各个小时风速平均值】
    [30.0, 3.54, 3.53, 3.44, 3.36, 3.36, 3.32, 3.3, 3.19, 3.1, 3.18, 3.28, 3.32, 3.26, 3.25, 3.24, 3.2, 3.24, 3.32, 3.57, 3.68, 3.68, 3.66, 3.6, 3.54]
    '''
    result = []
    result.append(int(wind_name.split('_')[0]))
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan)
    data[wind_name] = data[wind_name].astype('float')
    try:
        data['time'] = data[time_name].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S").split(' ')[1].split(':')[0])
    except:
        data['time'] = data[time_name].apply(lambda x: x.split(' ')[1].split(':')[0])
    for i in ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',  '20', '21', '22', '23']:
        try:
            result.append(np.around(np.nanmean(data[data['time'] == i][wind_name]), 2))
        except:
            result.append(np.nan)
    return result
def cal_wind_bin(data, wind_name):
    '''
    计算风速频率分布
    :param data: pd.Dataframe
    :param wind_name: 风速列名
    :return: 【测量高度，风速频率】
    [30.0, 3.514, 4.619, 6.007, 7.991, 9.857, 10.967, 11.585, 11.084, 10.01, 7.831, 5.927, 3.852, 2.588, 1.547, 0.981, 0.627, 0.424, 0.242, 0.133, 0.122, 0.053, 0.025, 0.009, 0.003, 0.003, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    '''
    result = []
    result.append(int(wind_name.split('_')[0]))
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan)
    data[wind_name] = data[wind_name].astype('float')
    for i in np.linspace(0.5, 25, 50):
        try:
            if i == 25:
                # print(i, len(data[(data[str(cengid) + 'WSAVG'] >= i-0.5) & (data[str(cengid) + 'WSAVG'] <= i)]))
                # result.append(np.around(len(data[(data[str(cengid) + 'WSAVG'] >= i-0.5) & (data[str(cengid) + 'WSAVG'] <= i)]) / len(data)*100, 3))
                result.append(np.around(len(data[(data[wind_name] >= i - 0.5)]) / len(data) * 100, 3))
            else:
                result.append(np.around(len(data[(data[wind_name] >= i - 0.5) & (data[wind_name] < i)]) / len(data)*100, 3))
        except:
            result.append(np.nan)
    return result

def cal_dir_WPD_bin(data, wind_name, dir_name):
    '''
    计算风能频率玫瑰图
    :param data:pd.Dataframe
    :param wind_name:风速列名
    :param dir_name:风向列名
    :return:【测量高度，各个风向区间风能频率】
    [30.0, 0.547, 5.649, 16.107, 20.576, 9.311, 3.291, 0.779, 0.638, 2.55, 11.451, 16.827, 7.499, 3.585, 0.392, 0.276, 0.521]
    '''
    result = []
    result.append(int(dir_name.split('_')[0]))
    data = data[data[dir_name] != ' ']
    data = data[data[wind_name] != ' ']
    data[dir_name] = data[dir_name].replace('None', np.nan)
    data[wind_name] = data[wind_name].replace('None', np.nan)
    data[dir_name] = data[dir_name].astype('float')
    data[wind_name] = data[wind_name].astype('float')
    WPD_zong = np.nansum(data[wind_name] ** 3)
    # for i in np.linspace(22.5, 360, 16):
    for i in np.linspace(11.25, 348.75, 16):
        # print(i, i - 22.5, i)
        try:
            if i == 11.25:
                result.append(np.around(np.nansum(data[(data[dir_name] <= i) | (data[dir_name] > 348.75)][wind_name] ** 3) / WPD_zong * 100, 3))
            else:
                result.append(np.around(np.nansum(data[(data[dir_name] > i - 22.5) & (data[dir_name] <= i)][wind_name] ** 3) / WPD_zong * 100, 3))
        except:
            result.append(np.nan)
    return result

def cal_dynamic_information(cefengta_id):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cefengta.data_%s_clean;" % cefengta_id)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list
    cursor.close()
    conn.close()
    # 生成结果表
    result = pd.DataFrame(columns=['ID', 'HIGHTNUM', 'STARTTIME', 'ENDTIME', 'SHEARX', 'SHEARY', 'rho', 'WD1', 'WD2', 'WD3', 'WP1', 'WP2', 'WP3',
                                   'MONTHWS1', 'MONTHWS2', 'MONTHWS3', 'MONTHWS4', 'MONTHWS5', 'MONTHWS6', 'MONTHWS7',
                                   'MONTHWS8', 'MONTHWS9', 'MONTHWS10', 'DAYWS1', 'DAYWS2', 'DAYWS3', 'DAYWS4', 'DAYWS5',
                                   'DAYWS6', 'DAYWS7', 'DAYWS8', 'DAYWS9', 'DAYWS10', 'WSDIS1', 'WSDIS2', 'WSDIS3',
                                   'WSDIS4', 'WSDIS5', 'WSDIS6', 'WSDIS7', 'WSDIS8', 'WSDIS9', 'WSDIS10', 'WPDIS1',
                                   'WPDIS2', 'WPDIS3', 'WPDIS4', 'WPDIS5', 'WPDIS6', 'WPDIS7', 'WPDIS8', 'WPDIS0',
                                   'WPDIS10'])
    # 开始计算
    result.loc[0, 'ID'] = cefengta_id
    time_name = 'Date_Time'
    #计算开始和结束时间
    result.loc[0, 'STARTTIME'] = data[time_name].min()
    result.loc[0, 'ENDTIME'] = data[time_name].max()
    # 计算'HIGHTNUM' 风速， 风向 个数
    list_col = data.columns
    num_dir = 0
    num_wind = 0
    fengsu_id = []
    dir_id = []
    tem_name = ' '
    P_name = ' '
    for i in list_col:
        if 'WS_AVG' in i:
            if 'ZWS' not in i:
                fengsu_id.append(int(i.split('_')[0]))
                num_wind = num_wind + 1
        if 'WD_AVG' in i:
            dir_id.append(int(i.split('_')[0]))
            num_dir = num_dir + 1
        if 'T_AVG' in i:
            tem_name = i
        if 'P_AVG' in i:
            P_name = i
    fengsu_id.sort()
    if num_wind > 10:
        num_wind = 10
        fengsu_id_1 = fengsu_id[-9:]
        fengsu_id_1.append(fengsu_id[0])
        fengsu_id_1.sort()
        fengsu_id = fengsu_id_1
    dir_id.sort()
    result.loc[0, 'HIGHTNUM'] = ','.join([str(i) for i in [num_dir] + [num_wind]])
    # 计算'SHEARX', 'SHEARY'平均风速和平均风速对应层id
    SHEARX, SHEARY = cal_wind_ceng(data, fengsu_id)
    result.loc[0, 'SHEARX'] = ','.join([str(i) for i in SHEARX])
    result.loc[0, 'SHEARY'] = ','.join([str(i) for i in SHEARY])
    # 计算'SHEAR' 100米风速4.519*np.power(120/100, shear)
    try:
        power_func = lambda x, c, a: c * np.power(x, a)
        params, cov = curve_fit(power_func, SHEARY, SHEARX)
        result.loc[0, 'SHEAR'] = '%.3f' % params[1]
    except:
        log = 'no wind'
    # 配合软件的
    # for wind_ceng in fengsu_id:
    #     wind_name_ceng = str(wind_ceng) + '_WS_AVG'
    #     data = data[data[wind_name_ceng] != ' ']
    #     data[wind_name_ceng] = data[wind_name_ceng].astype('float')
    # 计算'rho'计算空气密度
    # data[P_name] = data[P_name].astype('float') * 1000
    try:
        rho = cal_rho(data, tem_name, P_name)
        result.loc[0, 'RHO'] = '%.3f' % rho
    except:
        log = 'error'
    fengsu_id.sort(reverse=True)
    dir_id.sort(reverse=True)
    # 计算'WD1', 'WD2', 'WD3'
    try:
        if num_dir == 1:
            WD_bin_0 = cal_dir_bin(data, str(dir_id[0]) + '_WD_AVG')
            result.loc[0, 'WD1'] = ','.join([str(i) for i in WD_bin_0])
        elif num_dir == 2:
            WD_bin_0 = cal_dir_bin(data, str(dir_id[0]) + '_WD_AVG')
            result.loc[0, 'WD1'] = ','.join([str(i) for i in WD_bin_0])
            WD_bin_1 = cal_dir_bin(data, str(dir_id[1]) + '_WD_AVG')
            result.loc[0, 'WD3'] = ','.join([str(i) for i in WD_bin_1])
        else:
            WD_bin_0 = cal_dir_bin(data, str(dir_id[0]) + '_WD_AVG')
            result.loc[0, 'WD1'] = ','.join([str(i) for i in WD_bin_0])
            WD_bin_1 = cal_dir_bin(data, str(dir_id[1]) + '_WD_AVG')
            result.loc[0, 'WD2'] = ','.join([str(i) for i in WD_bin_1])
            WD_bin_2 = cal_dir_bin(data, str(dir_id[2]) + '_WD_AVG')
            result.loc[0, 'WD3'] = ','.join([str(i) for i in WD_bin_2])
    except:
        log = 'error'
    # 计算'WP1', 'WP2', 'WP3'
    # 判断同一层有风速不
    dir_wind_id = []
    dir_wind_id_num = 0
    for id in dir_id:
        if id in fengsu_id:
            dir_wind_id.append(id)
            dir_wind_id_num = dir_wind_id_num + 1
        else:
            dir_wind_id.append(min(fengsu_id, key=lambda x: abs(x - id)))
            dir_wind_id_num = dir_wind_id_num + 1
    dir_wind_id.sort(reverse=True)
    try:
        if dir_wind_id_num == 1:
            WD_bin_0 = cal_dir_WPD_bin(data, str(dir_wind_id[0]) + '_WS_AVG', str(dir_id[0]) + '_WD_AVG')
            result.loc[0, 'WP1'] = ','.join([str(i) for i in WD_bin_0])
        elif dir_wind_id_num == 2:
            WD_bin_0 = cal_dir_WPD_bin(data, str(dir_wind_id[0]) + '_WS_AVG', str(dir_id[0]) + '_WD_AVG')
            result.loc[0, 'WP1'] = ','.join([str(i) for i in WD_bin_0])
            WD_bin_1 = cal_dir_WPD_bin(data, str(dir_wind_id[1]) + '_WS_AVG', str(dir_id[1]) + '_WD_AVG')
            result.loc[0, 'WP3'] = ','.join([str(i) for i in WD_bin_1])
        else:
            WD_bin_0 = cal_dir_WPD_bin(data, str(dir_wind_id[0]) + '_WS_AVG', str(dir_id[0]) + '_WD_AVG')
            result.loc[0, 'WP1'] = ','.join([str(i) for i in WD_bin_0])
            WD_bin_1 = cal_dir_WPD_bin(data, str(dir_wind_id[1]) + '_WS_AVG', str(dir_id[1]) + '_WD_AVG')
            result.loc[0, 'WP2'] = ','.join([str(i) for i in WD_bin_1])
            WD_bin_2 = cal_dir_WPD_bin(data, str(dir_wind_id[2]) + '_WS_AVG', str(dir_id[2]) + '_WD_AVG')
            result.loc[0, 'WP3'] = ','.join([str(i) for i in WD_bin_2])
    except:
        log = 'error'
    # 计算 'MONTHWS1', 'MONTHWS2', 'MONTHWS3', 'MONTHWS4', 'MONTHWS5', 'MONTHWS6', 'MONTHWS7', 'MONTHWS8', 'MONTHWS9', 'MONTHWS10'
    try:
        if num_wind <= 10:
            for i_id, value_id in enumerate(fengsu_id):
                if i_id != num_wind-1:
                    wind_yue = cal_wind_yue(data, str(value_id) + '_WS_AVG', time_name)
                    result.loc[0, 'MONTHWS' + str(i_id+1)] = ','.join([str(i) for i in wind_yue])
                else:
                    wind_yue = cal_wind_yue(data, str(value_id) + '_WS_AVG', time_name)
                    result.loc[0, 'MONTHWS' + str(10)] = ','.join([str(i) for i in wind_yue])
    except:
        log = 'error'
    # 计算'DAYWS1', 'DAYWS2', 'DAYWS3', 'DAYWS4', 'DAYWS5', 'DAYWS6', 'DAYWS7', 'DAYWS8','DAYWS9', 'DAYWS10'
    try:
        if num_wind <= 10:
            for i_id, value_id in enumerate(fengsu_id):
                if i_id != num_wind-1:
                    wind_day = cal_wind_day(data, str(value_id) + '_WS_AVG', time_name)
                    result.loc[0, 'DAYWS' + str(i_id+1)] = ','.join([str(i) for i in wind_day])
                else:
                    wind_day = cal_wind_day(data, str(value_id) + '_WS_AVG', time_name)
                    result.loc[0, 'DAYWS' + str(10)] = ','.join([str(i) for i in wind_day])
    except:
        log = 'error'
    # 计算'WSDIS1', 'WSDIS2', 'WSDIS3', 'WSDIS4', 'WSDIS5', 'WSDIS6', 'WSDIS7', 'WSDIS8', 'WSDIS9', 'WSDIS10'
    try:
        if num_wind <= 10:
            for i_id, value_id in enumerate(fengsu_id):
                if i_id != num_wind-1:
                    wind_bin = cal_wind_bin(data, str(value_id) + '_WS_AVG')
                    result.loc[0, 'WSDIS' + str(i_id+1)] = ','.join([str(i) for i in wind_bin])
                else:
                    wind_bin = cal_wind_bin(data, str(value_id) + '_WS_AVG')
                    result.loc[0, 'WSDIS' + str(10)] = ','.join([str(i) for i in wind_bin])
    except:
        log = 'error'
    # 计算'WPDIS1', 'WPDIS2', 'WPDIS3', 'WPDIS4', 'WPDIS5', 'WPDIS6', 'WPDIS7', 'WPDIS8', 'WPDIS9', 'WPDIS10'
    try:
        if num_wind <= 10:
            for i_id, value_id in enumerate(fengsu_id):
                if i_id != num_wind-1:
                    WPD_bin = cal_WPD_bin(data, str(value_id) + '_WS_AVG')
                    result.loc[0, 'WPDIS' + str(i_id+1)] = ','.join([str(i) for i in WPD_bin])
                else:
                    WPD_bin = cal_WPD_bin(data, str(value_id) + '_WS_AVG')
                    result.loc[0, 'WPDIS' + str(10)] = ','.join([str(i) for i in WPD_bin])
    except:
        log = 'error'
    return result


def insert_dynamic_information(cefengta_id):
    table_name = 'dynamic_information'

    data = cal_dynamic_information(cefengta_id)
    # # 链接数据库
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断是否存在动态信息表，不存在就创建
    if not cursor.execute("SHOW TABLES LIKE '%s';" % table_name):
        create_word = 'CREATE TABLE dynamic_information (ID VARCHAR(300), HIGHTNUM VARCHAR(300), STARTTIME VARCHAR(300), ENDTIME VARCHAR(300), SHEARX VARCHAR(300), SHEARY VARCHAR(300), p VARCHAR(300), WD1 VARCHAR(300), WD2 VARCHAR(300), WD3 VARCHAR(300), WP1 VARCHAR(300), WP2 VARCHAR(300), WP3 VARCHAR(300), MONTHWS1 VARCHAR(300), MONTHWS2 VARCHAR(300), MONTHWS3 VARCHAR(300), MONTHWS4 VARCHAR(300), MONTHWS5 VARCHAR(300), MONTHWS6 VARCHAR(300), MONTHWS7 VARCHAR(300), MONTHWS8 VARCHAR(300), DAYWS1 VARCHAR(300), DAYWS2 VARCHAR(300), DAYWS3 VARCHAR(300), DAYWS4 VARCHAR(300), DAYWS5 VARCHAR(300), DAYWS6 VARCHAR(300), DAYWS7 VARCHAR(300), DAYWS8 VARCHAR(300), WSDIS1 VARCHAR(300), WSDIS2 VARCHAR(300), WSDIS3 VARCHAR(300), WSDIS4 VARCHAR(300), WSDIS5 VARCHAR(300), WSDIS6 VARCHAR(300), WSDIS7 VARCHAR(300), WSDIS8 VARCHAR(300), WPDIS1 VARCHAR(300), WPDIS2 VARCHAR(300), WPDIS3 VARCHAR(300), WPDIS4 VARCHAR(300), WPDIS5 VARCHAR(300), WPDIS6 VARCHAR(300), WPDIS7 VARCHAR(300), WPDIS8 VARCHAR(300), PRIMARY KEY(ID));'
        cursor.execute(create_word)
        conn.commit()
    if cursor.execute("SELECT ID FROM cefengta.dynamic_information where ID = '%s';" % cefengta_id) == 1:
        cursor.execute("DELETE FROM `cefengta`.`dynamic_information` WHERE `ID`='%s';" % cefengta_id)
        conn.commit()
    # 插入数据，一行插入一次
    for i in range(len(data)):
        part_1 = ""
        part_2 = ""
        for col_name in data.columns:
            if data[col_name][i] == data[col_name][i]:
                if len(part_1) != 0:
                    part_1 += "," + col_name
                    if isinstance(data[col_name][i], str):
                        part_2 += ",'" + data[col_name][i] + "'"
                    else:
                        part_2 += ",'" + str(data[col_name][i]) + "'"
                else:
                    part_1 += col_name
                    if isinstance(data[col_name][i], str):
                        part_2 += "'" + data[col_name][i] + "'"
                    else:
                        part_2 += "'" + str(data[col_name][i]) + "'"
        sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % (table_name, part_1, part_2)
        cursor.execute(sql_insert)
        conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    # cefengta_id = '003470'
    # cal_dynamic_information(cefengta_id)
    # cefengta_id = 'M003470'
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        insert_dynamic_information(cefengta_id)

