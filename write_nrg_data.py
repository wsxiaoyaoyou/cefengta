import os
import datetime
import shutil
from pathlib import Path
from scipy.optimize import curve_fit
import subprocess
import pandas as pd
import zipfile
import chardet
import numpy as np
import pymysql
import sys
import nrgpy
import warnings
warnings.filterwarnings("ignore")

host = 'localhost'
port = 3306
user = 'wyh' #用户名
password = 'Wyh123!@#' # 密码
database = 'cefengta'


def time_format_test(time_str, time_format):
    try:
        datetime.datetime.strptime(time_str, time_format)
        return True
    except:
        return False


def read_brakline(file_path):
    with open(file_path, 'r') as f:
        data = f.readlines()
    break_line = 0
    for i in range(0, len(data)):
        if data[i][:2] == '20':
            break_line = i - 1
            break
    return break_line

def test_format(time):
    if time_format_test(time, '%Y-%m-%d %H:%M:%S'):
        time_format = '%Y-%m-%d %H:%M:%S'
    elif time_format_test(time, '%Y-%m-%d %H:%M'):
        time_format = '%Y-%m-%d %H:%M'
    elif time_format_test(time, '%Y/%m/%d %H:%M:%S'):
        time_format = '%Y/%m/%d %H:%M:%S'
    elif time_format_test(time, '%Y/%m/%d %H:%M'):
        time_format = '%Y/%m/%d %H:%M'
    elif time_format_test(time, '%Y-%m-%d %H:%M:%S.%f'):
        time_format = '%Y-%m-%d %H:%M:%S.%f'
    return time_format


def create_cefeng_table(table_name, columns_sql):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if not cursor.execute("SHOW TABLES LIKE '%s';" % table_name):
        db_creat = "CREATE TABLE " + table_name + " ("
        for i in columns_sql:
            db_creat += i + " "
            db_creat += "VARCHAR(50), "
        db_creat += "PRIMARY KEY(Date_Time)) DEFAULT CHARSET=utf8mb4;"
        db_creat = db_creat.replace('.', '_')
        cursor.execute(db_creat)
    cursor.close()
    conn.close()


def write_data(data, table_name):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    num = 0
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
        # part_1 = part_1.replace('.', '_')
        sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % (table_name, part_1, part_2)
        # print(sql_insert)
        cursor.execute(sql_insert)
        num += 1
        if num > 1000:
            conn.commit()
            num = 0

    conn.commit()
    cursor.close()
    conn.close()


def read_columns(cefeng_name):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute("SELECT ORIGINCHANNEL, USEDCHANNEL FROM cefengta.channel_configuration where ID = '%s';" % cefeng_name)
    # # 获取查询结果
    # 获取表头
    col_name_list1 = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list1
    data = data[['ORIGINCHANNEL', 'USEDCHANNEL']]
    data = data.replace('', np.nan)
    data.dropna(inplace=True)
    return data['ORIGINCHANNEL'].tolist(), data['USEDCHANNEL'].tolist()


def write_log(cefeng_name, upload_file, upload_time, state):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    select_sql = "SELECT NAME FROM static_information where ID = '%s';" % (cefeng_name)
    cursor.execute(select_sql)
    cefengtaname = cursor.fetchone()[0]
    table_name = 'data_log_information'
    if not cursor.execute("SHOW TABLES LIKE 'data_log_information';"):
        creat_table = "CREATE TABLE data_log_information (Task_number INT NOT NULL AUTO_INCREMENT," \
                      "ID VARCHAR(45) NULL,NAME VARCHAR(45) NULL,UPLOAD_FILE VARCHAR(45) NULL,UPLOAD_TIME VARCHAR(45) NULL," \
                      "STATE VARCHAR(45) NULL,PRIMARY KEY (Task_number));"
        cursor.execute(creat_table)
        conn.commit()
    # 插入数据
    select_tasknumber = "select Task_number from cefengta.data_log_information where ID = '%s';" % (cefeng_name)
    cursor.execute(select_tasknumber)
    task_number = cursor.fetchall()
    task_number = pd.DataFrame(task_number)
    if len(task_number) > 0:
        task_number.columns = ['num']
        for num in task_number['num']:
            delete = "delete from cefengta.data_log_information where Task_number = '%s';" % num
            cursor.execute(delete)
    conn.commit()
    part_1 = "ID,NAME,UPLOAD_FILE,UPLOAD_TIME,STATE"
    part_2 = "'" + cefeng_name + "','" + cefengtaname + "','" + upload_file + "','" + upload_time + "','" + state + "'"
    sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % (table_name, part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    cursor.close()
    conn.close()


def find_discontinuities(lst):
    lst.sort(reverse=False)
    discontinuities = []
    for i in range(len(lst) - 1):
        if lst[i] + 1 != lst[i + 1]:  # 对于升序序列
            discontinuities.append(i + 1)  # 记录不连续的位置（索引+1）
    result_time = pd.DataFrame()
    if len(discontinuities) != 0:
        for index in range(len(discontinuities) + 1):
            if index == 0:
                result_time.loc[index, 'start'] = lst[0]
                result_time.loc[index, 'end'] = lst[discontinuities[index]-1]
            elif index == len(discontinuities):
                result_time.loc[index, 'start'] = lst[discontinuities[index-1]]
                result_time.loc[index, 'end'] = lst[len(lst) - 1]
            else:
                result_time.loc[index, 'start'] = lst[discontinuities[index-1]]
                result_time.loc[index, 'end'] = lst[discontinuities[index]-1]
    else:
        result_time.loc[0, 'start'] = lst[0]
        result_time.loc[0, 'end'] = lst[-1]
    return result_time

# JN01	风速波动合理性范围	1小时内平均风速变化≥0.001m/s,且1小时内平均风速变化<6m/s；
def rule_JN01(Data, wind_name, th1=0.001, th2=6.0):
    Data[wind_name].fillna(np.nan, inplace=True)
    Data[wind_name] = Data[wind_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[wind_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = wind_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = []
    if len(Data) > 6:
        for i in range(0, len(Data)-5):
            cal_data = Data.loc[i:i+5, wind_name]
            if (np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 3) < th1) | (np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 3) >= th2):
                index_list = index_list + list(range(i, i + 6))
    index_list = list(set(index_list))
    Data.loc[index_list, wind_name] = np.nan
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = wind_name + '-' + 'JN01'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na


# JN02	风速数值合理性范围	平均风速≥0m/s，且平均风速≤50m/s；
def rule_JN02(Data, wind_name, th1=0.0, th2=50.0):
    Data[wind_name].fillna(np.nan, inplace=True)
    Data[wind_name] = Data[wind_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[wind_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = wind_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = np.where((Data[wind_name] < th1) | (Data[wind_name] > th2))[0]
    index_list = list(set(index_list))

    Data[wind_name] = Data[wind_name].apply(lambda x: np.nan if (x < th1) | (x > th2) else x)
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = wind_name + '-' + 'JN02'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na


# JN03	风速标准差合理性范围	风速标准差≥0m/s，且风速标准差≤5m/s；
def rule_JN03(Data, wind_SD_name, th1=0.0, th2=5.0):
    Data[wind_SD_name].fillna(np.nan, inplace=True)
    Data[wind_SD_name] = Data[wind_SD_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[wind_SD_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = wind_SD_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = np.where((Data[wind_SD_name] < th1) | (Data[wind_SD_name] > th2))[0]
    index_list = list(set(index_list))

    Data['diff'] = Data[wind_SD_name].diff()
    Data['diff'] = Data['diff'].apply(lambda x: abs(x))
    Data[wind_SD_name] = Data[wind_SD_name].apply(lambda x: np.nan if (x < th1) | (x > th2) else x)

    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = wind_SD_name + '-' + 'JN03'
    else:
        result_time = pd.DataFrame()
    Data.drop(columns='diff', inplace=True)
    return Data, result_time, result_na


# JN04	风向波动合理性范围	1小时内平均风向变化≥0.001°；
def rule_JN04(Data, dir_name, th1=0.001):
    Data[dir_name].fillna(np.nan, inplace=True)
    Data[dir_name] = Data[dir_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[dir_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = dir_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = []
    if len(Data) > 6:
        for i in range(0, len(Data)-5):
            cal_data = Data.loc[i:i + 5, dir_name]
            if np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 1) < th1:
                index_list = index_list + list(range(i, i + 6))
    index_list = list(set(index_list))
    Data.loc[index_list, dir_name] = np.nan
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = dir_name + '-' + 'JN04'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na


# JN05	风向数值合理性范围	平均风向≥0°，且平均风向≤360°；
def rule_JN05(Data, dir_name, th1=0.0, th2=360.0):
    Data[dir_name].fillna(np.nan, inplace=True)
    Data[dir_name] = Data[dir_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[dir_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = dir_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = np.where((Data[dir_name] < th1) | (Data[dir_name] > th2))[0]
    index_list = list(set(index_list))

    Data[dir_name] = Data[dir_name].apply(lambda x: np.nan if (x < th1) | (x > th2) else x)

    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = dir_name + '-' + 'JN05'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na


# JN06	气温波动合理性范围	1小时内平均气温变化<5°C；
def rule_JN06(Data, tem_name, th1=5.0):
    Data[tem_name].fillna(np.nan, inplace=True)
    Data[tem_name] = Data[tem_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[tem_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = tem_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = []
    if len(Data) > 6:
        for i in range(0, len(Data)-5):
            cal_data = Data.loc[i:i+5, tem_name]
            if np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 1) >= th1:
                index_list = index_list + list(range(i, i+6))
    index_list = list(set(index_list))
    Data.loc[index_list, tem_name] = np.nan
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = tem_name + '-' + 'JN06'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na


# JN07	气温数值合理性范围	平均气温≥-40°C，且平均气温≤50°C
def rule_JN07(Data, tem_name, th1=-40.0, th2=50.0):
    Data[tem_name].fillna(np.nan, inplace=True)
    Data[tem_name] = Data[tem_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[tem_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = tem_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = np.where((Data[tem_name] < th1) | (Data[tem_name] > th2))[0]
    index_list = list(set(index_list))

    Data[tem_name] = Data[tem_name].apply(lambda x: np.nan if (x < th1) | (x > th2) else x)

    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = tem_name + '-' + 'JN07'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na


# JN08	气压波动合理性范围	3小时内平均气压变化<1kPA；
def rule_JN08(Data, p_name, th1=1.0):
    # 注意单位问题
    Data[p_name].fillna(np.nan, inplace=True)
    Data[p_name] = Data[p_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[p_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = p_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = []
    if len(Data) > 18:
        for i in range(0, len(Data)-17):
            cal_data = Data.loc[i:i + 17, p_name]
            if np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 1) >= th1*1000:
                index_list = index_list + list(range(i, i+18))
    index_list = list(set(index_list))
    Data.loc[index_list, p_name] = np.nan
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = p_name + '-' + 'JN08'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na


# JN09	气压数值合理性范围	平均气压≥50kPA，且平均气压≤110kPA；
def rule_JN09(Data, p_name, th1=50.0, th2=110.0):
    # 注意单位问题
    Data[p_name].fillna(np.nan, inplace=True)
    Data[p_name] = Data[p_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A = Data[Data[p_name].isna()].index.tolist()
    if len(na_in_A) > 0:
        result_na = find_discontinuities(na_in_A)
        result_na['start'] = result_na['start'].apply(lambda x: Data['Date_Time'][x])
        result_na['end'] = result_na['end'].apply(lambda x: Data['Date_Time'][x])
        result_na['can_name'] = p_name
        result_na['warnType'] = '缺测'
    else:
        result_na = pd.DataFrame()
    index_list = np.where((Data[p_name] < th1 * 1000) | (Data[p_name] > th2 * 1000))[0]
    index_list = list(set(index_list))
    Data[p_name] = Data[p_name].apply(lambda x: np.nan if (x < th1*1000) | (x > th2*1000) else x)

    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = p_name + '-' + 'JN09'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na


# JN10	风速相关性合理范围	50m高平均风速与30米高平均风速差值<2m/s，50m高平均风速与10米高平均风速差值<4m/s；
def rule_JN10(Data, wind_name_10, wind_name_30, wind_name_50, th1=2.0, th2=4.0):
    Data[wind_name_10].fillna(np.nan, inplace=True)
    Data[wind_name_10] = Data[wind_name_10].replace('None', np.nan).astype('float')
    Data[wind_name_30].fillna(np.nan, inplace=True)
    Data[wind_name_30] = Data[wind_name_30].replace('None', np.nan).astype('float')
    Data[wind_name_50].fillna(np.nan, inplace=True)
    Data[wind_name_50] = Data[wind_name_50].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)

    na_in_A1 = Data[Data[wind_name_10].isna()].index.tolist()
    if len(na_in_A1) > 0:
        result_na1 = find_discontinuities(na_in_A1)
        result_na1['start'] = result_na1['start'].apply(lambda x: Data['Date_Time'][x])
        result_na1['end'] = result_na1['end'].apply(lambda x: Data['Date_Time'][x])
        result_na1['can_name'] = wind_name_10
        result_na1['warnType'] = '缺测'
    else:
        result_na1 = pd.DataFrame()
    na_in_A2 = Data[Data[wind_name_30].isna()].index.tolist()
    if len(na_in_A2) > 0:
        result_na2 = find_discontinuities(na_in_A2)
        result_na2['start'] = result_na2['start'].apply(lambda x: Data['Date_Time'][x])
        result_na2['end'] = result_na2['end'].apply(lambda x: Data['Date_Time'][x])
        result_na2['can_name'] = wind_name_30
        result_na2['warnType'] = '缺测'
    else:
        result_na2 = pd.DataFrame()
    na_in_A3 = Data[Data[wind_name_50].isna()].index.tolist()
    if len(na_in_A3) > 0:
        result_na3 = find_discontinuities(na_in_A3)
        result_na3['start'] = result_na3['start'].apply(lambda x: Data['Date_Time'][x])
        result_na3['end'] = result_na3['end'].apply(lambda x: Data['Date_Time'][x])
        result_na3['can_name'] = wind_name_50
        result_na3['warnType'] = '缺测'
    else:
        result_na3 = pd.DataFrame()
    result_na = pd.concat([result_na1, result_na2])
    result_na = pd.concat([result_na, result_na3])

    Data['diff'] = Data[wind_name_50] - Data[wind_name_30]
    index_list1 = np.where(Data['diff'] > th1)[0]
    index_list1 = list(set(index_list1))
    if len(index_list1) > 0:
        result_time1 = find_discontinuities(index_list1)
        result_time1['start'] = result_time1['start'].apply(lambda x: Data['Date_Time'][x])
        result_time1['end'] = result_time1['end'].apply(lambda x: Data['Date_Time'][x])
        result_time1['can_name'] = wind_name_30 + '_' + wind_name_50 + '-' + 'JN10'
    else:
        result_time1 = pd.DataFrame()
    Data['diff'] = Data[wind_name_50] - Data[wind_name_10]
    # Data.loc[Data['diff'] >= th2, [wind_name_10, wind_name_50]] = np.nan
    # Data.drop(columns='diff', inplace=True)
    index_list2 = np.where(Data['diff'] > th2)[0]
    index_list2 = list(set(index_list2))
    if len(index_list2) > 0:
        result_time2 = find_discontinuities(index_list2)
        result_time2['start'] = result_time2['start'].apply(lambda x: Data['Date_Time'][x])
        result_time2['end'] = result_time2['end'].apply(lambda x: Data['Date_Time'][x])
        result_time2['can_name'] = wind_name_10 + '_' + wind_name_50 + '-' + 'JN10'
    else:
        result_time2 = pd.DataFrame()
    result_time = pd.concat([result_time1, result_time2])

    Data['diff'] = Data[wind_name_50] - Data[wind_name_30]
    Data.loc[Data['diff'] >= th1, [wind_name_30, wind_name_50]] = np.nan
    Data['diff'] = Data[wind_name_50] - Data[wind_name_10]
    Data.loc[Data['diff'] >= th2, [wind_name_10, wind_name_50]] = np.nan
    Data.drop(columns='diff', inplace=True)
    return Data, result_time, result_na


# JN11	风向相关性合理范围	50m高平均风向与30m高平均风向差值<22.5°；
def rule_JN11(Data, dir_name_30, dir_name_50, th1=22.5):
    Data[dir_name_30].fillna(np.nan, inplace=True)
    Data[dir_name_30] = Data[dir_name_30].replace('None', np.nan).astype('float')
    Data[dir_name_50].fillna(np.nan, inplace=True)
    Data[dir_name_50] = Data[dir_name_50].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A1 = Data[Data[dir_name_30].isna()].index.tolist()
    if len(na_in_A1) > 0:
        result_na1 = find_discontinuities(na_in_A1)
        result_na1['start'] = result_na1['start'].apply(lambda x: Data['Date_Time'][x])
        result_na1['end'] = result_na1['end'].apply(lambda x: Data['Date_Time'][x])
        result_na1['can_name'] = dir_name_30
        result_na1['warnType'] = '缺测'
    else:
        result_na1 = pd.DataFrame()
    na_in_A2 = Data[Data[dir_name_50].isna()].index.tolist()
    if len(na_in_A2) > 0:
        result_na2 = find_discontinuities(na_in_A2)
        result_na2['start'] = result_na2['start'].apply(lambda x: Data['Date_Time'][x])
        result_na2['end'] = result_na2['end'].apply(lambda x: Data['Date_Time'][x])
        result_na2['can_name'] = dir_name_50
        result_na2['warnType'] = '缺测'
    else:
        result_na2 = pd.DataFrame()
    result_na = pd.concat([result_na1, result_na2])
    Data['diff'] = Data[dir_name_50] - Data[dir_name_30]

    index_list = np.where(Data['diff'] > th1)[0]
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = dir_name_30 + '_' + dir_name_50 + '-' + 'JN11'
    else:
        result_time = pd.DataFrame()
    Data.loc[Data['diff'] >= th1, [dir_name_30, dir_name_50]] = np.nan
    Data.drop(columns='diff', inplace=True)
    return Data, result_time, result_na

# JN12，未发生冰冻：12小时内平均气温≥4°C，或平均风速变化大于等于0.001m/s
def rule_JN12(Data, wind_name, tem_name, th2=0.001, th1=4.0):
    Data[wind_name].fillna(np.nan, inplace=True)
    Data[wind_name] = Data[wind_name].astype('float')
    Data[tem_name].fillna(np.nan, inplace=True)
    Data[tem_name] = Data[tem_name].astype('float')
    Data.reset_index(inplace=True, drop=True)
    na_in_A1 = Data[Data[wind_name].isna()].index.tolist()
    if len(na_in_A1) > 0:
        result_na1 = find_discontinuities(na_in_A1)
        result_na1['start'] = result_na1['start'].apply(lambda x: Data['Date_Time'][x])
        result_na1['end'] = result_na1['end'].apply(lambda x: Data['Date_Time'][x])
        result_na1['can_name'] = wind_name
        result_na1['warnType'] = '缺测'
    else:
        result_na1 = pd.DataFrame()
    na_in_A2 = Data[Data[tem_name].isna()].index.tolist()
    if len(na_in_A2) > 0:
        result_na2 = find_discontinuities(na_in_A2)
        result_na2['start'] = result_na2['start'].apply(lambda x: Data['Date_Time'][x])
        result_na2['end'] = result_na2['end'].apply(lambda x: Data['Date_Time'][x])
        result_na2['can_name'] = tem_name
        result_na2['warnType'] = '缺测'
    else:
        result_na2 = pd.DataFrame()
    result_na = pd.concat([result_na1, result_na2])
    index_list = []
    if len(Data) > 72:
        for i in range(0, len(Data) - 71):
            cal_data = Data.loc[i:i + 71, wind_name]
            cal_data2 = Data.loc[i:i + 71, tem_name]
            if (np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 3) < th2) & (
                    np.around(np.nanmean(cal_data2), 3) < th1):
                index_list = index_list + list(range(i, i + 72))
    index_list = list(set(index_list))
    Data.loc[index_list, wind_name] = np.nan
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = wind_name + '-' + 'JN12'
    else:
        result_time = pd.DataFrame()
    return Data, result_time, result_na

def create_cefeng_table_clean(table_name, read_data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if not cursor.execute("SHOW TABLES LIKE '%s';" % table_name):
        db_creat = "CREATE TABLE " + table_name + " ("
        for i in read_data.columns:
            db_creat += i + " "
            db_creat += "VARCHAR(50), "
        db_creat += "PRIMARY KEY(Date_Time)) DEFAULT CHARSET=utf8;"
        db_creat = db_creat.replace('.', '_')
        # print(db_creat)
        cursor.execute(db_creat)
    cursor.close()
    conn.close()


def insert_data_clean(table_name, data):
    # # 链接数据库
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断是否存在清洗后数据表，不存在就创建
    if not cursor.execute("SHOW TABLES LIKE '%s';" % table_name):
        create_cefeng_table_clean(table_name, data)

    # 插入数据
    num = 0
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
        sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % (table_name, part_1, part_2.replace("'None'", "NULL"))
        cursor.execute(sql_insert)
        num += 1
        if num > 1000:
            conn.commit()
            num = 0
    if num < 1000:
        conn.commit()
    cursor.close()
    conn.close()


def cal_name_CHANNELNAME(USEDCHANNEL):
    try:
        name_split = USEDCHANNEL.split('_')
        if name_split[1] == 'WS':
            can = '风速'
        elif name_split[1] == 'ZWS':
            can = '其他方向风速'
        elif name_split[1] == 'WD':
            can = '风向'
        elif name_split[1] == 'T':
            can = '气温'
        elif name_split[1] == 'P':
            can = '气压'
        elif name_split[1] == 'V':
            can = '电池'
        elif name_split[1] == 'RH':
            can = '相对湿度'
        elif name_split[1] == 'REL':
            can = '可靠性'
        else:
            can = ''
        if len(name_split) > 2:
            if name_split[2] == 'AVG':
                type = '均值'
            elif name_split[2] == 'SD':
                type = '标准差'
            elif name_split[2] == 'MIN':
                type = '最小值'
            elif name_split[2] == 'MAX':
                type = '最大值'
        else:
            type=''
        return name_split[0] + 'm高度' + can + type
    except:
        return ''

def insert_warning_result(RESULT, cefengta_id):
    # # 链接数据库
    table_name = 'warning_result'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断是否存在清洗后数据表，不存在就创建
    if not cursor.execute("SHOW TABLES LIKE '%s';" % table_name):
        creat_table = "CREATE TABLE warning_result (tagID INT NOT NULL AUTO_INCREMENT," \
                      "ID VARCHAR(45) NULL,NAME VARCHAR(45) NULL,channelID VARCHAR(45) NULL,channelName VARCHAR(45) NULL,tagType VARCHAR(45) NULL,tagTime VARCHAR(45) NULL, PRIMARY KEY (tagID));"
        cursor.execute(creat_table)
    select_sql = "SELECT NAME FROM static_information where ID = '%s';" % (cefengta_id)
    cursor.execute(select_sql)
    cefengtaname = cursor.fetchone()[0]
    # 插入数据
    num = 0
    for i in range(len(RESULT)):
        part_1 = "ID,NAME,channelID,channelName,tagTime,tagType"
        part_2 = "'" + cefengta_id + "','" + cefengtaname + "','" + RESULT['can_name'][i].split('-')[0] + "','" + cal_name_CHANNELNAME(RESULT['can_name'][i].split('-')[0])+ "','" + \
                 RESULT['start'][i] + '-' + RESULT['end'][i] + "','" + RESULT['warnType'][
                     i] + "'"
        sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % (table_name, part_1, part_2.replace("'None'", "NULL"))
        print(sql_insert)
        cursor.execute(sql_insert)
        print(sql_insert.replace("'None'", "NULL"))
        cursor.execute(sql_insert.replace("'None'", "NULL"))
        num += 1
        if num > 1000:
            conn.commit()
            num = 0
    if num < 1000:
        conn.commit()
    if len(RESULT) > 0:
        sql_update = "UPDATE static_information set %s='%s' where ID = '%s';" % ('STATUS', '3', cefengta_id)
        cursor.execute(sql_update)
        conn.commit()
    cursor.close()
    conn.close()

def delete_warning_result(cefeng_name):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if cursor.execute("SHOW TABLES LIKE '%s';" % 'warning_result'):
        select_tasknumber = "select tagID from cefengta.warning_result where ID = '%s';" % (cefeng_name)
        cursor.execute(select_tasknumber)
        task_number = cursor.fetchall()
        task_number = pd.DataFrame(task_number)
        if len(task_number) > 0:
            task_number.columns = ['num']
            for num in task_number['num']:
                delete = "delete from cefengta.warning_result where tagID = '%s';" % num
                cursor.execute(delete)
    conn.commit()
    cursor.close()
    conn.close()

def main_data_clean_rule(cefengta_id, start_time, end_time):
    table_name = 'data_' + cefengta_id + '_clean'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cefengta.data_%s_yuanshi where Date_Time >= '%s' and Date_Time <='%s';" % (cefengta_id, start_time, end_time))
    # # 获取查询结果
    # 获取表头
    col_name_list1 = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list1
    # 查询通道配置表，查看压强单位，然后规范化到Pa
    cursor.execute(
        "SELECT UNIT, USEDCHANNEL FROM cefengta.channel_configuration where ID='%s' and USEDCHANNEL like '%%_P_AVG';" % cefengta_id)
    p_value = cursor.fetchall()
    # 根据风机编号筛选规则
    # 查询静态信息表static_information，查询清洗规则TEMPLET_ID，然后匹配verify_template ID,查询RULES_ID, 对应查看verify_rules, ID,读取SUBJECTION,返回THRESHOLD1,THRESHOLD2
    sql_select_RULES_ID = "select t2.RULES_ID from cefengta.static_information as t1 inner join cefengta.verify_template as t2 on t1.TEMPLET_ID = t2.ID where t1.ID='%s';" % cefengta_id
    cursor.execute(sql_select_RULES_ID)
    RULES_ID = cursor.fetchone()
    # 根据读取的规则表判断需要哪些条件
    # 读取规则表 查看需要哪些清洗规则
    cursor.execute("SELECT * FROM cefengta.verify_rules;")
    col_name_list1 = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data_rules = pd.DataFrame(values)
    data_rules.columns = col_name_list1
    cursor.close()
    conn.close()
    # if len(RULES_ID) > 0:

    # 把列名规整一下
    WS_list = []
    WS_SD_list = []
    WD_list = []
    T_list = []
    P_list = []
    for i in data.columns:
        if ('WS' in i) & ('AVG' in i):
            WS_list.append(i)
        elif ('WS' in i) & ('SD' in i):
            WS_SD_list.append(i)
        elif ('WD' in i) & ('AVG' in i):
            WD_list.append(i)
        elif ('T' in i) & ('AVG' in i):
            T_list.append(i)
        elif ('P' in i) & ('AVG' in i):
            P_list.append(i)
    if len(p_value) > 0:
        data_p_value = pd.DataFrame(p_value)
        data_p_value.columns = ['UNIT', 'USEDCHANNEL']
        for i, p_col in enumerate(data_p_value['USEDCHANNEL']):
            if (data_p_value.loc[i, 'UNIT'] == 'kPa') | (data_p_value.loc[i, 'UNIT'] == 'KPa'):
                p_unit = 1000
            elif data_p_value.loc[i, 'UNIT'] == 'hPa':
                p_unit = 100
            elif data_p_value.loc[i, 'UNIT'] == 'mb':
                p_unit = 100
            elif data_p_value.loc[i, 'UNIT'] == 'mmHg':
                p_unit = 133
            else:
                p_unit = 1
            data[p_col] = data[p_col].replace(' ', np.nan)
            data[p_col] = data[p_col].replace('None', np.nan).astype('float') * p_unit
            if p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MIN' in data.columns:
                data[p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MIN'] = data[p_col.split('_')[0] + '_' +
                                                                                           p_col.split('_')[
                                                                                               1] + '_' + 'MIN'].replace(
                    'None', np.nan).astype('float') * p_unit
            if p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MAX' in data.columns:
                data[p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MAX'] = data[p_col.split('_')[0] + '_' +
                                                                                           p_col.split('_')[
                                                                                               1] + '_' + 'MAX'].replace(
                    'None', np.nan).astype('float') * p_unit
    # 这里在清洗数据
    RESULT = pd.DataFrame()
    delete_warning_result(cefengta_id)
    if RULES_ID != None:
        if len(RULES_ID[0]) > 0:
            for rule in RULES_ID[0].split(','):
                data_sub = data_rules[data_rules['ID'] == rule]['SUBJECTION'].values[0]
                THRESHOLD1 = float(data_rules[data_rules['ID'] == rule]['THRESHOLD1'].values[0])
                if data_rules[data_rules['ID'] == rule]['THRESHOLD2'].values[0] != '':
                    THRESHOLD2 = float(data_rules[data_rules['ID'] == rule]['THRESHOLD2'].values[0])
                # JN01	风速波动合理性范围	1小时内平均风速变化≥0.001m/s,且1小时内平均风速变化<6m/s；
                if (data_sub == 'JN01') & (WS_list != []):
                    for WS_col in WS_list:
                        data, result_time, result_na = rule_JN01(data, WS_col, th1=THRESHOLD1, th2=THRESHOLD2)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN02	风速数值合理性范围	平均风速≥0m/s，且平均风速≤50m/s；
                elif (data_sub == 'JN02') & (WS_list != []):
                    for WS_col in WS_list:
                        data, result_time, result_na = rule_JN02(data, WS_col, th1=THRESHOLD1, th2=THRESHOLD2)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN03	风速标准差合理性范围	风速标准差≥0m/s，且风速标准差≤5m/s；
                elif (data_sub == 'JN03') & (WS_SD_list != []):
                    for WS_SD_col in WS_SD_list:
                        data, result_time, result_na = rule_JN03(data, WS_SD_col, th1=THRESHOLD1, th2=THRESHOLD2)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN04	风向波动合理性范围	1小时内平均风向变化≥0.001°；
                elif (data_sub == 'JN04') & (WD_list != []):
                    for WD_col in WD_list:
                        data, result_time, result_na = rule_JN04(data, WD_col, th1=THRESHOLD1)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN05	风向数值合理性范围	平均风向≥0°，且平均风向≤360°；
                elif (data_sub == 'JN05') & (WD_list != []):
                    for WD_col in WD_list:
                        data, result_time, result_na = rule_JN05(data, WD_col, th1=THRESHOLD1, th2=THRESHOLD2)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN06	气温波动合理性范围	1小时内平均气温变化<5°C；
                elif (data_sub == 'JN06') & (T_list != []):
                    for T_col in T_list:
                        data, result_time, result_na = rule_JN06(data, T_col, th1=THRESHOLD1)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN07	气温数值合理性范围	平均气温≥-40°C，且平均气温≤50°C
                elif (data_sub == 'JN07') & (T_list != []):
                    for T_col in T_list:
                        data, result_time, result_na = rule_JN07(data, T_col, th1=THRESHOLD1, th2=THRESHOLD2)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN08	气压波动合理性范围	3小时内平均气压变化<1kPA；
                elif (data_sub == 'JN08') & (P_list != []):
                    for P_col in P_list:
                        data, result_time, result_na = rule_JN08(data, P_col, th1=THRESHOLD1)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN09	气压数值合理性范围	平均气压≥50kPA，且平均气压≤110kPA；
                elif (data_sub == 'JN09') & (P_list != []):
                    for P_col in P_list:
                        data, result_time, result_na = rule_JN09(data, P_col, th1=THRESHOLD1, th2=THRESHOLD2)
                        if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                        'JN11', 'JN12']:
                            result_time['warnType'] = '可疑'
                        result_time['rules_ID'] = rule
                        RESULT = pd.concat([RESULT, result_time])
                        RESULT = pd.concat([RESULT, result_na])
                # JN10	风速相关性合理范围	50m高平均风速与30米高平均风速差值<2m/s，50m高平均风速与10米高平均风速差值<4m/s；
                elif (data_sub == 'JN10') & ('10_WS_AVG' in WS_list) & ('30_WS_AVG' in WS_list) & (
                        '50_WS_AVG' in WS_list):
                    data, result_time, result_na = rule_JN10(data, '10_WS_AVG', '30_WS_AVG', '50_WS_AVG',
                                                             th1=THRESHOLD1, th2=THRESHOLD2)
                    if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                    'JN11', 'JN12']:
                        result_time['warnType'] = '可疑'
                    result_time['rules_ID'] = rule
                    RESULT = pd.concat([RESULT, result_time])
                    RESULT = pd.concat([RESULT, result_na])
                # JN11	风向相关性合理范围	50m高平均风向与30m高平均风向差值<22.5°；
                elif (data_sub == 'JN11') & ('30_WD_AVG' in WS_list) & ('50_WD_AVG' in WS_list):
                    data, result_time, result_na = rule_JN11(data, '30_WD_AVG', '50_WD_AVG', th1=THRESHOLD1)
                    if rule not in ['JN01', 'JN02', 'JN03', 'JN04', 'JN05', 'JN06', 'JN07', 'JN08', 'JN09', 'JN10',
                                    'JN11', 'JN12']:
                        result_time['warnType'] = '可疑'
                    result_time['rules_ID'] = rule
                    RESULT = pd.concat([RESULT, result_time])
                    RESULT = pd.concat([RESULT, result_na])
                # JN12，未发生冰冻：12小时内平均气温≥4°C，或平均风速变化大于等于0.001m/s
                elif (data_sub == 'JN12') & (T_list != []) & (WS_SD_list != []):
                    for T_col in T_list:
                        for WS_col in WS_list:
                            data, result_time, result_na = rule_JN12(data, WS_col, T_col, th1=THRESHOLD1,
                                                                     th2=THRESHOLD2)
                            result_time['warnType'] = '冰冻'
                            RESULT = pd.concat([RESULT, result_time])
                            RESULT = pd.concat([RESULT, result_na])

    # 这里在插入数据
    data.replace(np.nan, 'None', inplace=True)
    insert_data_clean(table_name, data)
    RESULT.reset_index(inplace=True, drop=True)
    RESULT = RESULT.drop_duplicates(keep='first')
    RESULT.reset_index(inplace=True, drop=True)
    if len(RESULT) > 0:
        RESULT1 = RESULT[RESULT['warnType'] == '缺测']
        RESULT1.replace(np.nan, 'None', inplace=True)
        RESULT1.reset_index(inplace=True, drop=True)
        insert_warning_result(RESULT1, cefengta_id)
        RESULT2 = RESULT[RESULT['warnType'] == '可疑']
        RESULT2.replace(np.nan, 'None', inplace=True)
        RESULT2.reset_index(inplace=True, drop=True)
        insert_warning_result(RESULT2, cefengta_id)
        RESULT3 = RESULT[RESULT['warnType'] == '冰冻']
        RESULT3.replace(np.nan, 'None', inplace=True)
        RESULT3.reset_index(inplace=True, drop=True)
        insert_warning_result(RESULT3, cefengta_id)
    RESULT['warnType'] = '无效'
    RESULT.replace(np.nan, 'None', inplace=True)
    insert_warning_result(RESULT, cefengta_id)


def read_nrg(readpath, savepath, password, nrg_type, name_list):
    for i in name_list:
        date_filter = i.split('.')[0]
        save_path = savepath + '/'
        read_path = readpath + '/'
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        # 测风塔密码
        # password = '202102'
        if nrg_type == 'rwd':
            converter = nrgpy.local_rwd(rwd_dir=read_path, encryption_pin=password, out_dir=save_path, file_filter=date_filter)
        else:
            converter = nrgpy.local_rld(rld_dir=read_path, encryption_pass=password, out_dir=save_path, file_filter=date_filter)
        converter.convert()


def read_password(cefengta_ID):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT CODE FROM cefengta.static_information where ID='%s';" % cefengta_ID)
    password_nrg = cursor.fetchone()
    cursor.close()
    conn.close()
    return password_nrg[0]

def unzip_file(zip_path, savepath):
    if not os.path.exists(savepath):
        os.makedirs(savepath)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # 遍历ZIP文件中的所有文件条目
        for zip_info in zip_ref.infolist():
            # 获取文件名
            file_name = zip_info.filename
            # 单独提取每个文件到指定目录，不保留文件夹结构
            if not zip_info.is_dir():  # 确保它是一个文件
                dir_path_zip = os.path.join(savepath, os.path.basename(file_name))
                # 提取文件
                with zip_ref.open(zip_info) as zip_file:
                    with open(dir_path_zip, 'wb') as new_file:
                        new_file.write(zip_file.read())


def update_endtime(endtime, cefeng_name):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 插入数据
    sql_update = "UPDATE dynamic_information SET ENDTIME = '%s' WHERE ID='%s';" % (endtime, cefeng_name)
    cursor.execute(sql_update)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    # file_dir_path = Path('/home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/M005430')
    # python3 write_yuanshi_data.py /home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/M005430 0
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):

        file_dir_path = sys.argv[1]
        name_list = sys.argv[2]
        datatype = sys.argv[3]
        # start_time = sys.argv[4]
        # end_time = sys.argv[5]
        cefeng_name = Path(file_dir_path).name
        name_list = name_list.split(',')
        ##############################################################
        # start_time = (datetime.date.today() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        # end_time = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        savepath_zip = ''
        savepath_nrg = ''
        if name_list[0].split('.')[-1] in ['RWD', 'rwd', 'rld', 'RLD']:
            savepath_nrg = file_dir_path + '/nrg_online'
            if not os.path.exists(savepath_nrg):
                os.makedirs(savepath_nrg)
            password1 = read_password(cefeng_name)
            if (name_list[0].endswith('.RWD')) | (name_list[0].endswith('.rwd')):
                nrg_type = 'rwd'
            else:
                nrg_type = 'rld'
            read_nrg(file_dir_path, savepath_nrg, password1, nrg_type, name_list)
            if getattr(sys, 'frozen', False):  # 判断是exe还是.py程序
                dir = getattr(
                    sys, '_MEIPASS', os.path.dirname(
                        os.path.abspath(__file__)))
                qss_path = dir
            else:
                read = "./"
                for file in os.listdir(read):
                    if '\\' in file:
                        shutil.rmtree(read + file)
        elif name_list[0].split('.')[-1] in ['zip']:
            savepath_zip = file_dir_path + '/zip_online'
            for file_name_list in name_list:
                unzip_file(file_dir_path + '/' + file_name_list, savepath_zip)

        # 原始数据也要判断录入哪些
        # upload_file = str(file_dir_path)
        upload_file = 0
        # for file_name in name_list:
        #     if file_name.endswith('.zip'):
        #         file_dir_path = file_dir_path + '/zip'
        #         # upload_file = file_dir_path + '/' + file_name
        #         upload_file = upload_file + 1
        #     elif file_name.endswith('.rar'):
        #         file_dir_path = file_dir_path + '/rar'
        #         # upload_file = file_dir_path + '/' + file_name
        #         upload_file = upload_file + 1
        for file_name in os.listdir(file_dir_path):
            if file_name == 'nrg_online':
                file_dir_path = file_dir_path + '/nrg_online'
            elif file_name == 'zip_online':
                file_dir_path = file_dir_path + '/zip_online'
        list_dir = os.listdir(file_dir_path)
        file_type = list_dir[0].split('.')[-1]
        if file_type == 'json':
            file_type = list_dir[1].split('.')[-1]
        # 原始测风塔数据写入
        upload_file_wenjian = 0
        try:
            start_time = ''
            end_time = ''
            for file_name in os.listdir(file_dir_path):
                file_path = file_dir_path + '/' + file_name
                if not file_name.endswith('.json'):
                    if (file_type == 'csv') | (file_type == 'CSV'):
                        # csv data
                        if datatype == 0:
                            # 测风塔数据
                            data = pd.read_csv(file_path, skiprows=4, header=None)
                            with open(file_path) as f:
                                col = f.readlines()[1]
                            col = col.replace('"', '')
                            col = col.replace('\n', '')
                            data.columns = col.split(',')
                        else:
                            # 雷达数据
                            f = open(file_path, 'rb')
                            data = f.read()
                            with open(file_path, 'r', encoding=chardet.detect(data).get("encoding")) as f:
                                line = f.readline()
                            if 'Timestamp' in line:
                                data = pd.read_csv(file_path)
                            else:
                                data = pd.read_csv(file_path, skiprows=8, encoding='GB2312')
                    elif file_path.endswith('dat'):
                        data = pd.read_csv(file_path, skiprows=4, header=None)
                        with open(file_path) as f:
                            col = f.readlines()[1]
                        col = col.replace('"', '')
                        col = col.replace('\n', '')
                        data.columns = col.split(',')
                    elif file_path.endswith('wnd'):
                        data = pd.read_csv(file_path, skiprows=3, encoding='utf-8', sep=' ')
                        data['Date_Time'] = data.apply(lambda x: x['Date'] + ' ' + x['Time'], axis=1)
                        data.drop(columns=['Date', 'Time'], inplace=True)
                    else:
                        if datatype == 2:
                            data = pd.read_csv(file_path, encoding='GB2312', skiprows=9, sep='\t')
                        # NRG Systems SymphoniePRO Desktop Application rld
                        break_line = read_brakline(file_path)
                        data = pd.read_csv(file_path, skiprows=break_line, sep='\t')
                    # print(data)
                    # 以下是通用的
                    # 读取通道配置表
                    table_name = 'data_' + cefeng_name + '_yuanshi'
                    columns_yuanshi, columns_sql = read_columns(cefeng_name)
                    sql_list = []
                    yuan_list = []
                    for element in columns_sql:
                        positions = [index for index, value in enumerate(columns_sql) if value == element]
                        if columns_sql[positions[0]] not in sql_list:
                            sql_list.append(columns_sql[positions[0]])
                            yuan_list.append(columns_yuanshi[positions[0]])
                    data = data[yuan_list]
                    data.columns = sql_list
                    create_cefeng_table(table_name, sql_list)
                    data.reset_index(inplace=True, drop=True)
                    # # 整理时间格式
                    time_name = 'Date_Time'
                    time_format = test_format(data[time_name][0])
                    # 判断格式不满足的就更改
                    if time_format != '%Y-%m-%d %H:%M:%S':
                        data[time_name] = data[time_name].apply(
                            lambda x: datetime.datetime.strptime(x, time_format).strftime('%Y-%m-%d %H:%M:%S'))
                    write_data(data, table_name)
                    upload_file_wenjian = upload_file_wenjian + 1
                    if start_time == '':
                        start_time = np.nanmin(data[time_name])
                    else:
                        if start_time > np.nanmin(data[time_name]):
                            start_time = np.nanmin(data[time_name])
                    if end_time == '':
                        end_time = np.nanmax(data[time_name])
                    else:
                        if end_time < np.nanmax(data[time_name]):
                            end_time = np.nanmax(data[time_name])
            # start_time = start_time.split(' ')[0]
            # end_time = end_time.split(' ')[0]
            main_data_clean_rule(cefeng_name, start_time, end_time)
            update_endtime(end_time, cefeng_name)
            upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            state = '成功'
            if upload_file == 0:
                upload_file = upload_file_wenjian
            write_log(cefeng_name, str(upload_file), upload_time, state)
            if os.path.exists(savepath_nrg):
                shutil.rmtree(savepath_nrg)
            if os.path.exists(savepath_zip):
                shutil.rmtree(savepath_zip)
            # # warning_data 读取前几天数据预警 write_log 数据入库日志
            subprocess.run('./warning_data %s %s %s' % (cefeng_name, start_time, end_time), shell=True)
        except:
            upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            state = '失败'
            if upload_file == 0:
                upload_file = upload_file_wenjian
            write_log(cefeng_name, str(upload_file), upload_time, state)
            if os.path.exists(savepath_nrg):
                shutil.rmtree(savepath_nrg)
            if os.path.exists(savepath_zip):
                shutil.rmtree(savepath_zip)
