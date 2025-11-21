import pandas as pd
import numpy as np
import pymysql
import sys
import subprocess
host = 'localhost'
port = 3306
user = 'root' #用户名
password = '123456' # 密码
database = 'cefengta'


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

def create_cefeng_table(table_name, read_data):
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
        create_cefeng_table(table_name, data)

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
        if len(name_split) >2:
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
        # print(sql_insert)
        cursor.execute(sql_insert)
        # print(sql_insert.replace("'None'", "NULL"))
        cursor.execute(sql_insert.replace("'None'", "NULL"))
        num += 1
        if num > 1000:
            conn.commit()
            num = 0
    if num < 1000:
        conn.commit()
    # if len(RESULT) > 0:
    #     sql_update = "UPDATE static_information set %s='%s' where ID = '%s';" % ('STATUS', '3', cefengta_id)
    #     cursor.execute(sql_update)
    #     conn.commit()
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

def data_clean_rule(cefengta_id):
    table_name = 'data_' + cefengta_id + '_clean'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cefengta.data_%s_yuanshi;" % cefengta_id)
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
                                                                                               1] + '_' + 'MIN'].replace('None', np.nan).astype('float') * p_unit
            if p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MAX' in data.columns:
                data[p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MAX'] = data[p_col.split('_')[0] + '_' +
                                                                                           p_col.split('_')[
                                                                                               1] + '_' + 'MAX'].replace('None', np.nan).astype('float') * p_unit
    # 这里在清洗数据
    RESULT = pd.DataFrame(columns=['rules_ID','start','end','can_name','warnType'])
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
                        if rule not in ['JN01', 'JN02','JN03','JN04','JN05','JN06','JN07','JN08','JN09','JN10','JN11','JN12']:
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
                elif (data_sub == 'JN10') & ('10_WS_AVG' in WS_list) & ('30_WS_AVG' in WS_list) & ('50_WS_AVG' in WS_list):
                    data, result_time, result_na = rule_JN10(data, '10_WS_AVG', '30_WS_AVG', '50_WS_AVG', th1=THRESHOLD1, th2=THRESHOLD2)
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
                            data, result_time, result_na = rule_JN12(data, WS_col, T_col, th1=THRESHOLD1, th2=THRESHOLD2)
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


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings("ignore")
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        data_clean_rule(cefengta_id)
