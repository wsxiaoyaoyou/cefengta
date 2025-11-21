import pandas as pd
import numpy as np
import pymysql
import sys
import datetime
import subprocess
host = 'localhost'
port = 3306
user = 'wyh' #用户名
password = 'Wyh123!@#' # 密码
database = 'cefengta'


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


# SN01	风速波动合理性范围	1小时内平均风速变化≥0.001m/s,且1小时内平均风速变化<6m/s；
# 风速波动异常：1小时内平均风速变化<0.001m/s,或1小时内平均风速变化≥6m/s；
def warn_SN01(Data, wind_name, th1=0.001, th2=6.0):
    Data[wind_name].fillna(np.nan, inplace=True)
    Data[wind_name] = Data[wind_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    index_list = []
    if len(Data) > 6:
        for i in range(0, len(Data)-5):
            cal_data = Data.loc[i:i+5, wind_name]
            if (np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 3) < th1) | (np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 3) >= th2):
                index_list = index_list + list(range(i, i + 6))
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = wind_name + '-' + 'SN01'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN02	风速数值合理性范围	平均风速≥0m/s，且平均风速≤50m/s；
# 风速大小异常：平均风速<0m/s，或平均风速＞50m/s；
def warn_SN02(Data, wind_name, th1=0.0, th2=50.0):
    Data[wind_name].fillna(np.nan, inplace=True)
    Data[wind_name] = Data[wind_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    # Data[wind_name] = Data[wind_name].apply(lambda x: np.nan if (x < th1) | (x > th2) else x)
    index_list = np.where((Data[wind_name] < th1) | (Data[wind_name] > th2))[0]
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = wind_name + '-' + 'SN02'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN03	风速标准差合理性范围	风速标准差≥0m/s，且风速标准差≤5m/s；
# 风速标准差异常：风速标准差<0m/s，或风速标准差＞5m/s；
def warn_SN03(Data, wind_SD_name, th1=0.0, th2=5.0):
    Data[wind_SD_name].fillna(np.nan, inplace=True)
    Data[wind_SD_name] = Data[wind_SD_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    # Data['diff'] = Data[wind_SD_name].diff()
    # Data['diff'] = Data['diff'].apply(lambda x: abs(x))
    # Data[wind_SD_name] = Data[wind_SD_name].apply(lambda x: np.nan if (x < th1) | (x > th2) else x)
    index_list = np.where((Data[wind_SD_name] < th1) | (Data[wind_SD_name] > th2))[0]
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = wind_SD_name + '-' + 'SN03'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN04	风向波动合理性范围	1小时内平均风向变化≥0.001°；
# 风向波动异常：1小时内平均风向变化<0.001°；
def warn_SN04(Data, dir_name, th1=0.001):
    Data[dir_name].fillna(np.nan, inplace=True)
    Data[dir_name] = Data[dir_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    index_list = []
    if len(Data) > 6:
        for i in range(0, len(Data)-5):
            cal_data = Data.loc[i:i + 5, dir_name]
            if np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 1) < th1:
                index_list = index_list + list(range(i, i + 6))
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = dir_name + '-' + 'SN04'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN05	风向数值合理性范围	平均风向≥0°，且平均风向≤360°；
# 风向大小异常：平均风向<0°，或平均风向＞360°；
def warn_SN05(Data, dir_name, th1=0.0, th2=360.0):
    Data[dir_name].fillna(np.nan, inplace=True)
    Data[dir_name] = Data[dir_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    # Data[dir_name] = Data[dir_name].apply(lambda x: np.nan if (x < th1) | (x > th2) else x)
    index_list = np.where((Data[dir_name] < th1) | (Data[dir_name] > th2))[0]
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = dir_name + '-' + 'SN05'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN06	气温波动合理性范围	1小时内平均气温变化<5°C；
# 气温波动异常：1小时内平均气温变化≥5°C；
def warn_SN06(Data, tem_name, th1=5.0):
    Data[tem_name].fillna(np.nan, inplace=True)
    Data[tem_name] = Data[tem_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    index_list = []
    if len(Data) > 6:
        for i in range(0, len(Data)-5):
            cal_data = Data.loc[i:i+5, tem_name]
            if np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 1) >= th1:
                index_list = index_list + list(range(i, i+6))
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = tem_name + '-' + 'SN06'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN07	气温数值合理性范围	平均气温≥-40°C，且平均气温≤50°C
# 气温大小异常：平均气温<-40°C，或平均气温＞>50°C
def warn_SN07(Data, tem_name, th1=-40.0, th2=50.0):
    Data[tem_name].fillna(np.nan, inplace=True)
    Data[tem_name] = Data[tem_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    # Data[tem_name] = Data[tem_name].apply(lambda x: np.nan if (x < th1) | (x > th2) else x)
    index_list = np.where((Data[tem_name] < th1) | (Data[tem_name] > th2))[0]
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = tem_name + '-' + 'SN07'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN08	气压波动合理性范围	3小时内平均气压变化<1kPA；
# 气压波动异常：3小时内平均气压变化≥1kPA；
def warn_SN08(Data, p_name, th1=1.0):
    # 注意单位问题
    Data[p_name].fillna(np.nan, inplace=True)
    Data[p_name] = Data[p_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    index_list = []
    if len(Data) > 18:
        for i in range(0, len(Data)-17):
            cal_data = Data.loc[i:i + 17, p_name]
            if np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 1) >= th1*1000:
                index_list = index_list + list(range(i, i+18))
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = p_name + '-' + 'SN08'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN09	气压数值合理性范围	平均气压≥50kPA，且平均气压≤110kPA；
# 气压大小异常：平均气压<50kPA，或平均气压＞110kPA；
def warn_SN09(Data, p_name, th1=50.0, th2=110.0):
    # 注意单位问题
    Data[p_name].fillna(np.nan, inplace=True)
    Data[p_name] = Data[p_name].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    # Data[p_name] = Data[p_name].apply(lambda x: np.nan if (x < th1*1000) | (x > th2*1000) else x)
    index_list = np.where((Data[p_name] < th1*1000) | (Data[p_name] > th2*1000))[0]
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = p_name + '-' + 'SN09'
    else:
        result_time = pd.DataFrame()
    return result_time


# SN10	风速相关性合理范围	50m高平均风速与30米高平均风速差值<2m/s，50m高平均风速与10米高平均风速差值<4m/s；
# 风速相关性异常：50m高平均风速与30米高平均风速差值≥2m/s，或50m高平均风速与10米高平均风速差值≥4m/s；
def warn_SN10(Data, wind_name_10, wind_name_30, wind_name_50, th1=2.0, th2=4.0):
    Data[wind_name_10].fillna(np.nan, inplace=True)
    Data[wind_name_10] = Data[wind_name_10].replace('None', np.nan).astype('float')
    Data[wind_name_30].fillna(np.nan, inplace=True)
    Data[wind_name_30] = Data[wind_name_30].replace('None', np.nan).astype('float')
    Data[wind_name_50].fillna(np.nan, inplace=True)
    Data[wind_name_50] = Data[wind_name_50].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    Data['diff'] = Data[wind_name_50] - Data[wind_name_30]
    # Data.loc[Data['diff'] >= th1, [wind_name_30, wind_name_50]] = np.nan
    index_list1 = np.where(Data['diff'] > th1)[0]
    index_list1 = list(set(index_list1))
    if len(index_list1) > 0:
        result_time1 = find_discontinuities(index_list1)
        result_time1['start'] = result_time1['start'].apply(lambda x: Data['Date_Time'][x])
        result_time1['end'] = result_time1['end'].apply(lambda x: Data['Date_Time'][x])
        result_time1['can_name'] = wind_name_30 + '_' + wind_name_50 + '-' + 'SN10'
    else:
        result_time1 = pd.DataFrame()
    Data['diff'] = Data[wind_name_50] - Data[wind_name_10]
    # Data.loc[Data['diff'] >= th2, [wind_name_10, wind_name_50]] = np.nan
    Data.drop(columns='diff', inplace=True)
    index_list2 = np.where(Data['diff'] > th2)[0]
    index_list2 = list(set(index_list2))
    if len(index_list2) > 0:
        result_time2 = find_discontinuities(index_list2)
        result_time2['start'] = result_time2['start'].apply(lambda x: Data['Date_Time'][x])
        result_time2['end'] = result_time2['end'].apply(lambda x: Data['Date_Time'][x])
        result_time2['can_name'] = wind_name_10 + '_' + wind_name_50 + '-' + 'SN10'
    else:
        result_time2 = pd.DataFrame()
    result_time = pd.concat([result_time1, result_time2])
    return result_time


# SN11	风向相关性合理范围	50m高平均风向与30m高平均风向差值<22.5°；
# 风向相关性异常：50m高平均风向与30m高平均风向差值≥22.5°。
def warn_SN11(Data, dir_name_30, dir_name_50, th1=22.5):
    Data[dir_name_30].fillna(np.nan, inplace=True)
    Data[dir_name_30] = Data[dir_name_30].replace('None', np.nan).astype('float')
    Data[dir_name_50].fillna(np.nan, inplace=True)
    Data[dir_name_50] = Data[dir_name_50].replace('None', np.nan).astype('float')
    Data.reset_index(inplace=True, drop=True)
    Data['diff'] = Data[dir_name_50] - Data[dir_name_30]
    Data.loc[Data['diff'] >= th1, [dir_name_30, dir_name_50]] = np.nan
    index_list = np.where(Data['diff'] > th1)[0]
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = dir_name_30 + '_' + dir_name_50 + '-' + 'SN011'
    else:
        result_time = pd.DataFrame()
    Data.drop(columns='diff', inplace=True)
    return result_time


#SN13，数据缺测：数据出现空值
def warn_SN12(cefengta_id, end_time):
    start_time = (datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cefengta.data_%s_yuanshi where Date_Time >= '%s' and Date_Time <='%s';" % (
    cefengta_id, start_time, end_time))
    # # 获取查询结果
    # 获取表头
    col_name_list1 = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list1
    if len(data) > 0:
        result_time = pd.DataFrame()
    else:
        result_time = pd.DataFrame()
        result_time['start'] = start_time
        result_time['end'] = end_time
        result_time['can_name'] = '3内天未收到测风数据' + '-' + 'SN14'
    return result_time
# SN13，数据缺测：数据出现空值
def warn_SN13(Data):
    result_time = pd.DataFrame()
    for col in Data.columns:
        na_in_col = Data[Data[col].isna()].index.tolist()
        result_time2 = find_discontinuities(na_in_col)
        result_time2['start'] = result_time2['start'].apply(lambda x: Data['Date_Time'][x])
        result_time2['end'] = result_time2['end'].apply(lambda x: Data['Date_Time'][x])
        result_time2['can_name'] = col + '-' + 'SN13'
        result_time = pd.concat([result_time, result_time2])
    return result_time

# SN14，发生冰冻：12小时内平均气温<4°C，且平均风速变化<0.001m/s
def warn_SN14(Data, wind_name, tem_name):
    Data[wind_name].fillna(np.nan, inplace=True)
    Data[wind_name] = Data[wind_name].astype('float')
    Data[tem_name].fillna(np.nan, inplace=True)
    Data[tem_name] = Data[tem_name].astype('float')
    Data.reset_index(inplace=True, drop=True)
    index_list = []
    if len(Data) > 72:
        for i in range(0, len(Data) - 71):
            cal_data = Data.loc[i:i + 71, wind_name]
            cal_data2 = Data.loc[i:i + 71, tem_name]
            if (np.around(np.nanmax(cal_data) - np.nanmin(cal_data), 3) < 0.001) & (
                    np.around(np.nanmean(cal_data2), 3) < 4):
                index_list = index_list + list(range(i, i + 72))
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = wind_name + '-' + 'SN14'
    else:
        result_time = pd.DataFrame()
    return result_time

# SN15，电压异常：电池<250V或大于350V
def warn_SN15(Data, V_col):
    Data[V_col].fillna(np.nan, inplace=True)
    Data[V_col] = Data[V_col].astype('float')
    Data.reset_index(inplace=True, drop=True)
    index_list = np.where((Data[V_col] < 250) | (Data[V_col] > 350))[0]
    index_list = list(set(index_list))
    if len(index_list) > 0:
        result_time = find_discontinuities(index_list)
        result_time['start'] = result_time['start'].apply(lambda x: Data['Date_Time'][x])
        result_time['end'] = result_time['end'].apply(lambda x: Data['Date_Time'][x])
        result_time['can_name'] = V_col+ '-' + 'SN15'
    else:
        result_time = pd.DataFrame()
    return result_time


def insert_data_warn(RESULT, cefengta_id):
    # # 链接数据库
    table_name = 'alarm_information'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断是否存在清洗后数据表，不存在就创建
    if not cursor.execute("SHOW TABLES LIKE '%s';" % table_name):
        creat_table = "CREATE TABLE alarm_information (warnID INT NOT NULL AUTO_INCREMENT," \
                      "ID VARCHAR(45) NULL,NAME VARCHAR(45) NULL,channelID VARCHAR(45) NULL,warnTime VARCHAR(45) NULL,warnType VARCHAR(45) NULL,status VARCHAR(45) NULL, PRIMARY KEY (warnID));"
        cursor.execute(creat_table)
    select_sql = "SELECT NAME FROM static_information where ID = '%s';" % (cefengta_id)
    cursor.execute(select_sql)
    cefengtaname = cursor.fetchone()[0]
    # 插入数据
    num = 0
    for i in range(len(RESULT)):
        part_1 = "ID,NAME,channelID,warnTime,warnType,status"
        part_2 = "'" + cefengta_id + "','" + cefengtaname + "','" + RESULT['can_name'][i].split('-')[0] + "','" + RESULT['start'][i] + '-' + RESULT['end'][i] + "','" + RESULT['warnType'][i] + "','" + '告警中' + "'"
        sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % (table_name, part_1, part_2.replace("'None'", "NULL"))
        cursor.execute(sql_insert)
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




def main(cefengta_id, start_time, end_time):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cefengta.data_%s_yuanshi where Date_Time >= '%s' and Date_Time <='%s';" % (cefengta_id, start_time, end_time))
    # # 获取查询结果
    # 获取表头
    col_name_list1 = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list1
    # SN12，数据未收到：3内天未收到测风数据
    result_time_12 = warn_SN12(cefengta_id, end_time)
    result_time_12['warnType'] = '数据缺测'
    insert_data_warn(result_time_12, cefengta_id)
    if len(data) > 0:

        # 查询通道配置表，查看压强单位，然后规范化到Pa
        cursor.execute(
            "SELECT UNIT, USEDCHANNEL FROM cefengta.channel_configuration where ID='%s' and USEDCHANNEL like '%%_P_AVG';" % cefengta_id)
        p_value = cursor.fetchall()
        # 根据风机编号筛选规则
        # 查询静态信息表static_information，查询清洗规则warnTempID，然后匹配alarm_template ID,查询RULES_ID, 对应查看alarm_rules, ID,读取SUBJECTION,返回THRESHOLD1,THRESHOLD2
        sql_select_RULES_ID = "select t2.TULES_ID from cefengta.static_information as t1 inner join cefengta.alarm_template as t2 on t1.warnTempID = t2.ID where t1.ID='%s';" % cefengta_id
        cursor.execute(sql_select_RULES_ID)
        RULES_ID = cursor.fetchone()
        # 根据读取的规则表判断需要哪些条件
        # 读取规则表 查看需要哪些清洗规则
        cursor.execute("SELECT * FROM cefengta.alarm_rules;")
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
        V_list = []
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
            elif 'V_AVG' in i:
                V_list.append(i)
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
                else:
                    p_unit = 1
                data[p_col] = data[p_col].replace('None', np.nan).replace('None', np.nan).astype('float') * p_unit
                if p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MIN' in data.columns:
                    data[p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MIN'] = data[p_col.split('_')[0] + '_' +
                                                                                               p_col.split('_')[
                                                                                                   1] + '_' + 'MIN'].replace('None', np.nan).astype('float') * p_unit
                if p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MAX' in data.columns:
                    data[p_col.split('_')[0] + '_' + p_col.split('_')[1] + '_' + 'MAX'] = data[p_col.split('_')[0] + '_' +
                                                                                               p_col.split('_')[
                                                                                                   1] + '_' + 'MAX'].replace('None', np.nan).astype('float') * p_unit
        # 这里在告警数据
        RESULT = pd.DataFrame()
        if RULES_ID != None:
            if len(RULES_ID[0]) > 0:
                for rule in RULES_ID[0].split(','):
                    if rule not in ['SN12', 'SN13', 'SN14', 'SN15']:
                        data_sub = data_rules[data_rules['ID'] == rule]['SUBJECTION'].values[0]
                        THRESHOLD1 = float(data_rules[data_rules['ID'] == rule]['THRESHOLD1'].values[0])
                        if data_rules[data_rules['ID'] == rule]['THRESHOLD2'].values[0] != '':
                            THRESHOLD2 = float(data_rules[data_rules['ID'] == rule]['THRESHOLD2'].values[0])
                        # SN01	风速波动合理性范围	1小时内平均风速变化≥0.001m/s,且1小时内平均风速变化<6m/s；
                        # 风速波动异常：1小时内平均风速变化<0.001m/s,或1小时内平均风速变化≥6m/s；
                        if (data_sub == 'SN01') & (WS_list != []):
                            for WS_col in WS_list:
                                result_time = warn_SN01(data, WS_col, th1=THRESHOLD1, th2=THRESHOLD2)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])

                        # SN02	风速数值合理性范围	平均风速≥0m/s，且平均风速≤50m/s；
                        # 风速大小异常：平均风速<0m/s，或平均风速＞50m/s；
                        elif (data_sub == 'SN02') & (WS_list != []):
                            for WS_col in WS_list:
                                result_time = warn_SN02(data, WS_col, th1=THRESHOLD1, th2=THRESHOLD2)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
                        # SN03	风速标准差合理性范围	风速标准差≥0m/s，且风速标准差≤5m/s；
                        # 风速标准差异常：风速标准差<0m/s，或风速标准差＞5m/s；
                        elif (data_sub == 'SN03') & (WS_SD_list != []):
                            for WS_SD_col in WS_SD_list:
                                result_time = warn_SN03(data, WS_SD_col, th1=THRESHOLD1, th2=THRESHOLD2)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
                        # SN04	风向波动合理性范围	1小时内平均风向变化≥0.001°；
                        # 风向波动异常：1小时内平均风向变化<0.001°；
                        elif (data_sub == 'SN04') & (WD_list != []):
                            for WD_col in WD_list:
                                result_time = warn_SN04(data, WD_col, th1=THRESHOLD1)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
                        # SN05	风向数值合理性范围	平均风向≥0°，且平均风向≤360°；
                        # 风向大小异常：平均风向<0°，或平均风向＞360°；
                        elif (data_sub == 'SN05') & (WD_list != []):
                            for WD_col in WD_list:
                                result_time = warn_SN05(data, WD_col, th1=THRESHOLD1, th2=THRESHOLD2)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
                        # SN06	气温波动合理性范围	1小时内平均气温变化<5°C；
                        #  气温波动异常：1小时内平均气温变化≥5°C；
                        elif (data_sub == 'SN06') & (T_list != []):
                            for T_col in T_list:
                                result_time = warn_SN06(data, T_col, th1=THRESHOLD1)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
                        # SN07	气温数值合理性范围	平均气温≥-40°C，且平均气温≤50°C
                        # 气温大小异常：平均气温<-40°C，或平均气温＞>50°C
                        elif (data_sub == 'SN07') & (T_list != []):
                            for T_col in T_list:
                                result_time = warn_SN07(data, T_col, th1=THRESHOLD1, th2=THRESHOLD2)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
                        # SN08	气压波动合理性范围	3小时内平均气压变化<1kPA；
                        # 气压波动异常：3小时内平均气压变化≥1kPA；
                        elif (data_sub == 'SN08') & (P_list != []):
                            for P_col in P_list:
                                result_time = warn_SN08(data, P_col, th1=THRESHOLD1)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
                        # SN09	气压数值合理性范围	平均气压≥50kPA，且平均气压≤110kPA；
                        # 气压大小异常：平均气压<50kPA，或平均气压＞110kPA；
                        elif (data_sub == 'SN09') & (P_list != []):
                            for P_col in P_list:
                                result_time = warn_SN09(data, P_col, th1=THRESHOLD1, th2=THRESHOLD2)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
                        # SN10	风速相关性合理范围	50m高平均风速与30米高平均风速差值<2m/s，50m高平均风速与10米高平均风速差值<4m/s；
                        # 风速相关性异常：50m高平均风速与30米高平均风速差值≥2m/s，或50m高平均风速与10米高平均风速差值≥4m/s；
                        elif (data_sub == 'SN10') & ('10_WS_AVG' in WS_list) & ('30_WS_AVG' in WS_list) & ('50_WS_AVG' in WS_list):
                            result_time = warn_SN10(data, '10_WS_AVG', '30_WS_AVG', '50_WS_AVG', th1=THRESHOLD1, th2=THRESHOLD2)
                            result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                            RESULT = pd.concat([RESULT, result_time])
                        # SN11	风向相关性合理范围	50m高平均风向与30m高平均风向差值<22.5°；
                        # 风向相关性异常：50m高平均风向与30m高平均风向差值≥22.5°。
                        elif (data_sub == 'SN11') & ('30_WD_AVG' in WS_list) & ('50_WD_AVG' in WS_list):
                            result_time = warn_SN11(data, '30_WD_AVG', '50_WD_AVG', th1=THRESHOLD1)
                            result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                            RESULT = pd.concat([RESULT, result_time])
                    else:

                        # SN13，数据缺测：数据出现空值；
                        if rule == 'SN13':
                            result_time = warn_SN13(data)
                            result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                            RESULT = pd.concat([RESULT, result_time])

                        # SN14，发生冰冻：12小时内平均气温<4°C，且平均风速变化<0.001m/s
                        elif (rule == 'SN14') & (T_list != [])& (WS_list != []):
                            for T_col in T_list:
                                for WS_col in WS_list:
                                    result_time = warn_SN14(data, WS_col, T_col)
                                    result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                    RESULT = pd.concat([RESULT, result_time])
                        # SN15，电压异常：电池<250V或大于350V
                        elif (rule == 'SN15') & (V_list != []):
                            for V_col in V_list:
                                result_time = warn_SN15(data, V_col)
                                result_time['warnType'] = data_rules[data_rules['ID'] == rule]['NAME'].values[0]
                                RESULT = pd.concat([RESULT, result_time])
        # 查看需要插入哪些数据 警告信息 alarm_information
        RESULT.reset_index(inplace=True, drop=True)
        RESULT.replace(np.nan, 'None', inplace=True)
        insert_data_warn(RESULT, cefengta_id)





if __name__ == '__main__':
    import warnings
    warnings.filterwarnings("ignore")
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        start_time = sys.argv[2]
        end_time = sys.argv[3]
        # cefengta_id = 'M003470'
        # start_time = '2023-06-20 23:50:00'
        # end_time = '2023-07-20 23:50:00'
        main(cefengta_id, start_time, end_time)
