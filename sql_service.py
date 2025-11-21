import pandas as pd
import pymysql
import numpy as np
import datetime
import json
import os
import shutil
import sys
import math
from scipy.optimize import curve_fit
import warnings
warnings.filterwarnings("ignore")

host = 'localhost'
port = 3306
user = 'root' #用户名
password = '123456' # 密码
database = 'cefengta' #数据库名称



def add_user(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断有没有用户表，没有的话要新建
    if not cursor.execute("SHOW TABLES LIKE 'user_information';"):
        creat_table = "CREATE TABLE user_information (username VARCHAR(45) NOT NULL," \
                      "password VARCHAR(45) NULL,realName VARCHAR(45) NULL,userType INT NULL,mobile VARCHAR(45) NULL," \
                      "email VARCHAR(45) NULL,remarks VARCHAR(45) NULL,organize VARCHAR(45) NULL, token VARCHAR(45) NULL,PRIMARY KEY (username));"
        cursor.execute(creat_table)
        conn.commit()
    # 插入数据
    cursor.execute("SELECT username FROM user_information where username='%s';" % data['username'])
    p_value = cursor.fetchall()
    if len(p_value) == 0:
        part_1 = ""
        part_2 = ""
        for key_name in data.keys():
            if len(part_1) != 0:
                part_1 += "," + key_name
                if isinstance(data[key_name], str):
                    part_2 += ",'" + data[key_name] + "'"
                else:
                    part_2 += ",'" + str(data[key_name]) + "'"
            else:
                part_1 += key_name
                if isinstance(data[key_name], str):
                    part_2 += "'" + data[key_name] + "'"
                else:
                    part_2 += "'" + str(data[key_name]) + "'"
        sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('user_information', part_1, part_2)
        cursor.execute(sql_insert)
        conn.commit()
        insert_type = 0
    else:
        insert_type = 1
    cursor.close()
    conn.close()
    return insert_type

def edit_user(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 插入数据
    part_1 = ""
    part_2 = ""
    for key_name in data.keys():
        if len(part_1) != 0:
            part_1 += "," + key_name
            if isinstance(data[key_name], str):
                part_2 += ",'" + data[key_name] + "'"
            else:
                part_2 += ",'" + str(data[key_name]) + "'"
        else:
            part_1 += key_name
            if isinstance(data[key_name], str):
                part_2 += "'" + data[key_name] + "'"
            else:
                part_2 += "'" + str(data[key_name]) + "'"
    sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % ('user_information', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    insert_type = 0
    cursor.close()
    conn.close()
    return insert_type

def delete_user(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 删除数据
    for names in data['username'].split(','):
        sql_delete = "DELETE FROM user_information WHERE username='%s';" % names
        cursor.execute(sql_delete)
        conn.commit()
    cursor.close()
    conn.close()

def userinfo(data):
    result = pd.DataFrame()
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()

    sql_select = "SELECT * FROM user_information WHERE token='%s';" % data
    cursor.execute(sql_select)

    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values)!=0:
        result = pd.DataFrame(values)
        result.columns = col_name_list
        cursor.close()
        conn.close()
        return 1,result
    else:
        cursor.close()
        conn.close()
        return 2, "null"

def select_user(data):
    result = pd.DataFrame()
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 删除数据
    cursor.execute("SELECT username FROM user_information where username='%s';" % data['username'])
    p_value_usename = cursor.fetchone()
    if p_value_usename == None:
        typeid = 1
    else:
        sql_select = "SELECT password FROM user_information WHERE username='%s';" % data['username']
        cursor.execute(sql_select)
        p_value = cursor.fetchone()
        if p_value[0] == data['password']:
            typeid = 0
        else:
            typeid = 2
    if typeid == 0:
        sql_select = "SELECT * FROM user_information WHERE username='%s';" % data['username']
        cursor.execute(sql_select)
        col_name_list = [tuple[0] for tuple in cursor.description]
        values = cursor.fetchall()
        result = pd.DataFrame(values)
        result.columns = col_name_list
    cursor.close()
    conn.close()
    if typeid == 0:
        return typeid, result
    else:
        return typeid, result

def showUserPage(data):
    if ('username' in data.keys()) & ('realName' in data.keys()):
        select_data = "SELECT * FROM user_information where username like '%%%s%%' and realName like '%%%s%%';" % (data['username'], data['realName'])
    elif 'username' in data.keys():
        select_data = "SELECT * FROM user_information where username like '%%%s%%';" % (data['username'])
    elif 'realName' in data.keys():
        select_data = "SELECT * FROM user_information where username like '%%%s%%';" % (data['realName'])
    else:
        select_data = "SELECT * FROM user_information ;"
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records", force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

def channel_save(data):
    result = {}
    for i in range(len(data)):
        result[str(i)] = data[i]
    data = read_splic_channel(data)
    data = json.loads(data.to_json(orient="records", force_ascii=False))
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断有没有用户表,没有的话要新建
    for i in range(len(data)):
        cursor.execute("SELECT ID FROM channel_configuration where ID='%s' and ORIGINCHANNEL='%s';" % (data[i]["ID"], data[i]["ORIGINCHANNEL"]))
        p_value = cursor.fetchall()
        if len(p_value) == 0:
            part_1 = ""
            part_2 = ""
            for key_name in data[i].keys():
                if len(part_1) != 0:
                    part_1 += "," + key_name
                    if isinstance(data[i][key_name], str):
                        part_2 += ",'" + data[i][key_name] + "'"
                    else:
                        part_2 += ",'" + str(data[i][key_name]) + "'"
                else:
                    part_1 += key_name
                    if isinstance(data[i][key_name], str):
                        part_2 += "'" + data[i][key_name] + "'"
                    else:
                        part_2 += "'" + str(data[i][key_name]) + "'"
            sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('channel_configuration', part_1, part_2)
            cursor.execute(sql_insert)
            conn.commit()
            insert_type = 0
        else:
            insert_type = 1

    cursor.close()
    conn.close()
    return insert_type

def read_splic_channel(data):
    data = pd.DataFrame(data)
    data.reset_index(inplace=True, drop=True)
    data = data.fillna(np.nan)
    channel_result = pd.DataFrame()
    if 'ORIGINCHANNEL_use' not in data.columns:
        channel_result['ORIGINCHANNEL'] = data['ORIGINCHANNEL']
    else:
        channel_result['ORIGINCHANNEL'] = data['ORIGINCHANNEL_use']
    channel_result['CHID'] = data['CHID']
    channel_result['ID'] = data['ID']
    channel_result['HIGHT'] = data['HIGHT']
    channel_result['OFF'] = data['OFF'].apply(lambda x:float(x))
    channel_result['UNIT'] = data['UNIT']
    channel_result['SCALE'] = data['SCALE'].apply(lambda x:float(x))
    channel_result['UNIT'] = channel_result.apply(lambda x: cal_unit(x['UNIT']), axis=1)
    data['UNIT'] = data.apply(lambda x: cal_unit(x['UNIT']), axis=1)
    channel_result['USEDCHANNEL'] = data.apply(lambda x:cal_name_USEDCHANNEL(x['HIGHT'], x['UNIT'], x['type'], x['CHANNELNAMETYPE']), axis=1)
    channel_result['CHANNELNAME'] = channel_result.apply(lambda x: cal_name_CHANNELNAME(x['USEDCHANNEL']), axis=1)
    key = (channel_result['ORIGINCHANNEL'] == 'Date & Time Stamp') | (channel_result['ORIGINCHANNEL'] == 'Timestamp') | \
          (channel_result['ORIGINCHANNEL'] == 'TIMESTAMP') | (channel_result['ORIGINCHANNEL'] == 'Date_Time') | (channel_result['ORIGINCHANNEL'] == '时间戳')
    channel_result.loc[key, 'CHANNELNAME'] = '时间'
    channel_result.loc[key, 'USEDCHANNEL'] = 'Date_Time'
    # channel_result['USEDCHANNEL'] = channel_result.apply(lambda x:cal_name_USEDCHANNEL_nan(x['ORIGINCHANNEL'], x['USEDCHANNEL']), axis=1)
    return channel_result

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

def cal_name_USEDCHANNEL(height, unit, type, CHANNELNAMETYPE):
    if unit == 'm/s':
        can = CHANNELNAMETYPE
    elif unit == '°':
        can = 'WD'
    elif unit == '℃':
        can = 'T'
    elif (unit == 'kPa') | (unit == 'hPa') | (unit == 'mb') | (unit == 'mmHg')| (unit == 'Pa'):
        can = 'P'
    elif unit == 'V':
        can = 'V'
    elif unit == '%':
        if type != 'other':
            can = 'RH'
        else:
            can = 'REL'
    else:
        can = ''
    if can != '':
        return str(height) + '_' + can + '_' + type
    else:
        return ''

def cal_name_USEDCHANNEL_1(height, unit, type, CHANNELNAMETYPE):
    if unit == 'ms':
        can = CHANNELNAMETYPE
    elif unit == 'angle':
        can = 'WD'
    elif unit == 'temp':
        can = 'T'
    elif (unit == 'kpa') | (unit == 'hPa') | (unit == 'mb') | (unit == 'mmHg')| (unit == 'Pa'):
        can = 'P'
    elif unit == 'v':
        can = 'V'
    elif (unit == 'rh') | (unit == 'quality'):
        if type != 'other':
            can = 'RH'
        else:
            can = 'REL'
    else:
        can = ''
    if can != '':
        return str(height) + '_' + can + '_' + type
    else:
        return ''

def cal_unit(unit):
    if unit == 'ms':
        can = 'm/s'
    elif unit == 'angle':
        can = '°'
    elif unit == 'temp':
        can = '℃'
    elif (unit == 'kpa'):
        can = 'kPa'
    elif (unit == 'hPa'):
        can = 'hPa'
    elif (unit == 'mb'):
        can = 'mb'
    elif (unit == 'mmHg'):
        can = 'mmHg'
    elif unit == 'v':
        can = 'V'
    elif (unit == 'rh') | (unit == 'quality'):
        can = '%'
    else:
        can = ''
    if can != '':
        return can
    else:
        return ''

def channel_get(data):
    select_data = "SELECT * FROM channel_configuration where ID='%s';" % data['ID']
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        items = result.to_json(orient="records", force_ascii=False)
        items = json.loads(items)
    else:
        items = []
    return items

def cal_unit_show(unit, type):
    if unit == 'm/s':
        can = 'ms'
    elif unit == '°':
        can = 'angle'
    elif unit == '℃':
        can = 'temp'
    elif unit == 'kPa':
        can = 'kpa'
    elif unit == 'hPa':
        can = 'hPa'
    elif unit == 'mb':
        can = 'mb'
    elif unit == 'mmHg':
        can = 'mmHg'
    elif unit == 'Pa':
        can = 'Pa'
    elif unit == 'V':
        can = 'v'
    elif unit == '%':
        if type != 'other':
            can = 'rh'
        else:
            can = 'quality'
    # elif unit == 'quality':
    #     can = '%'
    else:
        can = ''
    if can != '':
        return can
    else:
        return ''

def show_map(params):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database='cefengta', charset='utf8mb4')
    cursor = conn.cursor()
    if params['userType'] == "1":
        select_data = "select t1.ID, t1.NAME, t1.LON, t1.LAT, t1.PROJECT, t1.STATUS, t2.STARTTIME, t2.ENDTIME, t2.SHEARY from cefengta.static_information as t1 " \
                      "inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID;"
    else:
        select_data = "select t1.ID, t1.NAME, t1.LON, t1.LAT, t1.PROJECT, t1.STATUS, t2.STARTTIME, t2.ENDTIME, t2.SHEARY from cefengta.static_information as t1 " \
                      "inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID WHERE t1.organize_uuid = '%s';" % params['organize']

    cursor.execute(select_data)
    values = cursor.fetchall()

    if len(values) > 0:
        result = pd.DataFrame(values)
        result.columns = ['ID', 'NAME', 'LON', 'LAT', 'PROJECT', 'STATUS', 'STARTTIME', 'ENDTIME', 'SHEARY']
        result['STATUS'] = result['STATUS'].replace({'1': '正常', '2': '正在添加'})

        items = result.to_json(orient="records", force_ascii=False)
        mastItems = json.loads(items)
    else:
        result = pd.DataFrame()
        mastItems = []
    mastTotal = len(result)
    return mastItems, mastTotal

def show_mastdata(data):
    list_key = []
    for key_i in data.keys():
        if key_i not in ['pageIndex', 'pageSize', 'UPLOAD_TIME']:
            list_key.append(key_i)
    if len(list_key) == 0:
        if 'UPLOAD_TIME' in data.keys():
            select_data = "select * from data_log_information where UPLOAD_TIME like '%s%%';" % data['UPLOAD_TIME']
        else:
            select_data = "select * from data_log_information;"
    elif len(list_key) == 1:
        if 'UPLOAD_TIME' in data.keys():
            select_data = "select * from data_log_information where %s = '%s' and UPLOAD_TIME like '%s%%';" % (list_key[0], data[list_key[0]], data['UPLOAD_TIME'])
        else:
            select_data = "select * from data_log_information where %s = '%s' ;" % (list_key[0], data[list_key[0]])
    elif len(list_key) == 2:
        if 'UPLOAD_TIME' in data.keys():
            select_data = "select * from data_log_information where %s = '%s' and %s = '%s' and UPLOAD_TIME like '%s%%';" % (list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], data['UPLOAD_TIME'])
        else:
            select_data = "select * from data_log_information where %s = '%s' and %s = '%s';" % (list_key[0], data[list_key[0]], list_key[1], data[list_key[1]])
    else:
        if 'UPLOAD_TIME' in data.keys():
            select_data = "select * from data_log_information where %s = '%s' and %s = '%s' and %s = '%s' and UPLOAD_TIME like '%s%%';" % (list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]], data['UPLOAD_TIME'])
        else:
            select_data = "select * from data_log_information where %s = '%s' and %s = '%s' and %s = '%s';" % (list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]])
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database='cefengta', charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    values = cursor.fetchall()
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    if len(values) > 0:
        result = pd.DataFrame(values)
        result.columns = col_name_list
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records", force_ascii=False)
        Items = json.loads(items)
        total = len(result)
    else:
        Items = []
        total = 0
    return Items,total

def showMastPage(data):
    list_key = []
    if data['userType'] == 1:
        for key_i in data.keys():
            if key_i not in ['sort', 'pageIndex', 'pageSize', 'sortUD', 'userType', 'organize']:
                list_key.append(key_i)
        if len(list_key) == 0:
            select_data = "select * from cefengta.static_information;"
        elif len(list_key) == 1:
            select_data = "select * from cefengta.static_information where %s = '%s' ;" % (
            list_key[0], data[list_key[0]])
        elif len(list_key) == 2:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]])
        elif len(list_key) == 3:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and %s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]])
        elif len(list_key) == 4:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]])
        elif len(list_key) == 5:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]], list_key[4], data[list_key[4]])
        else:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]], list_key[4], data[list_key[4]], list_key[5], data[list_key[5]])
    else:
        for key_i in data.keys():
            if key_i not in ['sort', 'pageIndex', 'pageSize', 'sortUD', 'userType', 'organize']:
                list_key.append(key_i)
        if len(list_key) == 0:
            select_data = "select * from cefengta.static_information where organize_uuid = '%s';" % data['organize']
        elif len(list_key) == 1:
            select_data = "select * from cefengta.static_information where %s = '%s' and organize_uuid = '%s';" % (
            list_key[0], data[list_key[0]], data['organize'])
        elif len(list_key) == 2:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and organize_uuid = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], data['organize'])
        elif len(list_key) == 3:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and %s = '%s' and organize_uuid = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]], data['organize'])
        elif len(list_key) == 4:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s' and organize_uuid = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]], data['organize'])
        elif len(list_key) == 5:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s' and organize_uuid = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]], list_key[4], data[list_key[4]], data['organize'])
        else:
            select_data = "select * from cefengta.static_information where %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s'  and %s = '%s' and %s = '%s' and organize_uuid = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]], list_key[4], data[list_key[4]], list_key[5], data[list_key[5]], data['organize'])
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database='cefengta', charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    values = cursor.fetchall()
    col_name_list = [tuple[0] for tuple in cursor.description]
    if len(values) > 0:
        result = pd.DataFrame(values)
        result.columns = col_name_list
        result.drop(['RES'], axis=1, inplace=True)
        result['STATUS'] = result['STATUS'].replace({'1': '正常', '2': '正在添加'})
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records", force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

def write_job(token,savename):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database='cefengta', charset='utf8mb4')
    cursor = conn.cursor()

    savename = savename.replace("\\","\\\\")

    part_1 = "upid,respath"

    part_2 = "'" + token + "','" + savename + "'"

    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('jobs_select', part_1, part_2)

    print(sql_insert)

    cursor.execute(sql_insert)
    conn.commit()
    return(1)

def get_savename(token):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database='cefengta', charset='utf8mb4')
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT respath FROM jobs_select where upid='%s';" % token)
        p_value = cursor.fetchone()
        return(p_value[0])
    except:
        return("null")

def write_basic_rules_warn():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 规则1 （1）风速波动异常：1小时内平均风速变化<0.001m/s,或1小时内平均风速变化≥6m/s；
    part_1 = "ID, NAME, RULES, THRESHOLD1, THRESHOLD2, SUBJECTION"
    part_2 = "'" + 'SN01' + "','" + '风速波动异常' + "','" + '1小时内平均风速变化<0.001m/s,或1小时内平均风速变化≥6m/s' + "','" + '0.001' + "','" + '6' + "','" + 'SN01' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    # 规则2（2）风速大小异常：平均风速<0m/s，或平均风速>50m/s；
    part_1 = "ID, NAME, RULES, THRESHOLD1, THRESHOLD2, SUBJECTION"
    part_2 = "'" + 'SN02' + "','" + '风速大小异常' + "','" + '平均风速<0m/s，或平均风速>50m/s' + "','" + '0' + "','" + '50' + "','" + 'SN02' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    # 规则3 （3）风速标准差异常：风速标准差<0m/s，或风速标准差>5m/s；
    part_1 = "ID, NAME, RULES, THRESHOLD1, THRESHOLD2, SUBJECTION"
    part_2 = "'" + 'SN03' + "','" + '风速标准差异常' + "','" + '风速标准差<0m/s，或风速标准差>5m/s' + "','" + '0' + "','" + '5' + "','" + 'SN03' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    # 规则4（4）风向波动异常：1小时内平均风向变化<0.001°；
    part_1 = "ID, NAME, RULES, THRESHOLD1, SUBJECTION"
    part_2 = "'" + 'SN04' + "','" + '风向波动异常' + "','" + '1小时内平均风向变化<0.001°' + "','" + '0.001' + "','" + 'SN04' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    #规则5 （5）风向大小异常：平均风向<0°，或平均风向>360°；
    part_1 = "ID, NAME, RULES, THRESHOLD1, THRESHOLD2, SUBJECTION"
    part_2 = "'" + 'SN05' + "','" + '风向大小异常' + "','" + '平均风向<0°，或平均风向>360°' + "','" + '0' + "','" + '360' + "','" + 'SN05' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    #规则6 （6）气温波动异常：1小时内平均气温变化≥5°C；
    part_1 = "ID, NAME, RULES, THRESHOLD1, SUBJECTION"
    part_2 = "'" + 'SN06' + "','" + '气温波动异常' + "','" + '1小时内平均气温变化≥5°C' + "','" + '5' + "','" + 'SN06' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    #规则7 （7）气温大小异常：平均气温<-40°C，或平均气温>50°C
    part_1 = "ID, NAME, RULES, THRESHOLD1, THRESHOLD2, SUBJECTION"
    part_2 = "'" + 'SN07' + "','" + '气温大小异常' + "','" + '平均气温<-40°C，或平均气温>50°C' + "','" + '-40' + "','" + '50' + "','" + 'SN07' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()

    #规则8 （8）气压波动异常：3小时内平均气压变化≥1kPA；
    part_1 = "ID, NAME, RULES, THRESHOLD1, SUBJECTION"
    part_2 = "'" + 'SN08' + "','" + '气压波动异常' + "','" + '3小时内平均气压变化≥1kPA' + "','" + '1' + "','" + 'SN08' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    #规则9 （9）气压大小异常：平均气压<50kPA，或平均气压>110kPA；
    part_1 = "ID, NAME, RULES, THRESHOLD1, THRESHOLD2, SUBJECTION"
    part_2 = "'" + 'SN09' + "','" + '气压大小异常' + "','" + '平均气压<50kPA，或平均气压>110kPA' + "','" + '50' + "','" + '110' + "','" + 'SN09' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    #规则10 （10）风速相关性异常：50m高平均风速与30米高平均风速差值≥2m/s，或50m高平均风速与10米高平均风速差值≥4m/s；
    part_1 = "ID, NAME, RULES, THRESHOLD1, THRESHOLD2, SUBJECTION"
    part_2 = "'" + 'SN10' + "','" + '风速相关性异常' + "','" + '50m高平均风速与30米高平均风速差值≥2m/s，或50m高平均风速与10米高平均风速差值≥4m/s' + "','" + '2' + "','" + '4' + "','" + 'SN10' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    #规则11 （11）风向相关性异常：50m高平均风向与30m高平均风向差值≥22.5°。'
    part_1 = "ID, NAME, RULES, THRESHOLD1, SUBJECTION"
    part_2 = "'" + 'SN11' + "','" + '风向相关性异常' + "','" + '50m高平均风向与30m高平均风向差值≥22.5°' + "','" + '22.5' + "','" + 'SN11' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()

    # 规则12 （12）数据未收到：3内天未收到测风数据'
    part_1 = "ID, NAME, RULES, SUBJECTION"
    part_2 = "'" + 'SN12' + "','" + '数据未收到' + "','" + '3内天未收到测风数据' + "','" + 'SN12' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    # 规则13 （13）SN13，数据缺测：数据出现空值'
    part_1 = "ID, NAME, RULES, SUBJECTION"
    part_2 = "'" + 'SN13' + "','" + '数据缺测' + "','" + '数据出现空值' + "','" + 'SN13' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    # 规则14 （14）SN14，发生冰冻：12小时内平均气温<4°C，且平均风速变化<0.001m/s
    part_1 = "ID, NAME, RULES, SUBJECTION"
    part_2 = "'" + 'SN14' + "','" + '发生冰冻' + "','" + '12小时内平均气温<4°C，且平均风速变化<0.001m/s' + "','" + 'SN14' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    # 规则15 （15）SN15，电压异常：电池<250V或大于350V
    part_1 = "ID, NAME, RULES, SUBJECTION"
    part_2 = "'" + 'SN15' + "','" + '电压异常' + "','" + '电池<250V或大于350V' + "','" + 'SN15' + "'"
    sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()


    cursor.close()
    conn.close()

def basic_rules_warn_subjection(data):
    # 规则1 （1）风速波动异常：1小时内平均风速变化<0.001m/s,或1小时内平均风速变化≥6m/s；
    if data['SUBJECTION'] == 'SN01':
        return '1小时内平均风速变化<%sm/s,或1小时内平均风速变化≥%sm/s' % (data['THRESHOLD1'], data['THRESHOLD2'])

    # 规则2（2）风速大小异常：平均风速<0m/s，或平均风速>50m/s；
    elif data['SUBJECTION'] == 'SN02':
        return '平均风速<%sm/s，或平均风速>%sm/s' % (data['THRESHOLD1'], data['THRESHOLD2'])

    # 规则3 （3）风速标准差异常：风速标准差<0m/s，或风速标准差>5m/s；
    elif data['SUBJECTION'] == 'SN03':
        return '风速标准差<%sm/s，或风速标准差>%sm/s' % (data['THRESHOLD1'], data['THRESHOLD2'])

    # 规则4（4）风向波动异常：1小时内平均风向变化<0.001°；
    elif data['SUBJECTION'] == 'SN04':
        return '1小时内平均风向变化<%s°' % data['THRESHOLD1']

    #规则5 （5）风向大小异常：平均风向<0°，或平均风向>360°；
    elif data['SUBJECTION'] == 'SN05':
        return '平均风向<%s°，或平均风向>%s°' % (data['THRESHOLD1'], data['THRESHOLD2'])

    #规则6 （6）气温波动异常：1小时内平均气温变化≥5°C；
    elif data['SUBJECTION'] == 'SN06':
        return '1小时内平均气温变化≥%s°C' % data['THRESHOLD1']

    #规则7 （7）气温大小异常：平均气温<-40°C，或平均气温>50°C
    elif data['SUBJECTION'] == 'SN07':
        return '平均气温<%s°C，或平均气温>%s°C' % (data['THRESHOLD1'], data['THRESHOLD2'])

    #规则8 （8）气压波动异常：3小时内平均气压变化≥1kPA；
    elif data['SUBJECTION'] == 'SN08':
        return '3小时内平均气压变化≥%skPA' % data['THRESHOLD1']

    #规则9 （9）气压大小异常：平均气压<50kPA，或平均气压>110kPA；
    elif data['SUBJECTION'] == 'SN09':
        return '平均气压<%skPA，或平均气压>%skPA' % (data['THRESHOLD1'], data['THRESHOLD2'])

    #规则10 （10）风速相关性异常：50m高平均风速与30米高平均风速差值≥2m/s，或50m高平均风速与10米高平均风速差值≥4m/s；
    elif data['SUBJECTION'] == 'SN10':
        return '50m高平均风速与30米高平均风速差值≥%sm/s，或50m高平均风速与10米高平均风速差值≥%sm/s' % (data['THRESHOLD1'], data['THRESHOLD2'])

    #规则11 （11）风向相关性异常：50m高平均风向与30m高平均风向差值≥22.5°。'
    elif data['SUBJECTION'] == 'SN11':
        return '50m高平均风向与30m高平均风向差值≥%s°' % data['THRESHOLD1']

    else:
        return 'error'

def rules_verify_subjection(data):
    RULES = basic_rules_verify_subjection(data)
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    sql_update = "UPDATE verify_rules set %s='%s' where ID = '%s';" % ('RULES', RULES, data['ID'])
    cursor.execute(sql_update)
    conn.commit()
    cursor.close()
    conn.close()

def basic_rules_verify_subjection(data):
    # 规则1 （1）风速波动异常：1小时内平均风速变化<0.001m/s,或1小时内平均风速变化≥6m/s；
    if data['SUBJECTION'] == 'JN01':
        return '1小时内平均风速变化≥%sm/s,且1小时内平均风速变化<%sm/s' % (data['THRESHOLD1'], data['THRESHOLD2'])

    # 规则2（2）风速大小异常：平均风速<0m/s，或平均风速>50m/s；
    elif data['SUBJECTION'] == 'JN02':
        return '平均风速≥%sm/s，且平均风速≤%sm/s' % (data['THRESHOLD1'], data['THRESHOLD2'])

    # 规则3 （3）风速标准差异常：风速标准差<0m/s，或风速标准差>5m/s；
    elif data['SUBJECTION'] == 'JN03':
        return '风速标准差≥sm/s，且风速标准差≤%sm/s' % (data['THRESHOLD1'], data['THRESHOLD2'])

    # 规则4（4）风向波动异常：1小时内平均风向变化<0.001°；
    elif data['SUBJECTION'] == 'JN04':
        return '1小时内平均风向变化≥%s°' % data['THRESHOLD1']

    #规则5 （5）风向大小异常：平均风向<0°，或平均风向>360°；
    elif data['SUBJECTION'] == 'JN05':
        return '平均风向≥%s°，且平均风向≤%s°' % (data['THRESHOLD1'], data['THRESHOLD2'])

    #规则6 （6）气温波动异常：1小时内平均气温变化≥5°C；
    elif data['SUBJECTION'] == 'JN06':
        return '1小时内平均气温变化<%s°C' % data['THRESHOLD1']

    #规则7 （7）气温大小异常：平均气温<-40°C，或平均气温>50°C
    elif data['SUBJECTION'] == 'JN07':
        return '平均气温≥%s°C，且平均气温≤%s°C' % (data['THRESHOLD1'], data['THRESHOLD2'])

    #规则8 （8）气压波动异常：3小时内平均气压变化≥1kPA；
    elif data['SUBJECTION'] == 'JN08':
        return '3小时内平均气压变化<%skPA' % data['THRESHOLD1']

    #规则9 （9）气压大小异常：平均气压<50kPA，或平均气压>110kPA；
    elif data['SUBJECTION'] == 'JN09':
        return '平均气压≥%skPA，且平均气压≤%skPA' % (data['THRESHOLD1'], data['THRESHOLD2'])

    #规则10 （10）风速相关性异常：50m高平均风速与30米高平均风速差值≥2m/s，或50m高平均风速与10米高平均风速差值≥4m/s；
    elif data['SUBJECTION'] == 'JN10':
        return '50m高平均风速与30米高平均风速差值<%sm/s，50m高平均风速与10米高平均风速差值<%sm/s' % (data['THRESHOLD1'], data['THRESHOLD2'])

    #规则11 （11）风向相关性异常：50m高平均风向与30m高平均风向差值≥22.5°。'
    elif data['SUBJECTION'] == 'JN11':
        return '50m高平均风向与30m高平均风向差值<%s°' % data['THRESHOLD1']

    else:
        return 'error'

def add_warn(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if not cursor.execute("SHOW TABLES LIKE 'alarm_rules';"):
        creat_table = "CREATE TABLE alarm_rules (ID VARCHAR(45) NOT NULL," \
                      "NAME VARCHAR(45) NULL,RULES VARCHAR(100) NULL,THRESHOLD1 FLOAT NULL,THRESHOLD2 FLOAT NULL," \
                      "SUBJECTION VARCHAR(45) NULL, PRIMARY KEY (ID));"
        cursor.execute(creat_table)
        conn.commit()
        write_basic_rules_warn()

    cursor.execute("SELECT ID FROM alarm_rules where ID='%s';" % data['ID'])
    p_value = cursor.fetchall()
    if len(p_value) == 0:
        part_1 = ""
        part_2 = ""
        for key_name in data.keys():
            if len(part_1) != 0:
                part_1 += "," + key_name
                if isinstance(data[key_name], str):
                    part_2 += ",'" + data[key_name] + "'"
                else:
                    part_2 += ",'" + str(data[key_name]) + "'"
            else:
                part_1 += key_name
                if isinstance(data[key_name], str):
                    part_2 += "'" + data[key_name] + "'"
                else:
                    part_2 += "'" + str(data[key_name]) + "'"
        RULES = basic_rules_warn_subjection(data)
        part_1 += "," + 'RULES'
        part_2 += ",'" + RULES + "'"
        sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_rules', part_1, part_2)
        cursor.execute(sql_insert)
        conn.commit()
        insert_type = 0
    else:
        insert_type = 1
    cursor.close()
    conn.close()
    return insert_type

def edit_warn(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    for key_name in data.keys():
        if key_name not in ['ID']:
            sql_update = "UPDATE alarm_rules set %s='%s' where ID = '%s';" % (key_name, data[key_name], data['ID'])
            cursor.execute(sql_update)
    RULES = basic_rules_warn_subjection(data)
    sql_update = "UPDATE alarm_rules set %s='%s' where ID = '%s';" % ('RULES', RULES, data['ID'])
    cursor.execute(sql_update)
    conn.commit()
    cursor.close()
    conn.close()

def list_warn():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    select_data = "SELECT NAME from alarm_rules;"
    cursor.execute(select_data)
    p_value = cursor.fetchall()
    cursor.close()
    conn.close()
    return ','.join(pd.DataFrame(p_value).loc[:, 0].tolist())

def showWarnPage(data):
    list_key = []
    for key_i in data.keys():
        if key_i not in ['pageIndex', 'pageSize']:
            list_key.append(key_i)

    if len(list_key) == 0:
        select_data = "SELECT * FROM alarm_rules;"
    elif len(list_key) == 1:
        select_data = "SELECT * FROM alarm_rules where %s = '%s' ;" % (list_key[0], data[list_key[0]])
    else:
        select_data = "SELECT * FROM alarm_rules where %s = '%s' and %s = '%s';" % (
        list_key[0], data[list_key[0]], list_key[1], data[list_key[1]])


    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = ['ID', 'NAME', 'RULES', 'THRESHOLD1', 'THRESHOLD2', 'SUBJECTION']
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records",
                                                                                        force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

def delete_warn(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 删除数据
    sql_delete = "DELETE FROM alarm_rules WHERE ID='%s';" % data['ID']
    cursor.execute(sql_delete)
    conn.commit()
    cursor.close()
    conn.close()

def add_warnTemp(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if not cursor.execute("SHOW TABLES LIKE 'alarm_template';"):
        creat_table = "CREATE TABLE alarm_template (ID VARCHAR(45) NOT NULL," \
                      "TEMPLATE VARCHAR(45) NULL,TULES_ID VARCHAR(45) NULL, PRIMARY KEY (ID));"
        cursor.execute(creat_table)
        conn.commit()
    cursor.execute("SELECT ID FROM alarm_template where ID='%s';" % data['ID'])
    p_value = cursor.fetchall()
    if len(p_value) == 0:
        part_1 = ""
        part_2 = ""
        for key_name in data.keys():
            if len(part_1) != 0:
                part_1 += "," + key_name
                if isinstance(data[key_name], str):
                    part_2 += ",'" + data[key_name] + "'"
                else:
                    part_2 += ",'" + str(data[key_name]) + "'"
            else:
                part_1 += key_name
                if isinstance(data[key_name], str):
                    part_2 += "'" + data[key_name] + "'"
                else:
                    part_2 += "'" + str(data[key_name]) + "'"
        sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('alarm_template', part_1, part_2)
        cursor.execute(sql_insert)
        conn.commit()
        insert_type = 0
    else:
        insert_type = 1
    cursor.close()
    conn.close()
    return insert_type

def edit_warnTemp(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    for key_name in data.keys():
        if key_name not in ['ID']:
            sql_update = "UPDATE alarm_template set %s='%s' where ID = '%s';" % (key_name, data[key_name], data['ID'])
            cursor.execute(sql_update)
    conn.commit()
    cursor.close()
    conn.close()

def delete_warnTemp(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 删除数据
    sql_delete = "DELETE FROM alarm_template WHERE ID='%s';" % data['ID']
    cursor.execute(sql_delete)
    conn.commit()
    cursor.close()
    conn.close()

def showWarnTempPage(data):
    list_key = []
    for key_i in data.keys():
        if key_i not in ['pageIndex', 'pageSize']:
            list_key.append(key_i)
    if len(list_key) == 0:
        select_data = "SELECT * FROM alarm_template;"
    elif len(list_key) == 1:
        select_data = "SELECT * FROM alarm_template where %s = '%s' ;" % (list_key[0], data[list_key[0]])
    else:
        select_data = "SELECT * FROM alarm_template where %s = '%s' and %s = '%s';" % (
            list_key[0], data[list_key[0]], list_key[1], data[list_key[1]])

    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records",
                                                                                        force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

def list_channel(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()

    if data['channelType'] == '1':
        select_sql = "SELECT USEDCHANNEL, CHANNELNAME FROM channel_configuration WHERE ID = '%s';" % data['ID']
        cursor.execute(select_sql)
        values = cursor.fetchall()
        if len(values):
            values = pd.DataFrame(values)
            values.columns = ['USEDCHANNEL', 'CHANNELNAME']
            values = values.replace('', np.nan)
            values.dropna(axis=0, inplace=True)
            values = values[values['USEDCHANNEL'] != 'Date_Time']
            values.reset_index(inplace=True, drop=True)
            result = {}
            for i in range(len(values)):
                result[values.loc[i, 'USEDCHANNEL']] = values.loc[i, 'CHANNELNAME']
    elif data['channelType'] == '2':
        select_sql = "SELECT USEDCHANNEL, CHANNELNAME FROM channel_configuration WHERE USEDCHANNEL LIKE '%%WS_AVG%%' and ID = '%s';" % data['ID']
        cursor.execute(select_sql)
        values = cursor.fetchall()
        if len(values):
            values = pd.DataFrame(values)
            values.columns = ['USEDCHANNEL', 'CHANNELNAME']
            result = {}
            for i in range(len(values)):
                result[values.loc[i, 'USEDCHANNEL']] = values.loc[i, 'CHANNELNAME']
    elif data['channelType'] == '3':
        select_sql = "SELECT USEDCHANNEL, CHANNELNAME FROM channel_configuration WHERE USEDCHANNEL LIKE '%%WD_AVG%%' and ID = '%s';" % data['ID']
        cursor.execute(select_sql)
        values = cursor.fetchall()
        if len(values):
            values = pd.DataFrame(values)
            values.columns = ['USEDCHANNEL', 'CHANNELNAME']
            result = {}
            for i in range(len(values)):
                result[values.loc[i, 'USEDCHANNEL']] = values.loc[i, 'CHANNELNAME']
    else:
        select_sql = "SELECT HIGHT FROM channel_configuration WHERE USEDCHANNEL LIKE '%%WS_AVG%%' and ID = '%s';" % data['ID']
        cursor.execute(select_sql)
        values = cursor.fetchall()
        if len(values):
            values = pd.DataFrame(values)
            values.columns = ['HIGHT']
            result = {}
            height_list = [int(item) for item in list(set(values['HIGHT'].tolist()))]
            height_list.sort(reverse=False)
            result['height'] = ','.join([str(item) for item in height_list])
    cursor.close()
    conn.close()
    return result

def create_alarm_information():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if not cursor.execute("SHOW TABLES LIKE 'alarm_information';"):
        creat_table = "CREATE TABLE alarm_information (warnID INT NOT NULL AUTO_INCREMENT," \
                      "ID VARCHAR(45) NULL,NAME VARCHAR(45) NULL,channelID VARCHAR(45) NULL,warnTime VARCHAR(45) NULL,warnType VARCHAR(45) NULL,status VARCHAR(45) NULL, PRIMARY KEY (warnID));"
        cursor.execute(creat_table)
        conn.commit()
    cursor.close()
    conn.close()

def showWarnHistoryPage(data):
    if 'channelName' in data:
        data['channelID'] = data.pop('channelName')

    list_key = []
    if data['userType'] == 1:
        for key_i in data.keys():
            # if key_i == 'channelID'
            if key_i not in ['pageIndex', 'pageSize', 'startDate', 'endDate', 'userType', 'organize']:
                list_key.append(key_i)
        if len(list_key) == 0:
            select_data = "SELECT * FROM alarm_information;"
        elif len(list_key) == 1:
            select_data = "SELECT * FROM alarm_information where %s = '%s';" % (list_key[0], data[list_key[0]])
        elif len(list_key) == 2:
            select_data = "SELECT * FROM alarm_information where %s = '%s' and %s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]])
        else:
            select_data = "SELECT * FROM alarm_information where %s = '%s' and %s = '%s' and %s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]])
    else:
        for key_i in data.keys():
            if key_i not in ['pageIndex', 'pageSize', 'startDate', 'endDate', 'userType', 'organize']:
                list_key.append(key_i)
        if len(list_key) == 0:
            select_data = "SELECT t2.* FROM cefengta.static_information as t1 inner join cefengta.alarm_information as t2 on t1.ID = t2.ID where t1.organize_uuid = '%s';" % data['organize']
        elif len(list_key) == 1:
            select_data = "SELECT t2.* FROM cefengta.static_information as t1 inner join cefengta.alarm_information as t2 on t1.ID = t2.ID where t2.%s = '%s' and t1.organize_uuid = '%s';" % (list_key[0], data[list_key[0]], data['organize'])
        elif len(list_key) == 2:
            select_data = "SELECT t2.* FROM cefengta.static_information as t1 inner join cefengta.alarm_information as t2 on t1.ID = t2.ID where t2.%s = '%s' and t2.%s = '%s' and t1.organize_uuid = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], data['organize'])
        else:
            select_data = "SELECT t2.* FROM cefengta.static_information as t1 inner join cefengta.alarm_information as t2 on t1.ID = t2.ID where t2.%s = '%s' and t2.%s = '%s' and t2.%s = '%s' and t1.organize_uuid = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]], data['organize'])


    if ('startDate' in data.keys()) and ('endDate' in data.keys()):
        if len(list_key) == 0:
            select_data = select_data[:-1] + " where warnTime >= '%s' and warnTime <= '%s';" % (data['startDate'], data['endDate'])
        else:
            select_data = select_data[:-1] + " and warnTime >= '%s' and warnTime <= '%s';" % (
            data['startDate'], data['endDate'])
    elif 'startDate' in data.keys():
        if len(list_key) == 0:
            select_data = select_data[:-1] + " where warnTime >= '%s';" %data['startDate']
        else:
            select_data = select_data[:-1] + " and warnTime >= '%s';" % data['startDate']
    elif 'endDate' in data.keys():
        if len(list_key) == 0:
            select_data = select_data[:-1] + " where warnTime <= '%s';" % data['endDate']
        else:
            select_data = select_data[:-1] + " and warnTime <= '%s';" % data['endDate']
    else:
        select_data = select_data
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records",
                                                                                        force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

def warn_handle(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if data == 'all':
        select_data = "select warnID from alarm_information where status = '告警中';"
        cursor.execute(select_data)
        values = cursor.fetchall()
        Va_ID = pd.DataFrame(values)
        Va_ID.columns = ['warn_id']
        warn_id_list = Va_ID['warn_id'].tolist()
    else:
        warn_id_list = data.split(',')
    for warn_id in warn_id_list:
        sql_update = "UPDATE alarm_information set %s='%s' where warnID = '%s';" % ('status', '已处理', warn_id)
        cursor.execute(sql_update)
        conn.commit()
        select_data1 = "select ID from alarm_information where warnID = '%s';" % warn_id
        cursor.execute(select_data1)
        values = cursor.fetchall()
        select_data2 = "select ID from alarm_information where status = '告警中' and ID = '%s';" % values[0][0]
        cursor.execute(select_data2)
        values1 = cursor.fetchall()
        Value = pd.DataFrame(values1)
        if len(Value) == 0:
            sql_update1 = "UPDATE static_information set %s='%s' where ID = '%s';" % ('STATUS', '1', values[0][0])
            cursor.execute(sql_update1)
            conn.commit()
    cursor.close()
    conn.close()

def warn_mainPage(params):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if params['userType'] == 1:
        sql_select = "SELECT * from alarm_information where status = '告警中';"
    else:
        sql_select = "SELECT t2.* from cefengta.static_information as t1 inner join cefengta.alarm_information as t2 on t1.ID = t2.ID where t2.status = '告警中' and t1.organize_uuid = '%s';" % params['organize']


    cursor.execute(sql_select)
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        result.sort_values(by='warnTime', ascending=False, inplace=True)
        result.reset_index(inplace=True, drop=True)
        ID = result['ID'].tolist()
        R = pd.DataFrame(columns=['warnInfo', 'ID'])
        for i, mast_id in enumerate(set(ID)):
            index = result[result['ID'] == mast_id]
            index.reset_index(inplace=True, drop=True)
            R.loc[i, 'warnInfo'] = '%s测风塔%s通道发生%s告警' % (index['ID'][0], index['channelID'][0], index['warnType'][0])
            R.loc[i, 'ID'] = '%s' % index['ID'][0]
        items = R.to_json(orient="records", force_ascii=False)
        items = json.loads(items)
    else:
        R = pd.DataFrame()
        items = []
    total = len(R)
    cursor.close()
    conn.close()
    return items, total

def warnTemp_mast(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    sql_update = "UPDATE static_information set warnTempID='%s' where ID = '%s';" % (data['warnTempID'], data['ID'])
    cursor.execute(sql_update)
    conn.commit()
    cursor.close()
    conn.close()
    return (1)

def cleanTemp_mast(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    sql_update = "UPDATE static_information set TEMPLET_ID='%s' where ID = '%s';" % (data['TEMPLATE_ID'], data['ID'])
    cursor.execute(sql_update)
    conn.commit()
    cursor.close()
    conn.close()
    return (1)

def list_project():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()

    select_sql = "SELECT DISTINCT PROJECT FROM static_information;"
    cursor.execute(select_sql)
    values = cursor.fetchall()

    cursor.close()
    conn.close()
    return ','.join(pd.DataFrame(values).loc[:, 0].tolist())

def add_mast(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断有没有用户表，没有的话要新建
    if not cursor.execute("SHOW TABLES LIKE 'static_information';"):
        creat_table = "CREATE TABLE static_information (ID VARCHAR(45) NOT NULL," \
                      "NAME VARCHAR(45) NULL,organize_uuid VARCHAR(45) NULL,TEMPLET_ID VARCHAR(45) NULL,warnTempID VARCHAR(45) NULL," \
                      "STATUS int NULL, UPLOAD_STATUS VARCHAR(45) NULL,PROVICE VARCHAR(45) NULL,CITY VARCHAR(45) NULL, TOWN VARCHAR(45) NULL," \
                      "RES VARCHAR(45) NULL, LON VARCHAR(45) NULL, LAT VARCHAR(45) NULL, MAILUSER VARCHAR(45) NULL, MAILPASSWD VARCHAR(45) NULL," \
                      "MAILCODE VARCHAR(45) NULL, UTC VARCHAR(45) NULL, ANGLE VARCHAR(45) NULL, SHADOW VARCHAR(45) NULL, PROJECT VARCHAR(45) NULL, " \
                      "ELE VARCHAR(45) NULL, SERIALNUM VARCHAR(45) NULL, EQUIPTYPE VARCHAR(45) NULL, CODE VARCHAR(45) NULL, DATATYPE VARCHAR(45) NULL, " \
                      "MISSPRID VARCHAR(45) NULL, DATALINE VARCHAR(45) NULL, CREATTIME VARCHAR(45) NULL,PRIMARY KEY (ID));"
        cursor.execute(creat_table)
        conn.commit()
    # 插入数据
    cursor.execute("SELECT ID FROM static_information where ID='%s';" % data['ID'])
    p_value = cursor.fetchall()
    if len(p_value) == 0:
        part_1 = ""
        part_2 = ""
        for key_name in data.keys():
            if len(part_1) != 0:
                if key_name == 'organize':
                    part_1 += "," + 'organize_uuid'
                else:
                    part_1 += "," + key_name
                if isinstance(data[key_name], str):
                    part_2 += ",'" + data[key_name] + "'"
                else:
                    part_2 += ",'" + str(data[key_name]) + "'"
            else:
                part_1 += key_name
                if isinstance(data[key_name], str):
                    part_2 += "'" + data[key_name] + "'"
                else:
                    part_2 += "'" + str(data[key_name]) + "'"
        part_1 += "," + 'CREATTIME'
        part_2 += ",'" + datetime.datetime.now().strftime('%Y-%m-%d') + "'"
        sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('static_information', part_1, part_2)
        cursor.execute(sql_insert)
        conn.commit()
        insert_type = 0
    else:
        insert_type = 1
    cursor.execute("SELECT * FROM static_information where ID='%s';" % data['ID'])
    values = cursor.fetchall()
    col_name_list = [tuple[0] for tuple in cursor.description]
    result = pd.DataFrame(values)
    result.columns = col_name_list
    result = result.to_json(orient="records", force_ascii=False)
    result = json.loads(result)[0]
    cursor.close()
    conn.close()
    return insert_type,result

def edit_mast(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()

    for key_name in data.keys():
        if key_name != 'ID':
            if key_name == 'organize':
                sql_update = "UPDATE static_information set %s='%s' where ID = '%s';" % ('organize_uuid', data[key_name], data['ID'])
            else:
                sql_update = "UPDATE static_information set %s='%s' where ID = '%s';" % (key_name, data[key_name], data['ID'])
            cursor.execute(sql_update)

    conn.commit()
    insert_type = 0
    cursor.close()
    conn.close()
    return insert_type

def showMastPageTrue(data):
    list_key = []
    if data['userType'] == 1:
        for key_i in data.keys():
            if key_i not in ['sort', 'pageIndex', 'pageSize', 'sortUD', 'userType', 'organize']:
                list_key.append(key_i)
        if len(list_key) == 0:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功';"
        elif len(list_key) == 1:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' ;" % (
            list_key[0], data[list_key[0]])
        elif len(list_key) == 2:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.%s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]])
        elif len(list_key) == 3:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]])
        elif len(list_key) == 4:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]])
        else:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]], list_key[4], data[list_key[4]])
    else:
        for key_i in data.keys():
            if key_i not in ['sort', 'pageIndex', 'pageSize', 'sortUD', 'userType', 'organize']:
                list_key.append(key_i)
        if len(list_key) == 0:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.organize_uuid ='%s';" % data['organize']
        elif len(list_key) == 1:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.organize_uuid ='%s';" % (
            list_key[0], data[list_key[0]], data['organize'])
        elif len(list_key) == 2:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.%s = '%s' and t1.organize_uuid ='%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], data['organize'])
        elif len(list_key) == 3:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.organize_uuid ='%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]], data['organize'])
        elif len(list_key) == 4:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.organize_uuid ='%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]], data['organize'])
        else:
            select_data = "select t1.*, t2.SHEARY, t2.STARTTIME, t2.ENDTIME from cefengta.static_information as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.UPLOAD_STATUS = '成功' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.%s = '%s' and t1.organize_uuid ='%s';" % (
                list_key[0], data[list_key[0]], list_key[1], data[list_key[1]], list_key[2], data[list_key[2]],
                list_key[3],
                data[list_key[3]], list_key[4], data[list_key[4]], data['organize'])

    conn = pymysql.connect(host=host, port=port, user=user, password=password, database='cefengta', charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    values = cursor.fetchall()

    col_name_list = [tuple[0] for tuple in cursor.description]
    if len(values) > 0:
        result = pd.DataFrame(values)
        result.columns = col_name_list
        result.drop(['organize_uuid', 'TEMPLET_ID', 'warnTempID', 'UPLOAD_STATUS', 'RES'], axis=1, inplace=True)
        result['STATUS'] = result['STATUS'].replace({'1': '正常', '2': '正在添加'})
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records", force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

def list_mastTrue(params):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if (params['userType'] == 1) | (params['userType'] == '1'):
        select_sql = "SELECT ID FROM static_information where UPLOAD_STATUS = '成功';"
    else:
        select_sql = "SELECT ID FROM static_information where UPLOAD_STATUS = '成功' and organize_uuid = '%s';" % params['organize']
    cursor.execute(select_sql)
    keys = cursor.fetchall()
    if (params['userType'] == 1) | (params['userType'] == '1'):
        select_sql = "SELECT NAME FROM static_information where UPLOAD_STATUS = '成功';"
    else:
        select_sql = "SELECT NAME FROM static_information where UPLOAD_STATUS = '成功' and organize_uuid = '%s';" % params['organize']
    cursor.execute(select_sql)
    values = cursor.fetchall()

    cursor.close()
    conn.close()
    return ','.join(pd.DataFrame(keys).loc[:, 0].tolist()),','.join(pd.DataFrame(values).loc[:, 0].tolist())

def list_organize():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()

    select_sql = "SELECT DISTINCT organize FROM user_information;"
    cursor.execute(select_sql)
    values = cursor.fetchall()

    cursor.close()
    conn.close()
    return ','.join(pd.DataFrame(values).loc[:, 0].tolist())

def write_token(username,token):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断有没有用户表，没有的话要新建
    # 插入数据

    sql_set = "UPDATE user_information set token= '%s' WHERE username = '%s';" %(token,username)

    cursor.execute(sql_set)
    conn.commit()
    cursor.close()
    conn.close()

def read_data_from_sql_statistics(ID, start_time, end_time):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (
    ID, start_time, end_time))
    col_name_list_ID = [tuple[0] for tuple in cursor.description]
    values_ID = cursor.fetchall()
    data_ID = pd.DataFrame(values_ID)
    data_ID.columns = col_name_list_ID

    cursor.execute("SELECT CHANNELNAME, USEDCHANNEL, UNIT FROM cefengta.channel_configuration where ID='%s';" % (ID))
    col_name_list_channel = [tuple[0] for tuple in cursor.description]
    values_channel = cursor.fetchall()
    data_channel = pd.DataFrame(values_channel)
    data_channel.columns = col_name_list_channel
    cursor.close()
    conn.close()
    return data_ID, data_channel

def cal_len_time(tagTime):
    start_time = tagTime[:19]
    end_time = tagTime[20:]
    time_1 = pd.date_range(start=start_time, end=end_time, freq='10T')
    return len(time_1)

def read_warning_result_from_sql(ID):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    select_data = "select * from cefengta.warning_result where ID = '%s';" % ID
    cursor.execute(select_data)
    col_name_list_warning_result = [tuple[0] for tuple in cursor.description]
    values_warning_result = cursor.fetchall()
    if len(values_warning_result) > 0:
        data_warning_result = pd.DataFrame(values_warning_result)
        data_warning_result.columns = col_name_list_warning_result
    else:
        data_warning_result = pd.DataFrame(columns=['tagID', 'ID', 'NAME', 'channelID', 'channelName', 'tagType', 'tagTime'])
    cursor.close()
    conn.close()
    return data_warning_result

def statistics(data_params):
    ID = data_params['ID']
    start_time = data_params['startDate']
    end_time = data_params['endDate']
    data_ID, data_channel = read_data_from_sql_statistics(ID, start_time, end_time)
    time_1 = pd.date_range(start=start_time, end=end_time, freq='10T')
    data_len_lillun = len(time_1)
    data_warning_result = read_warning_result_from_sql(ID)
    data_warning_result['len'] = data_warning_result['tagTime'].apply(lambda x: cal_len_time(x))
    Result = pd.DataFrame(
        columns=['channelName', 'UNIT', 'HEIGHT', 'total', 'useable', 'miss', 'invalid', 'ice', 'recovery', 'avg',
                 'min', 'max',
                 'std'])
    for i, col_id in enumerate(data_ID.columns):
        index_channel = np.where(data_channel['USEDCHANNEL'] == col_id)[0]
        if col_id != 'Date_Time':
            data_ID[col_id] = data_ID[col_id].replace('None', np.nan)
            data_ID[col_id] = (data_ID[col_id].replace(' ', np.nan).astype('float'))
            if '_P_' in col_id:
                if 'SD' not in col_id:
                    Result.loc[i, 'UNIT'] = 'kPa'
                    if (data_channel.loc[index_channel, 'UNIT'].values[0] == 'kPa') | (
                            data_channel.loc[index_channel, 'UNIT'].values[0] == 'KPa'):
                        p_unit = 1000
                    elif data_channel.loc[index_channel, 'UNIT'].values[0] == 'hPa':
                        p_unit = 100
                    elif data_channel.loc[index_channel, 'UNIT'].values[0] == 'mb':
                        p_unit = 100
                    elif data_channel.loc[index_channel, 'UNIT'].values[0] == 'mmHg':
                        p_unit = 133
                    else:
                        p_unit = 1
                    data_ID[col_id] = data_ID[col_id] / p_unit
            else:
                Result.loc[i, 'UNIT'] = data_channel.loc[index_channel, 'UNIT'].values[0]
            Result.loc[i, 'channelName'] = data_channel.loc[index_channel, 'CHANNELNAME'].values[0]
            Result.loc[i, 'HEIGHT'] = int(col_id.split('_')[0])
            Result.loc[i, 'total'] = data_len_lillun
            Result.loc[i, 'useable'] = len(data_ID[col_id].dropna(how='any'))
            Result.loc[i, 'miss'] = Result.loc[i, 'total'] - Result.loc[i, 'useable']
            Result.loc[i, 'invalid'] = np.nansum(data_warning_result[(data_warning_result['channelID'] == col_id) & (
                        data_warning_result['tagType'] == '无效')]['len'])
            Result.loc[i, 'ice'] = np.nansum(data_warning_result[(data_warning_result['channelID'] == col_id) & (
                        data_warning_result['tagType'] == '冰冻')]['len'])
            Result.loc[i, 'recovery'] = np.around(Result.loc[i, 'useable'] / Result.loc[i, 'total'] * 100, 2)
            Result.loc[i, 'avg'] = np.around(np.nanmean(data_ID[col_id]), 3)
            Result.loc[i, 'min'] = np.around(np.nanmin(data_ID[col_id]), 3)
            Result.loc[i, 'max'] = np.around(np.nanmax(data_ID[col_id]), 3)
            Result.loc[i, 'std'] = np.around(np.nanstd(data_ID[col_id]), 3)
    Result.reset_index(inplace=True, drop=True)
    items = Result.to_json(orient="records", force_ascii=False)
    items = json.loads(items)
    total = len(Result)
    return items, total

def update_verifyTempID(ID, verifyRuleIDs):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    try:
        add_temp = "INSERT INTO verify_template (ID,TEMPLATE,RULES_ID) VALUES ('%s','%s','%s');" % (ID,ID,verifyRuleIDs)
        cursor.execute(add_temp)
    except:
        add_temp = "UPDATE verify_template set RULES_ID= '%s' WHERE ID = '%s';" % (verifyRuleIDs,ID)
        cursor.execute(add_temp)
    select_temp = "SELECT TEMPLET_ID from static_information WHERE ID = '%s';" % (ID)
    cursor.execute(select_temp)
    TID = cursor.fetchone()[0]
    update_verifyTempID = "UPDATE static_information set TEMPLET_ID= '%s' WHERE ID = '%s';" % (ID,ID)
    cursor.execute(update_verifyTempID)
    conn.commit()
    cursor.close()
    conn.close()
    return TID

def callback_verifyTempID(ID,TID):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    update_verifyTempID = "UPDATE static_information set TEMPLET_ID= '%s' WHERE ID = '%s';" % (TID,ID)
    cursor.execute(update_verifyTempID)
    delete_temp = "DELETE from verify_template WHERE ID= '%s';"% (ID)
    cursor.execute(delete_temp)
    conn.commit()
    cursor.close()
    conn.close()

def read_data_from_sql_cal_corr(ID, ID_can, yuan_ID, yuan_ID_can, start_time, end_time):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT Date_Time, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (ID_can, ID, start_time, end_time))
    col_name_list_ID = [tuple[0] for tuple in cursor.description]
    values_ID = cursor.fetchall()
    data_ID = pd.DataFrame(values_ID)
    data_ID.columns = col_name_list_ID

    cursor.execute("SELECT Date_Time, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (yuan_ID_can, yuan_ID, start_time, end_time))
    col_name_list_yuan_ID = [tuple[0] for tuple in cursor.description]
    values_yuan_ID = cursor.fetchall()
    data_yuan_ID = pd.DataFrame(values_yuan_ID)
    data_yuan_ID.columns = col_name_list_yuan_ID
    cursor.close()
    conn.close()
    return data_ID, ID_can, data_yuan_ID

def linear_func(x, a, b):
    return a*x+b

def cal_corr_data(can_data, yuan_can_data, a,b):
    if np.isnan(can_data):
        if np.isnan(yuan_can_data):
            return can_data
        else:
            return a*yuan_can_data + b
    else:
        return can_data

def cal_correlation(yuan_ID, yuan_ID_can, ID, ID_can, start_time, end_time):
    # ID(被分析测风编号)   yuan_ID
    # channelName（被分析通道名称）  yuan_ID_can
    # sourceID（参考测风编号，可与ID一致） ID
    # sourceChannelName（参考通道名称，可以是原始名称）ID_can
    # startDate（开始时间）  start_time
    # endDate（结束时间）  end_time
    data_ID, ID_can, data_yuan_ID = read_data_from_sql_cal_corr(ID, ID_can, yuan_ID, yuan_ID_can, start_time, end_time)
    data_ID = data_ID[data_ID[ID_can] != ' ']
    data_ID[ID_can] = data_ID[ID_can].replace('None', np.nan).astype('float')
    data_yuan_ID = data_yuan_ID[data_yuan_ID[yuan_ID_can] != ' ']
    data_yuan_ID[yuan_ID_can] = data_yuan_ID[yuan_ID_can].replace('None', np.nan).astype('float')
    Data_corr = pd.merge(data_ID, data_yuan_ID, how='outer', on='Date_Time')
    Data_corr = Data_corr.dropna(subset=[ID_can])
    Data_corr = Data_corr.dropna(subset=[yuan_ID_can])
    popt, pcov = curve_fit(linear_func, Data_corr[yuan_ID_can], Data_corr[ID_can])
    a = popt[0]
    b = popt[1]
    calc_ydata = [linear_func(i, a, b) for i in Data_corr[yuan_ID_can]]
    res_ydata = np.array(Data_corr[ID_can]) - np.array(calc_ydata)
    ss_res = np.sum(res_ydata ** 2)
    ss_tot = np.sum((Data_corr[ID_can] - np.mean(Data_corr[ID_can])) ** 2)
    r_squared = 1 - (ss_res / ss_tot)
    # 相关性方程 相关性系数 均值差
    return 'y = %.2f * x + %.2f' % (a, b), r_squared, np.nanmean(Data_corr[ID_can]) - np.nanmean(Data_corr[yuan_ID_can])

def adjust_data(ID, type_can, off=0, scale=1):
    # ID(测风编号)
    # type_can（通道名称，如90_WS_AVG）
    # OFF（偏移量，默认0）
    # SCALE（尺度因子，默认1）
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # select_tasknumber = "UPDATE cefengta.data_%s_clean SET %s = CASE WHEN %s IS NULL THEN NULL ELSE (%s * %s + %s) END;" % (ID, type_can, type_can, type_can, str(scale), str(off))
    select_tasknumber = "UPDATE cefengta.data_%s_clean SET %s = %s * %s + %s" % (
    ID, type_can, type_can, str(scale), str(off))
    cursor.execute(select_tasknumber)
    conn.commit()
    cursor.close()
    conn.close()

def cal_shear_all(data, ceng_list):
    ceng_list.sort(reverse=False)
    ceng = np.array(ceng_list)
    wind_mean = []
    for id in ceng_list:
        wind_mean.append(np.nanmean(data[data[str(id) + '_WS_AVG'] != ' '][str(id) + '_WS_AVG'].replace('None', np.nan).astype('float')))
    wind = np.array(wind_mean)
    power_func = lambda x, c, a: c * np.power(x, a)
    params, cov = curve_fit(power_func, wind, ceng, maxfev=1000)
    # wind = params[0] * np.power(ceng_list, params[1])
    return np.around(params[0], 3), np.around(params[1], 3)

def verticalExtra_data(ID, type_can, start_time, end_time, height):
    # ID(测风编号)
    # type_can（通道名称，如90_WS_AVG,100_WS_AVG）
    # start_time（开始时间）
    # end_time（结束时间）
    # height（外推高度）
    height_can = [int(x.split('_')[0]) for x in type_can.split(',')]
    yuan_ID_can_id = min(height_can, key=lambda x: abs(x - float(height)))
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT Date_Time, %s, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (
    type_can,str(yuan_ID_can_id) + '_WS_SD', ID, start_time, end_time))
    col_name_list_ID = [tuple[0] for tuple in cursor.description]
    values_ID = cursor.fetchall()
    data_ID = pd.DataFrame(values_ID)
    data_ID.columns = col_name_list_ID
    c, a = cal_shear_all(data_ID, height_can)
    cal_data = data_ID[['Date_Time', str(yuan_ID_can_id) + '_WS_AVG', str(yuan_ID_can_id) + '_WS_SD']]
    cal_data[str(yuan_ID_can_id) + '_WS_AVG'] = cal_data[str(yuan_ID_can_id) + '_WS_AVG'].replace(' ', np.nan).replace('None', np.nan).astype('float')
    cal_data[str(yuan_ID_can_id) + '_WS_SD'] = cal_data[str(yuan_ID_can_id) + '_WS_SD'].replace(' ', np.nan).replace(
        'None', np.nan).astype('float')
    cal_data[str(height) + '_WS_AVG'] = cal_data.apply(lambda x: x[str(yuan_ID_can_id) + '_WS_AVG'] * np.power(float(height) / yuan_ID_can_id, a), axis=1)
    cal_data[str(height) + '_WS_AVG'] = cal_data[str(height) + '_WS_AVG'].apply(lambda x: np.around(x, 3))
    cal_data[str(height) + '_WS_SD'] = cal_data[str(yuan_ID_can_id) + '_WS_SD']
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    part_1 = 'ID, ORIGINCHANNEL, CHANNELNAME, USEDCHANNEL, HIGHT, UNIT'
    part_2 = "'" + ID + "','" + str(height) + 'm高度风速均值' + "','" + str(height) + 'm高度风速均值' + "','" + str(height) + '_WS_AVG' + "','" + str(height) + "','" + 'm/s' + "'"
    insert_row = 'REPLACE INTO %s (%s) VALUES (%s);' % ('channel_configuration', part_1, part_2)
    cursor.execute(insert_row)
    conn.commit()
    part_2 = "'" + ID + "','" + str(height) + 'm高度风速标准差'+ "','" + str(height) + 'm高度风速标准差' + "','" + str(height) + '_WS_SD' + "','" + str(height) + "','" + 'm/s' + "'"
    insert_row = 'REPLACE INTO %s (%s) VALUES (%s);' % ('channel_configuration', part_1, part_2)
    cursor.execute(insert_row)
    conn.commit()
    cursor.close()
    conn.close()
    try:
        conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
        cursor = conn.cursor()
        add_column = "ALTER TABLE cefengta.data_%s_clean ADD COLUMN %s VARCHAR(50) NULL, ADD COLUMN %s VARCHAR(50) NULL;" % (ID, str(height) + '_WS_AVG', str(height) + '_WS_SD')
        cursor.execute(add_column)
        conn.commit()
        cursor.close()
        conn.close()
    except:
        print('exist columns')
    cal_data.reset_index(inplace=True)
    cal_data.replace(np.nan, 'None', inplace=True)
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    num = 0
    for i in range(len(cal_data)):
        sql_update = "UPDATE cefengta.data_%s_clean set %s='%s', %s='%s' where Date_Time = '%s';" % (ID, str(height) + '_WS_AVG', cal_data.loc[i, str(height) + '_WS_AVG'], str(height) + '_WS_SD', cal_data.loc[i, str(height) + '_WS_SD'], cal_data.loc[i, 'Date_Time'])
        cursor.execute(sql_update.replace("'None'", "NULL"))
        num += 1
        if num > 1000:
            conn.commit()
            num = 0
    if num < 1000:
        conn.commit()
    cursor.close()
    conn.close()

def showVRPage(data):
    if data['userType'] == 1:
        if ('ID' in data.keys()) & ('tagType' in data.keys()):
            select_data = "SELECT * FROM warning_result where ID ='%s' and tagType ='%s';" % (
            data['ID'], data['tagType'])
        elif ('ID' in data.keys()):
            select_data = "SELECT * FROM warning_result where ID ='%s';" % (data['ID'])
        elif ('tagType' in data.keys()):
            select_data = "SELECT * FROM warning_result where tagType='%s';" % (data['tagType'])
        else:
            select_data = "SELECT * FROM warning_result ;"
    else:
        if ('ID' in data.keys()) & ('tagType' in data.keys()):
            select_data = "SELECT t2.* FROM cefengta.static_information as t1 inner join cefengta.warning_result as t2 on t1.ID = t2.ID where t2.ID ='%s' and t2.tagType ='%s' and t1.organize_uuid = '%s';" % (
            data['ID'], data['tagType'], data['organize'])
        elif ('ID' in data.keys()):
            select_data = "SELECT t2.* FROM cefengta.static_information as t1 inner join cefengta.warning_result as t2 on t1.ID = t2.ID where ID ='%s' and t1.organize_uuid = '%s';" % (data['ID'], data['organize'])
        elif ('tagType' in data.keys()):
            select_data = "SELECT t2.* FROM cefengta.static_information as t1 inner join cefengta.warning_result as t2 on t1.ID = t2.ID where tagType='%s' and t1.organize_uuid = '%s';" % (data['tagType'], data['organize'])
        else:
            select_data = "SELECT t2.* FROM cefengta.static_information as t1 inner join cefengta.warning_result as t2 on t1.ID = t2.ID where t1.organize_uuid = '%s';" % data['organize']

    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records", force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

def showChannelTempPage(data):
    select_data = "SELECT * FROM channel_configuration where ID ='%s';" % (data['ID'])
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        result['type'] = result['USEDCHANNEL'].apply(lambda x: x.split('_')[-1])
        result['UNIT'] = result.apply(lambda x:cal_unit_show(x['UNIT'], x['type']), axis=1)
        # result['type'] = result['type'].replace('MIN', '最小值')
        result = result[['ID', 'ORIGINCHANNEL', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'CHANNELNAMETYPE', 'type', 'USEDCHANNEL']]


        if 'pageIndex' in data.keys():
            pageIndex = data['pageIndex']
            pageSize = data['pageSize']
            items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records",
                                                                                            force_ascii=False)
        else:
            items = result.to_json(orient="records", force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

##################
def detail(data):
    select_data = "SELECT t1.ID, t1.LON, t1.LAT, t2.STARTTIME, t2.ENDTIME, t2.SHEAR, t2.RHO, t2.WD1, t2.WD2, t2.WD3, " \
                  "t2.WP1, t2.WP2, t2.WP3, t2.MONTHWS1, t2.MONTHWS2, t2.MONTHWS3, t2.MONTHWS4, t2.MONTHWS5, t2.MONTHWS6, " \
                  "t2.MONTHWS7, t2.MONTHWS8, t2.MONTHWS9, t2.MONTHWS10, t2.DAYWS1, t2.DAYWS2, t2.DAYWS3, t2.DAYWS4, " \
                  "t2.DAYWS5, t2.DAYWS6, t2.DAYWS7, t2.DAYWS8, t2.DAYWS9, t2.DAYWS10, t2.WSDIS1, t2.WSDIS2, t2.WSDIS3, t2.WSDIS4, t2.WSDIS5, " \
                  "t2.WSDIS6, t2.WSDIS7, t2.WSDIS8, t2.WSDIS9, t2.WSDIS10, t2.WPDIS1, t2.WPDIS2, t2.WPDIS3, t2.WPDIS4, " \
                  "t2.WPDIS5, t2.WPDIS6, t2.WPDIS7, t2.WPDIS8, t2.WPDIS9, t2.WPDIS10 FROM cefengta.static_information " \
                  "as t1 inner join cefengta.dynamic_information as t2 on t1.ID = t2.ID where t1.ID = '%s';" % (data['ID'])
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        items = result.to_json(orient="records", force_ascii=False)
        items = json.loads(items)[0]
    else:
        items = []
    cursor.close()
    conn.close()
    return items

def deldelmastchannel(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 删除数据
    sql_delete = "DELETE FROM channel_configuration WHERE ID='%s' and ORIGINCHANNEL='%s';" % (data['ID'], data['ORIGINCHANNEL'])
    cursor.execute(sql_delete)
    try:
        sql_delete_col = "ALTER TABLE %s DROP COLUMN %s ;" % ('data_' + data['ID'] + '_clean', data['USEDCHANNEL'])
        cursor.execute(sql_delete_col)
    except:
        print('error')
    conn.commit()
    cursor.close()
    conn.close()


def editmastchannel(data):
    data['USEDCHANNEL'] = cal_name_USEDCHANNEL_1(data['HIGHT'], data['UNIT'], data['type'], data['CHANNELNAMETYPE'])
    data['CHANNELNAME'] =cal_name_CHANNELNAME(data['USEDCHANNEL'])
    data['UNIT'] = cal_unit(data['UNIT'])
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    # 插入数据
    part_1 = ""
    part_2 = ""
    for key_name in data.keys():
        if key_name not in ['type', 'CHANNELNAMETYPE']:
            if len(part_1) != 0:
                part_1 += "," + key_name
                if isinstance(data[key_name], str):
                    part_2 += ",'" + data[key_name] + "'"
                else:
                    part_2 += ",'" + str(data[key_name]) + "'"
            else:
                part_1 += key_name
                if isinstance(data[key_name], str):
                    part_2 += "'" + data[key_name] + "'"
                else:
                    part_2 += "'" + str(data[key_name]) + "'"
    sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % ('channel_configuration', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    insert_type = 0
    cursor.close()
    conn.close()
    return insert_type


def delete_rule(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 删除数据
    sql_delete = "DELETE FROM verify_rules WHERE ID='%s';" % (data['ID'])
    cursor.execute(sql_delete)
    conn.commit()
    cursor.close()
    conn.close()

def showrulePage(data):
    list_key = []
    for key_i in data.keys():
        if key_i not in ['pageIndex', 'pageSize', 'startDate', 'endDate', 'userType', 'organize']:
            list_key.append(key_i)
    if len(list_key) == 0:
        select_data = "SELECT * FROM verify_rules;"
    elif len(list_key) == 1:
        select_data = "SELECT * FROM verify_rules where %s = '%s';" % (list_key[0], data[list_key[0]])
    else:
        select_data = "SELECT * FROM verify_rules where %s = '%s' and %s = '%s';" % (
            list_key[0], data[list_key[0]], list_key[1], data[list_key[1]])
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # print(select_data)
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records",
                                                                                        force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total


def add_rule(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断有没有用户表，没有的话要新建
    if not cursor.execute("SHOW TABLES LIKE 'verify_rules';"):
        creat_table = "CREATE TABLE verify_rules (ID VARCHAR(32) NOT NULL," \
                      "NAME VARCHAR(256) NULL,RULES VARCHAR(1024) NULL,THRESHOLD1 VARCHAR(32) NULL,THRESHOLD2 VARCHAR(32) NULL," \
                      "SUBJECTION VARCHAR(32) NULL,STATUS tinyint(1) DEFAULT 1,create_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,PRIMARY KEY (ID));"
        cursor.execute(creat_table)
        conn.commit()
    # 插入数据
    cursor.execute("SELECT ID FROM verify_rules where ID='%s';" % data['ID'])
    p_value = cursor.fetchall()
    if len(p_value) == 0:
        part_1 = ""
        part_2 = ""
        for key_name in data.keys():
            if len(part_1) != 0:
                part_1 += "," + key_name
                if isinstance(data[key_name], str):
                    part_2 += ",'" + data[key_name] + "'"
                else:
                    part_2 += ",'" + str(data[key_name]) + "'"
            else:
                part_1 += key_name
                if isinstance(data[key_name], str):
                    part_2 += "'" + data[key_name] + "'"
                else:
                    part_2 += "'" + str(data[key_name]) + "'"
        sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('verify_rules', part_1, part_2)
        cursor.execute(sql_insert)
        conn.commit()
        insert_type = 0
    else:
        insert_type = 1
    cursor.close()
    conn.close()
    return insert_type

def edit_rule(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    # 插入数据
    part_1 = ""
    part_2 = ""
    for key_name in data.keys():
        if len(part_1) != 0:
            part_1 += "," + key_name
            if isinstance(data[key_name], str):
                part_2 += ",'" + data[key_name] + "'"
            else:
                part_2 += ",'" + str(data[key_name]) + "'"
        else:
            part_1 += key_name
            if isinstance(data[key_name], str):
                part_2 += "'" + data[key_name] + "'"
            else:
                part_2 += "'" + str(data[key_name]) + "'"
    sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % ('verify_rules', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    insert_type = 0
    cursor.close()
    conn.close()
    return insert_type

def delete_ruleTemp(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 删除数据
    sql_delete = "DELETE FROM verify_template WHERE ID='%s';" % (data['ID'])
    cursor.execute(sql_delete)
    conn.commit()
    cursor.close()
    conn.close()

def showruleTempPage(data):
    list_key = []
    for key_i in data.keys():
        if key_i not in ['pageIndex', 'pageSize', 'startDate', 'endDate', 'userType', 'organize']:
            list_key.append(key_i)
    if len(list_key) == 0:
        select_data = "SELECT * FROM verify_template;"
    elif len(list_key) == 1:
        select_data = "SELECT * FROM verify_template where %s = '%s';" % (list_key[0], data[list_key[0]])
    else:
        select_data = "SELECT * FROM verify_template where %s = '%s' and %s = '%s';" % (
            list_key[0], data[list_key[0]], list_key[1], data[list_key[1]])
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    cursor.execute(select_data)
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    if len(values):
        result = pd.DataFrame(values)
        result.columns = col_name_list
        pageIndex = data['pageIndex']
        pageSize = data['pageSize']
        items = result.iloc[(pageIndex - 1) * pageSize:pageIndex * pageSize, :].to_json(orient="records",
                                                                                        force_ascii=False)
        items = json.loads(items)
    else:
        result = pd.DataFrame()
        items = []
    total = len(result)
    cursor.close()
    conn.close()
    return items, total

def add_ruleTemp(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 判断有没有用户表，没有的话要新建
    if not cursor.execute("SHOW TABLES LIKE 'verify_template';"):
        creat_table = "CREATE TABLE verify_template (ID VARCHAR(128) NOT NULL," \
                      "TEMPLATE VARCHAR(128) NULL,RULES_ID text NULL,STATE VARCHAR(100) DEFAULT '正常' NULL,PRIMARY KEY (ID));"
        cursor.execute(creat_table)
        conn.commit()
    # 插入数据
    cursor.execute("SELECT ID FROM verify_template where ID='%s';" % data['ID'])
    p_value = cursor.fetchall()
    if len(p_value) == 0:
        part_1 = ""
        part_2 = ""
        for key_name in data.keys():
            if len(part_1) != 0:
                part_1 += "," + key_name
                if isinstance(data[key_name], str):
                    part_2 += ",'" + data[key_name] + "'"
                else:
                    part_2 += ",'" + str(data[key_name]) + "'"
            else:
                part_1 += key_name
                if isinstance(data[key_name], str):
                    part_2 += "'" + data[key_name] + "'"
                else:
                    part_2 += "'" + str(data[key_name]) + "'"
        sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % ('verify_template', part_1, part_2)
        cursor.execute(sql_insert)
        conn.commit()
        insert_type = 0
    else:
        insert_type = 1
    cursor.close()
    conn.close()
    return insert_type

def edit_ruleTemp(data):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    # 插入数据
    part_1 = ""
    part_2 = ""
    for key_name in data.keys():
        if len(part_1) != 0:
            part_1 += "," + key_name
            if isinstance(data[key_name], str):
                part_2 += ",'" + data[key_name] + "'"
            else:
                part_2 += ",'" + str(data[key_name]) + "'"
        else:
            part_1 += key_name
            if isinstance(data[key_name], str):
                part_2 += "'" + data[key_name] + "'"
            else:
                part_2 += "'" + str(data[key_name]) + "'"
    sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % ('verify_template', part_1, part_2)
    cursor.execute(sql_insert)
    conn.commit()
    insert_type = 0
    cursor.close()
    conn.close()
    return insert_type

def list_rule():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()

    select_sql = "SELECT DISTINCT NAME FROM verify_rules;"
    cursor.execute(select_sql)
    values = cursor.fetchall()

    cursor.close()
    conn.close()
    return ','.join(pd.DataFrame(values).loc[:, 0].tolist())

def list_ruleTemp():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()

    select_sql = "SELECT DISTINCT TEMPLATE FROM verify_template;"
    cursor.execute(select_sql)
    values = cursor.fetchall()

    cursor.close()
    conn.close()
    return ','.join(pd.DataFrame(values).loc[:, 0].tolist())



if __name__ == '__main__':
    # data = {}
    # data['ID'] ='S001'
    # data['NAME'] = '风速波动异常0'
    # data['THRESHOLD1'] =0.003
    # data['THRESHOLD2'] = 7
    # data['SUBJECTION'] = 'SN01'
    # add_warn(data)
    # data = {}
    # data['ID'] = 'S001'
    # data['NAME'] = '风速波动异常0'
    # data['THRESHOLD1'] = 0.005
    # data['THRESHOLD2'] = 9
    # data['SUBJECTION'] = 'SN01'
    # edit_warn(data)
    # data = {}
    # data['ID'] = 'L000906'
    # data['channelType'] = 4
    # l = list_channel(data)
    # print(l)
    # llllllllllllllllllllllllll
    # create_alarm_information()
    # data = {}
    # data['ID'] = 'M607314'
    # data['startDate'] = '2024-6-10'
    # data['endDate'] = '2024-6-15'
    # data['pageIndex'] = 1
    # data['pageSize'] = 4
    # items, total = showWarnHistoryPage(data)
    # print(items, total)
    # data = '1,2,3'
    # warn_handle(data)
    # items, total = warn_mainPage()
    # print(items, total)
    # update_verifyTempID('1', 'M607314')
    # verticalExtra_data('M607314', '30_WS_AVG, 50_WS_AVG', '2021-04-30 00:00:00', '2021-05-30 00:00:00', 85)
    # adjust_data('M607314', '30_WS_AVG', -1, 1)
    # data ={}
    # data['ID'] = '003470'
    # a, b = showChannelTempPage(data)
    # print(a,b)
    data = {}
    data['ID'] = 'M607314'
    data['ORIGINCHANNEL'] = 'new'
    deldelmastchannel(data)