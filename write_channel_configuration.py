import pandas as pd
import json
import pymysql
import numpy as np
import sys
import subprocess


# def cal_name_USEDCHANNEL(height, unit, type):
#     if unit == 'ms':
#         can = 'WS'
#     elif unit == 'angle':
#         can = 'WD'
#     elif unit == 'temp':
#         can = 'T'
#     elif (unit == 'kPa') | (unit == 'hPa') | (unit == 'mb') | (unit == 'mmHg'):
#         can = 'P'
#     elif unit == 'v':
#         can = 'BATTERY'
#     elif unit == 'rh':
#         can = 'RH'
#     elif unit == 'quality':
#         can = 'REL'
#     else:
#         can = ''
#     if can != '':
#         return height + '_' + can + '_' + type
#     else:
#         return ''

def cal_name_USEDCHANNEL(height, unit, type):
    if unit == 'm/s':
        can = 'WS'
    elif unit == '°':
        can = 'WD'
    elif unit == '℃':
        can = 'T'
    elif (unit == 'kPa') | (unit == 'hPa') | (unit == 'mb') | (unit == 'mmHg'):
        can = 'P'
    elif unit == 'V':
        can = 'BATTERY'
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


def cal_unit(unit):
    if unit == 'ms':
        can = 'm/s'
    elif unit == 'angle':
        can = '°'
    elif unit == 'temp':
        can = '℃'
    elif unit == 'kpa':
        can = 'kPa'
    elif unit == 'hPa':
        can = 'hPa'
    elif unit == 'mb':
        can = 'mb'
    elif unit == 'mmHg':
        can = 'mmHg'
    elif unit == 'v':
        can = 'V'
    elif unit == 'rh':
        can = '%'
    elif unit == 'quality':
        can = '%'
    else:
        can = ''
    if can != '':
        return can
    else:
        return ''

def cal_name_CHANNELNAME(USEDCHANNEL):
    try:
        name_split = USEDCHANNEL.split('_')
        if name_split[1] == 'WS':
            can = '风速'
        elif name_split[1] == 'ZWS':
            can = 'Z向风速'
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


def read_splic_channel(data):
    data = data.fillna(np.nan)
    channel_result = pd.DataFrame()
    if 'ORIGINCHANNEL_use' not in data.columns:
        channel_result['ORIGINCHANNEL'] = data['ORIGINCHANNEL']
    else:
        channel_result['ORIGINCHANNEL'] = data['ORIGINCHANNEL_use']
    # data['type'] = data['type'].apply(lambda x: x.upper())
    channel_result['CHID'] = data['CHID']
    channel_result['ID'] = data['ID']
    channel_result['HIGHT'] = data['HIGHT']
    channel_result['OFF'] = data['OFF'].apply(lambda x:float(x))
    channel_result['UNIT'] = data['UNIT']
    channel_result['SCALE'] = data['SCALE'].apply(lambda x:float(x))
    data['UNIT'] = data.apply(lambda x: cal_unit(x['UNIT']), axis=1)
    channel_result['USEDCHANNEL'] = data.apply(lambda x:cal_name_USEDCHANNEL(x['HIGHT'], x['UNIT'], x['type']), axis=1)
    channel_result['CHANNELNAME'] = channel_result.apply(lambda x: cal_name_CHANNELNAME(x['USEDCHANNEL']), axis=1)
    channel_result['UNIT'] = channel_result.apply(lambda x: cal_unit(x['UNIT']), axis=1)
    key = (channel_result['ORIGINCHANNEL'] == 'Date & Time Stamp') | (channel_result['ORIGINCHANNEL'] == 'Timestamp') | \
          (channel_result['ORIGINCHANNEL'] == 'TIMESTAMP')
    channel_result.loc[key, 'CHANNELNAME'] = '时间'
    channel_result.loc[key, 'USEDCHANNEL'] = 'Date_Time'
    # channel_result['USEDCHANNEL'] = channel_result.apply(lambda x:cal_name_USEDCHANNEL_nan(x['ORIGINCHANNEL'], x['USEDCHANNEL']), axis=1)
    return channel_result


def create_channel_configuration_table(table_name, read_data):
    host = 'localhost'
    port = 3306
    user = 'wyh'  # 用户名
    password = 'Wyh123!@#'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if not cursor.execute("SHOW TABLES LIKE '%s';" % table_name):
        db_creat = "CREATE TABLE " + table_name + " ("
        for i in read_data.columns:
            db_creat += i + " "
            db_creat += "VARCHAR(100), "
        db_creat += "PRIMARY KEY(ID, ORIGINCHANNEL)) DEFAULT CHARSET=utf8;"
        db_creat = db_creat.replace('.', '_')
        # print(db_creat)
        cursor.execute(db_creat)
    cursor.close()
    conn.close()

def write_data(data, table_name):
    host = 'localhost'
    port = 3306
    user = 'wyh'  # 用户名
    password = 'Wyh123!@#'  # 密码
    database = 'cefengta'
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
        sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % (table_name, part_1, part_2)
        cursor.execute(sql_insert)
        num += 1
        if num > 1000:
            conn.commit()
            num = 0
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    import warnings

    warnings.filterwarnings("ignore")
    # 这里应该是输入页面信息然后存入数据库，只有一个输入的json就够了
    # file_path = '/home/xiaowu/share/202311/测风塔系统/function/cal_html/运达/M067315.json'
    #python3 write_channel_configuration.py /home/xiaowu/share/202311/测风塔系统/function/cal_html/运达/M067315.json
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        file_path = sys.argv[1]
        with open (file_path, 'r') as f:
            data = json.load(f)
        read_data = pd.DataFrame(data).T
        read_data = read_splic_channel(read_data)
        table_name = 'channel_configuration'
        create_channel_configuration_table(table_name, read_data)
        write_data(read_data, table_name)