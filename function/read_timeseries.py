import numpy as np
import pandas as pd
import pymysql
import json
import subprocess
import sys


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

def read_timeseries(cefengta_id,can_name,start_time,end_time,savename):
    if '均值' in can_name:
        type = 'AVG'
    elif '标准差' in can_name:
        type = 'SD'
    elif '最大值' in can_name:
        type = 'MIN'
    elif '最小值' in can_name:
        type = 'MAX'
    name_yuan = can_name.split('m')
    if 'Z向风速' in can_name:
        name = can_name.split('m')[0] + '_ZWS_' + type
    elif '风速' in can_name:
        name = can_name.split('m')[0] + '_WS_' + type
    elif '风向' in can_name:
        name = can_name.split('m')[0] + '_WD_' + type
    elif '气温' in can_name:
        name = can_name.split('m')[0] + '_T_' + type
    elif '气压' in can_name:
        name = can_name.split('m')[0] + '_P_' + type
    elif '相对湿度' in can_name:
        name = can_name.split('m')[0] + '_RH_' + type
    elif '电池' in can_name:
        name = can_name.split('m')[0] + '_V_' + type
    elif '可靠性' in can_name:
        name = can_name.split('m')[0] + '_REL'
    cefengta = read_data_from_sql(cefengta_id, start_time, end_time, name)
    if len(sys.argv) == 8:
        freq = sys.argv[7]

    result_json = {}

    result_json['xAxisData'] = cefengta['Date_Time'].tolist()
    result_json['seriesData'] = cefengta[name].tolist()
    json_str = json.dumps(result_json, indent=4)
    with open(savename, 'w') as f:
        f.write(json_str)


if __name__ == '__main__':
    # cefengta_id = 'M003470'
    # can_name = '30m风速'
    # can_type = '平均值'
    # start_time = '2022-05'
    # end_time = '2022-06'
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/timeseries.json'
    # freq = '原始'
    #  塔名，通道名称，通道类型，时间起点，时间终点， 结果存储名称路径
    # python3 read_timeseries.py M003470 30m风速均值 2022-05 2022-06 /home/xiaowu/share/202311/测风塔系统/接口/timeseries.json

    #
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_id = sys.argv[1]
        can_name = sys.argv[2]
        # can_name = sys.argv[2].split('m')[0] + 'm' + sys.argv[2].split('m')[1][:2]
        # can_type = sys.argv[2].split('m')[1][2:]
        start_time = sys.argv[3]
        end_time = sys.argv[4]
        savename = sys.argv[5]
        if '均值' in can_name:
            type = 'AVG'
        elif '标准差' in can_name:
            type = 'SD'
        elif '最大值' in can_name:
            type = 'MIN'
        elif '最小值' in can_name:
            type = 'MAX'
        name_yuan = can_name.split('m')
        if 'Z向风速' in can_name:
            name = can_name.split('m')[0] + '_ZWS_' + type
        elif '风速' in can_name:
            name = can_name.split('m')[0] + '_WS_' + type
        elif '风向' in can_name:
            name = can_name.split('m')[0] + '_WD_' + type
        elif '气温' in can_name:
            name = can_name.split('m')[0] + '_T_' + type
        elif '气压' in can_name:
            name = can_name.split('m')[0] + '_P_' + type
        elif '相对湿度' in can_name:
            name = can_name.split('m')[0] + '_RH_' + type
        elif '电池' in can_name:
            name = can_name.split('m')[0] + '_V_' + type
        elif '可靠性' in can_name:
            name = can_name.split('m')[0] + '_REL'
        cefengta = read_data_from_sql(cefengta_id, start_time, end_time, name)
        if len(sys.argv) == 8:
            freq = sys.argv[7]

        result_json = {}

        result_json['xAxisData'] = cefengta['Date_Time'].tolist()
        result_json['seriesData'] = cefengta[name].tolist()
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)

