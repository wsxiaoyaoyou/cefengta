import pandas as pd
import pymysql
import os
import datetime
from pathlib import Path
import numpy as np
import sys
import subprocess
import chardet
import data_clean_rule,insert_dynamic_information

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
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
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
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
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
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
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
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
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


def write_static_information(cefeng_name, state):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 插入数据
    sql_update = "UPDATE static_information SET UPLOAD_STATUS = '%s' WHERE ID='%s';" % (state, cefeng_name)
    cursor.execute(sql_update)
    conn.commit()
    cursor.close()
    conn.close()

def data_clean_exist(clean_table):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    if not cursor.execute("SHOW TABLES LIKE '%s';" % clean_table):
        clean_table_log = 'no'
    else:
        clean_table_log = 'yes'

    cursor.close()
    conn.close()
    return clean_table_log

def write_yuanshi_data(file_dir_path, datatype):
    cefeng_name = Path(file_dir_path).name
    # upload_file = str(file_dir_path)
    upload_file = 0
    upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    write_log(cefeng_name, str(upload_file), upload_time, '正在入库中')
    for file_name in os.listdir(file_dir_path):
        if file_name.endswith('.zip'):
            file_dir_path = file_dir_path + '/zip'
            # upload_file = file_dir_path + '/' + file_name
            upload_file = upload_file + 1
        elif file_name.endswith('.rar'):
            file_dir_path = file_dir_path + '/rar'
            # upload_file = file_dir_path + '/' + file_name
            upload_file = upload_file + 1
    for file_name in os.listdir(file_dir_path):
        if file_name == 'nrg':
            file_dir_path = file_dir_path + '/nrg'
    list = os.listdir(file_dir_path)
    file_type = list[0].split('.')[-1]
    if file_type == 'json':
        file_type = list[1].split('.')[-1]
    # 原始测风塔数据写入
    upload_file_wenjian = 0
    try:
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
                    else:
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
                # count_dict = {}
                # for item in columns_sql:
                #     if item in count_dict:
                #         count_dict[item] += 1
                #     else:
                #         count_dict[item] = 1

                # 使用列表推导式筛选出只出现一次的元素
                # name_data = [item for item in columns_sql if count_dict[item] == 1]
                # data = data[name_data]
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
                print('end write data at:' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                upload_file_wenjian = upload_file_wenjian + 1
        # 远程要带着这句
        data_clean_rule.data_clean_rule(cefeng_name)
        insert_dynamic_information.insert_dynamic_information(cefeng_name)
        clean_table_log = data_clean_exist('data_' + cefeng_name + '_clean')
        if clean_table_log == 'yes':
            upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            state = '成功'
            if upload_file == 0:
                upload_file = upload_file_wenjian
            write_log(cefeng_name, str(upload_file), upload_time, state)
            write_static_information(cefeng_name, '成功')
        else:
            upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            state = '失败'
            if upload_file == 0:
                upload_file = upload_file_wenjian
            write_log(cefeng_name, str(upload_file), upload_time, state)
            write_static_information(cefeng_name, '失败')
    except:
        upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        state = '失败'
        if upload_file == 0:
            upload_file = upload_file_wenjian
        write_log(cefeng_name, str(upload_file), upload_time, state)
        write_static_information(cefeng_name, '失败')
        # 开始处理数据
        # # A = ord(cefeng_name[0])
        # # B = 0
        # # for i in range(1, 7):
        # #     B = B + int(cefeng_name[i])
        # # for col in columns_sql:
        # #     read_data[col] = (read_data[col] + A) * B
        # write_data(data, table_name)


if __name__ == '__main__':
    # file_dir_path = Path('/home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/M005430')
    # python3 write_yuanshi_data.py /home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/M005430 0
    # result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    #                         shell=True)
    # if '67E3-17ED' in result.stdout.decode('utf-8'):
    #     file_dir_path = sys.argv[1]
    #     datatype = int(sys.argv[2])
        file_dir_path = 'D:\cefengta\\res\mast' + '\\' + "002"
        datatype = 0


        cefeng_name = Path(file_dir_path).name
        # upload_file = str(file_dir_path)
        upload_file = 0
        upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        write_log(cefeng_name, str(upload_file), upload_time, '正在入库中')
        for file_name in os.listdir(file_dir_path):
            if file_name.endswith('.zip'):
                file_dir_path = file_dir_path + '/zip'
                # upload_file = file_dir_path + '/' + file_name
                upload_file = upload_file + 1
            elif file_name.endswith('.rar'):
                file_dir_path = file_dir_path + '/rar'
                # upload_file = file_dir_path + '/' + file_name
                upload_file = upload_file + 1
        for file_name in os.listdir(file_dir_path):
            if file_name == 'nrg':
                file_dir_path = file_dir_path + '/nrg'
        list = os.listdir(file_dir_path)
        file_type = list[0].split('.')[-1]
        if file_type == 'json':
            file_type = list[1].split('.')[-1]
        # 原始测风塔数据写入
        upload_file_wenjian = 0
        # try:
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
                    else:
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
                # count_dict = {}
                # for item in columns_sql:
                #     if item in count_dict:
                #         count_dict[item] += 1
                #     else:
                #         count_dict[item] = 1

                # 使用列表推导式筛选出只出现一次的元素
                # name_data = [item for item in columns_sql if count_dict[item] == 1]
                # data = data[name_data]
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
                print('end write data at:' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                upload_file_wenjian = upload_file_wenjian + 1
        # 远程要带着这句
        data_clean_rule.data_clean_rule(cefeng_name)
        insert_dynamic_information.insert_dynamic_information(cefeng_name)
        clean_table_log = data_clean_exist('data_' + cefeng_name + '_clean')
        if clean_table_log == 'yes':
            upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            state = '成功'
            if upload_file == 0:
                upload_file = upload_file_wenjian
            write_log(cefeng_name, str(upload_file), upload_time, state)
            write_static_information(cefeng_name, '成功')
        else:
            upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            state = '失败'
            if upload_file == 0:
                upload_file = upload_file_wenjian
            write_log(cefeng_name, str(upload_file), upload_time, state)
            write_static_information(cefeng_name, '失败')
        # except:
        #     upload_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #     state = '失败'
        #     if upload_file == 0:
        #         upload_file = upload_file_wenjian
        #     write_log(cefeng_name, str(upload_file), upload_time, state)
        #     write_static_information(cefeng_name, '失败')
            # 开始处理数据
            # # A = ord(cefeng_name[0])
            # # B = 0
            # # for i in range(1, 7):
            # #     B = B + int(cefeng_name[i])
            # # for col in columns_sql:
            # #     read_data[col] = (read_data[col] + A) * B
            # write_data(data, table_name)