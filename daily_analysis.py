import numpy as np
import pandas as pd
import pymysql
import json
import subprocess
import sys


def cal_wind_can(data, wind_name, can):
    '''
    计算风速统计年况或者月况
    :param data:pd.Dataframe
    :param wind_name:风速列名——均值
    :param can: 'year'或者'month'
    :return: 计算结果pd.Dataframe， can='month'计算结果如下
          00    01    02    03    04    05  ...    18    19    20    21    22    23
    01  3.31  3.25  3.37  3.07  3.08  3.04  ...  3.41  3.54  3.78  3.58  3.39  3.49
    02  3.33  3.12  3.16  3.15  3.33  3.18  ...  2.89  2.84  3.03  3.00  3.18  3.27
    03  3.76  3.66  3.59  3.42  3.44  3.35  ...  3.53  3.81  3.82  3.49  3.49  3.58
    04  3.06  3.21  3.32  3.10  3.11  2.96  ...  3.56  3.54  3.48  3.49  3.33  3.11
    05  3.27  3.10  3.14  3.12  3.23  3.20  ...  3.04  3.26  3.25  3.39  3.25  3.24
    06  3.78  3.83  3.68  3.67  3.71  3.71  ...  3.51  3.65  3.69  3.84  3.83  3.77
    07  4.18  4.43  4.35  4.45  4.32  4.37  ...  3.67  3.96  3.97  3.88  3.95  4.01
    08  3.35  3.32  3.15  3.03  3.00  2.92  ...  3.05  3.23  3.15  3.24  3.34  3.38
    09  4.29  4.12  3.46  3.17  3.17  3.31  ...  4.32  4.50  4.73  4.80  4.59  4.23
    10  3.97  3.89  3.73  3.38  3.43  3.23  ...  5.19  5.32  5.09  4.89  4.66  4.41
    11  2.68  2.80  2.66  2.62  2.62  2.50  ...  3.41  3.36  3.27  3.05  2.75  2.56
    12  2.90  2.90  2.93  3.16  2.92  2.86  ...  3.95  3.49  3.20  3.23  3.28  2.93
    can='year'
       bin  mean
    00  00  3.54
    01  01  3.53
    02  02  3.44
    03  03  3.36
    04  04  3.36
    05  05  3.32
    '''
    time_name = 'Date_Time'
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    result = {}
    if can == '年况':
        try:
            data['time'] = data[time_name].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S").split(' ')[1].split(':')[0])
        except:
            data['time'] = data[time_name].apply(lambda x: x.split(' ')[1].split(':')[0])
        result_i = {}
        for i in ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16',
                  '17', '18', '19', '20', '21', '22', '23']:
            data_i = data[data['time'] == i]
            if len(data_i) > 0:
                result_i[i] = np.around(np.nanmean(data_i[wind_name]), 2)
            else:
                result_i[i] = []
        result['year'] = result_i
    if can == '月况':
        try:
            data['time'] = data[time_name].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S").split('-')[1])
        except:
            data['time'] = data[time_name].apply(lambda x: x.split('-')[1])
        try:
            data['time_h'] = data[time_name].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S").split(' ')[1].split(':')[0])
        except:
            data['time_h'] = data[time_name].apply(lambda x: x.split(' ')[1].split(':')[0])
        for j in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            result_j = {}
            for i in ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16',
                      '17', '18', '19', '20', '21', '22', '23']:
                data_i = data[(data['time'] == j) & (data['time_h'] == i)]
                if len(data_i) > 0:
                    result_j[i] = np.around(np.nanmean(data_i[wind_name]), 2)
                else:
                    result_j[i] = []
            result[j] = result_j
    return result


def cal_wind_DWP_can(data, wind_name, rho, can):
    '''
    计算风速统计年况或者月况
    :param data:pd.Dataframe
    :param wind_name:风速列名——均值
    :param can: 'year'或者'month'
    :return: 计算结果pd.Dataframe， can='month'计算结果如下
           00     01     02     03     04  ...      19     20     21     22     23
    01  27.77  27.88  32.82  28.44  26.68  ...   38.89  48.89  38.69  33.04  31.93
    02  34.69  26.37  27.36  26.18  30.05  ...   25.67  31.87  29.64  30.32  32.43
    03  52.90  44.12  39.76  34.41  39.23  ...   57.26  59.34  40.90  39.49  47.59
    04  26.87  31.65  35.27  33.10  36.72  ...   36.43  37.88  35.98  30.00  26.56
    05  31.17  27.31  29.78  30.78  33.71  ...   29.75  26.81  31.41  30.43  29.94
    06  39.61  39.69  35.22  36.87  38.78  ...   36.66  40.28  42.39  46.03  39.51
    07  53.45  61.90  62.16  67.78  62.77  ...   54.02  51.89  53.60  53.22  52.42
    08  34.36  36.00  29.68  28.23  28.60  ...   33.57  36.79  37.33  36.73  35.71
    09  63.84  56.62  33.67  27.36  32.47  ...   70.21  69.43  74.13  64.87  55.61
    10  45.02  41.37  38.13  28.08  32.19  ...  101.38  92.97  83.87  73.63  60.32
    11  24.32  24.51  24.30  20.18  20.81  ...   36.23  29.98  25.29  21.94  21.27
    12  26.13  26.63  31.40  27.58  23.51  ...   40.15  35.63  34.79  36.04  31.33
    can='year'
       bin   mean
    00  00  38.95
    01  01  38.39
    02  02  36.38
    03  03  34.86
    04  04  35.87
    05  05   34.9
    '''
    time_name = 'Date_Time'
    data = data[data[wind_name] != ' ']
    data[wind_name] = data[wind_name].replace('None', np.nan).astype('float')
    wind_DWP_name = 'wind_DWP'
    data[wind_DWP_name] = data[wind_name].apply(lambda x: 0.5 * rho * np.power(x, 3))
    result = {}
    if can == '年况':
        try:
            data['time'] = data[time_name].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S").split(' ')[1].split(':')[0])
        except:
            data['time'] = data[time_name].apply(lambda x: x.split(' ')[1].split(':')[0])
        result_i = {}
        for i in ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16',
                  '17', '18', '19', '20', '21', '22', '23']:
            data_i = data[data['time'] == i]
            if len(data_i) > 0:
                result_i[i] = np.around(np.nanmean(data_i[wind_DWP_name]), 2)
            else:
                result_i[i] = []
        result['year'] = result_i
    if can == '月况':
        try:
            data['time'] = data[time_name].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S").split('-')[1])
        except:
            data['time'] = data[time_name].apply(lambda x: x.split('-')[1])
        try:
            data['time_h'] = data[time_name].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S").split(' ')[1].split(':')[0])
        except:
            data['time_h'] = data[time_name].apply(lambda x: x.split(' ')[1].split(':')[0])
        for j in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            result_j = {}
            for i in ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16',
                      '17', '18', '19', '20', '21', '22', '23']:
                data_i = data[(data['time'] == j) & (data['time_h'] == i)]
                if len(data_i) > 0:
                    result_j[i] = np.around(np.nanmean(data_i[wind_DWP_name]), 2)
                else:
                    result_j[i] = []
            result[j] = result_j
    return result


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


def read_rho(cefengta_id):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT RHO FROM cefengta.dynamic_information where ID='%s';" % cefengta_id)
    # # 获取查询结果
    # 获取表头
    values = float(cursor.fetchall()[0][0])
    cursor.close()
    conn.close()
    return values

def daily_analysis(cefengta_id,can_name,start_time,end_time,year_yue,key_value, savename):
    wind_name = can_name.split('m')[0] + '_WS_AVG'
    cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
    if key_value == '风速':
        result_json = cal_wind_can(cefengta_data, wind_name, year_yue)
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)
    elif key_value == '风功率':
        rho = read_rho(cefengta_id)
        result_json = cal_wind_DWP_can(cefengta_data, wind_name, rho, year_yue)
        json_str = json.dumps(result_json, indent=4)
        with open(savename, 'w') as f:
            f.write(json_str)


if __name__ == '__main__':
    import warnings

    warnings.filterwarnings("ignore")
    # cefengta_id = 'M003470'
    # start_time = '2022-05'
    # end_time = '2022-09'
    # can_name = '30m风速'
    # year_yue = '月况'
    # key_value = '风速'
    # savename = '/home/xiaowu/share/202311/测风塔系统/接口/daily_month.json'
    # 塔名，通道名称，时间起点，时间终点，月况/年况，风速/风功率
    # python3 daily_analysis.py M003470 30m风速 2022-05 2022-09 月况 风速 /home/xiaowu/share/202311/测风塔系统/接口/daily_month.json
    #
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):

        cefengta_id = sys.argv[1]
        can_name = sys.argv[2]
        start_time = sys.argv[3]
        end_time = sys.argv[4]
        year_yue = sys.argv[5]
        key_value = sys.argv[6]
        savename = sys.argv[7]

        wind_name = can_name.split('m')[0] + '_WS_AVG'
        cefengta_data = read_data_from_sql(cefengta_id, start_time, end_time, wind_name)
        if key_value == '风速':
            result_json = cal_wind_can(cefengta_data, wind_name, year_yue)
            json_str = json.dumps(result_json, indent=4)
            with open(savename, 'w') as f:
                f.write(json_str)
        elif key_value == '风功率':
            rho = read_rho(cefengta_id)
            print(rho)
            result_json = cal_wind_DWP_can(cefengta_data, wind_name, rho, year_yue)
            json_str = json.dumps(result_json, indent=4)
            with open(savename, 'w') as f:
                f.write(json_str)



