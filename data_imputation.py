
import sys
import simplejson
import pandas as pd
import numpy as np
import pymysql
from scipy.optimize import curve_fit
host = 'localhost'
port = 3306
user = 'root' #用户名
password = '123456' # 密码
database = 'cefengta'


def update_Result(Result, ID, ID_can):
    Result.replace(np.nan, 'None', inplace=True)
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    num = 0
    for i in range(len(Result)):
        sql_update = "UPDATE cefengta.data_%s_clean set %s='%s' where Date_Time = '%s';" % (ID, ID_can, Result.loc[i, ID_can], Result.loc[i, 'Date_Time'])
        cursor.execute(sql_update.replace("'None'", "NULL"))
        num += 1
        if num > 1000:
            conn.commit()
            num = 0
    if num < 1000:
        conn.commit()
    cursor.close()
    conn.close()

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


def corr_interpolatin(data_ID, ID_can, data_yuan_ID, yuan_ID_can, start_time, end_time, savename):
    data_ID = data_ID[data_ID[ID_can] != ' ']
    data_ID[ID_can] = data_ID[ID_can].replace('None', np.nan).astype('float')
    data_yuan_ID = data_yuan_ID[data_yuan_ID[yuan_ID_can] != ' ']
    data_yuan_ID[yuan_ID_can] = data_yuan_ID[yuan_ID_can].replace('None', np.nan).astype('float')

    Data_corr = pd.merge(data_ID, data_yuan_ID, how='outer', on='Date_Time')
    Data_corr1 = Data_corr.copy()
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
    if r_squared > 0.8:
        Result = pd.DataFrame()
        Result['Date_Time'] = pd.date_range(start=start_time, end=end_time, freq='10T')
        Result['Date_Time'] = Result['Date_Time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        Result = pd.merge(Result, Data_corr1, how='left', on='Date_Time')
        Result[ID_can] = Result.apply(lambda x: cal_corr_data(x[ID_can], x[yuan_ID_can], a, b), axis=1)
        Result[ID_can] = Result[ID_can].apply(lambda x: np.around(x, 3))
        # result = {}
        # result['Date_Time'] = Result['Date_Time'].values.tolist()
        # result[ID_can] = Result[ID_can].values.tolist()
        # return result
        update_Result(Result, ID, ID_can)
    else:
        result = {}
        result['error'] = 'error 0.8'
        json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
        with open(savename, 'w') as f:
            f.write(json_str)
        # return result


def read_data_from_sql(ID, ID_can, yuan_ID, yuan_ID_can):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT Date_Time, %s FROM cefengta.data_%s_clean;" % (ID_can, ID))
    col_name_list_ID = [tuple[0] for tuple in cursor.description]
    values_ID = cursor.fetchall()
    data_ID = pd.DataFrame(values_ID)
    data_ID.columns = col_name_list_ID

    cursor.execute("SELECT Date_Time, %s FROM cefengta.data_%s_clean;" % (yuan_ID_can, yuan_ID))
    col_name_list_yuan_ID = [tuple[0] for tuple in cursor.description]
    values_yuan_ID = cursor.fetchall()
    data_yuan_ID = pd.DataFrame(values_yuan_ID)
    data_yuan_ID.columns = col_name_list_yuan_ID
    cursor.close()
    conn.close()
    return data_ID, data_yuan_ID

def read_data_from_sql_cols(ID, ID_can, cans, start_time, end_time):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT Date_Time, %s, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (
    ID_can, cans, ID, start_time, end_time))
    # # 获取查询结果
    # 获取表头
    col_name_list = [tuple[0] for tuple in cursor.description]
    values = cursor.fetchall()
    data = pd.DataFrame(values)
    data.columns = col_name_list
    cursor.close()
    conn.close()
    return data


def cal_shear_data(can_data, ID_can_id, yuan_can_data, yuan_ID_can_id, a):
    if np.isnan(can_data):
        if np.isnan(yuan_can_data):
            return can_data
        else:
            return yuan_can_data * np.power(ID_can_id / yuan_ID_can_id, a)
    else:
        return can_data

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


def cal_shear_every(data, ceng_list, yuan_ID_can_id):
    ceng = np.array(ceng_list)
    wind = np.array([float(xx) for xx in data])
    power_func = lambda x, c, a: c * np.power(x, a)
    params, cov = curve_fit(power_func, wind, ceng, maxfev=3000)
    return np.around(np.around(params[0], 3) * np.power(yuan_ID_can_id, np.around(params[1], 3)), 3)


def shear_interpolatin(data, ID_can, height, yuan_ID_can_id, start_time, end_time):
    # 每一个时刻的风切变，随机性太强
    # data_can = data[cans.split(',')]
    # data[ID_can] = data_can.apply(lambda x: cal_shear_every(x, height, yuan_ID_can_id), axis=1)
    # 计算总体的风切变
    c, a = cal_shear_all(data, height)
    cal_data = data[['Date_Time', ID_can, str(yuan_ID_can_id) + '_WS_AVG']]
    cal_data[ID_can] = cal_data[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')

    cal_data[str(yuan_ID_can_id) + '_WS_AVG'] = cal_data[str(yuan_ID_can_id) + '_WS_AVG'].replace(' ', np.nan).replace('None', np.nan).astype('float')
    Result = pd.DataFrame()
    Result['Date_Time'] = pd.date_range(start=start_time, end=end_time, freq='10T')
    Result['Date_Time'] = Result['Date_Time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    Result = pd.merge(Result, cal_data, how='left', on='Date_Time')
    Result[ID_can] = Result.apply(lambda x: cal_shear_data(x[ID_can], int(ID_can.split('_')[0]), x[str(yuan_ID_can_id) + '_WS_AVG'], yuan_ID_can_id, a), axis=1)
    Result[ID_can] = Result[ID_can].apply(lambda x: np.around(x, 3))
    # result = {}
    # result['Date_Time'] = Result['Date_Time'].values.tolist()
    # result[ID_can] = Result[ID_can].values.tolist()
    # return result
    update_Result(Result, ID, ID_can)

def read_data_from_sql_k(ID, ID_can, start_time, end_time, yuan_ID, yuan_ID_can, yuan_start_time, yuan_end_time):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute("SELECT Date_Time, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (ID_can, ID, start_time, end_time))
    col_name_list_ID = [tuple[0] for tuple in cursor.description]
    values_ID = cursor.fetchall()
    data_ID = pd.DataFrame(values_ID)
    data_ID.columns = col_name_list_ID

    cursor.execute("SELECT Date_Time, %s FROM cefengta.data_%s_clean where Date_Time >= '%s' and Date_Time <='%s';" % (yuan_ID_can, yuan_ID, yuan_start_time, yuan_end_time))
    col_name_list_yuan_ID = [tuple[0] for tuple in cursor.description]
    values_yuan_ID = cursor.fetchall()
    data_yuan_ID = pd.DataFrame(values_yuan_ID)
    data_yuan_ID.columns = col_name_list_yuan_ID
    cursor.close()
    conn.close()
    return data_ID, data_yuan_ID

def cal_k_data(can_data, yuan_can_data, k):
    if np.isnan(can_data):
        if np.isnan(yuan_can_data):
            return can_data
        else:
            return k*yuan_can_data
    else:
        return can_data


def cal_copy_data(can_data, yuan_can_data):
    if np.isnan(can_data):
        if np.isnan(yuan_can_data):
            return can_data
        else:
            return yuan_can_data
    else:
        return can_data

def cal_mean_data(can_data, mean_data_yuan):
    if np.isnan(can_data):
        return mean_data_yuan
    else:
        return can_data

def data_imputation(arg1="null",arg2="null",arg3="null",arg4="null",arg5="null",arg6="null",arg7="null",arg8="null",arg9="null",arg10="null",arg11="null",):
    cal_type = arg1
    if cal_type == 1:
        # type 被插补测风塔编号，插补开始，结束时间，被插补通道名称，插补数据源测风塔编号，插补数据源通道名称，存储名称路径
        # ID = 'M003470'
        # start_time = '2022-05-13 22:30:00'
        # end_time = '2022-05-14 22:30:00'
        # ID_can = '100_WS_AVG'
        # yuan_ID = 'M003470'
        # yuan_ID_can = '90_WS_AVG'
        # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
        ID = arg2
        start_time = arg3.replace('_', ' ')
        end_time = arg4.replace('_', ' ')
        ID_can = arg5
        yuan_ID = arg6
        yuan_ID_can = arg7
        savename = arg8
        data_ID, data_yuan_ID = read_data_from_sql(ID, ID_can, yuan_ID, yuan_ID_can)
        corr_interpolatin(data_ID, ID_can, data_yuan_ID, yuan_ID_can, start_time, end_time, savename)
        # json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
        # with open(savename, 'w') as f:
        #     f.write(json_str)
    elif cal_type == 2:
        # type 被插补测风塔编号 插补开始 结束时间 被插补通道名称 插补数据源通道名称（可多选，塔号为被插补测风塔编号）存储名称路径
        # ID = 'M003470'
        # start_time = '2022-05-07 22:30:00'
        # end_time = '2022-05-14 22:30:00'
        # ID_can = '100_WS_AVG'
        # cans = '90_WS_AVG,70_WS_AVG,50_WS_AVG'
        # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
        ID = arg2
        start_time = arg3.replace('_', ' ')
        end_time = arg4.replace('_', ' ')
        ID_can = arg5
        cans = arg6
        # savename = arg7
        if ID_can in cans:
            list = cans.split(',')
            list.remove(ID_can)
            cans = ','.join(list)
        height = [int(x.split('_')[0]) for x in cans.split(',')]
        yuan_ID_can_id = min(height, key=lambda x: abs(x - int(ID_can.split('_')[0])))
        data = read_data_from_sql_cols(ID, ID_can, cans, start_time, end_time)
        shear_interpolatin(data, ID_can, height, yuan_ID_can_id, start_time, end_time)
        # json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
        # with open(savename, 'w') as f:
        #     f.write(json_str)
    elif cal_type == 3:
        # type 被插补测风塔编号 插补开始 插补结束时间 被插补通道名称 插补数据源测风塔编号 插补数据源通道名称 插补数据源开始 插补数据源结束时间，比值K 存储名称路径
        # ID = 'M003470'
        # start_time = '2022-05-13 22:30:00'
        # end_time = '2022-05-14 22:30:00'
        # ID_can = '100_WS_AVG'
        # yuan_ID = 'M003470'
        # yuan_ID_can = '90_WS_AVG'
        # yuan_start_time = '2022-05-13 22:30:00'
        # yuan_end_time = '2022-05-14 22:30:00'
        # k = 0.9
        # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
        ID = arg2
        start_time = arg3.replace('_', ' ')
        end_time = arg4.replace('_', ' ')
        ID_can = arg5
        yuan_ID = arg6
        yuan_ID_can = arg7
        yuan_start_time = arg8.replace('_', ' ')
        yuan_end_time = arg9.replace('_', ' ')
        k = float(arg10)
        savename = arg11

        time_1 = pd.date_range(start=start_time, end=end_time, freq='10T')
        time_2 = pd.date_range(start=yuan_start_time, end=yuan_end_time, freq='10T')
        if len(time_1) == len(time_2):
            data_ID, data_yuan_ID = read_data_from_sql_k(ID, ID_can, start_time, end_time, yuan_ID, yuan_ID_can,
                                                         yuan_start_time, yuan_end_time)
            Result = pd.DataFrame()
            Result['Date_Time'] = data_ID['Date_Time']
            Result[ID_can] = data_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
            Result[yuan_ID_can] = data_yuan_ID[yuan_ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
            Result[ID_can] = Result.apply(lambda x: cal_k_data(x[ID_can], x[yuan_ID_can], k), axis=1)
            Result[ID_can] = Result[ID_can].apply(lambda x: np.around(x, 3))
            result_json = {}
            result_json['Date_Time'] = Result['Date_Time'].values.tolist()
            result_json[ID_can] = Result[ID_can].values.tolist()
            update_Result(Result, ID, ID_can)
        else:
            result_json = {}
            result_json['error'] = 'error'
            json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
            with open(savename, 'w') as f:
                f.write(json_str)
    elif cal_type == 4:
        # type 被插补测风塔编号 插补开始 插补结束时间 被插补通道名称 插补数据源开始 插补数据源结束时间 存储名称路径
        # ID = 'M003470'
        # start_time = '2022-05-13 22:30:00'
        # end_time = '2022-05-14 22:30:00'
        # ID_can = '100_WS_AVG'
        # yuan_start_time = '2022-05-12 22:30:00'
        # yuan_end_time = '2022-05-13 22:30:00'
        # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
        ID = arg2
        start_time = arg3.replace('_', ' ')
        end_time = arg4.replace('_', ' ')
        ID_can = arg5
        yuan_start_time = arg6.replace('_', ' ')
        yuan_end_time = arg7.replace('_', ' ')
        savename = arg8
        time_1 = pd.date_range(start=start_time, end=end_time, freq='10T')
        time_2 = pd.date_range(start=yuan_start_time, end=yuan_end_time, freq='10T')
        if len(time_1) == len(time_2):
            data_ID, data_yuan_ID = read_data_from_sql_k(ID, ID_can, start_time, end_time, ID, ID_can, yuan_start_time,
                                                         yuan_end_time)
            Result = pd.DataFrame()
            Result['Date_Time'] = data_ID['Date_Time']
            Result[ID_can] = data_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
            Result['yuan'] = data_yuan_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
            Result[ID_can] = Result.apply(lambda x: cal_copy_data(x[ID_can], x['yuan']), axis=1)
            Result[ID_can] = Result[ID_can].apply(lambda x: np.around(x, 3))
            # result_json = {}
            # result_json['Date_Time'] = Result['Date_Time'].values.tolist()
            # result_json[ID_can] = Result[ID_can].values.tolist()
            update_Result(Result, ID, ID_can)
        else:
            result_json = {}
            result_json['error'] = 'error'
            json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
            with open(savename, 'w') as f:
                f.write(json_str)
    else:
        # type 被插补测风塔编号 插补开始 插补结束时间 被插补通道名称 插补数据源开始 插补数据源结束时间 存储名称路径
        # ID = 'M003470'
        # start_time = '2022-05-13 22:30:00'
        # end_time = '2022-05-14 22:30:00'
        # ID_can = '100_WS_AVG'
        # yuan_start_time = '2022-05-12 22:30:00'
        # yuan_end_time = '2022-05-13 22:30:00'
        # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
        ID = arg2
        start_time = arg3.replace('_', ' ')
        end_time = arg4.replace('_', ' ')
        ID_can = arg5
        yuan_start_time = arg6.replace('_', ' ')
        yuan_end_time = arg7.replace('_', ' ')
        savename = arg8
        time_1 = pd.date_range(start=start_time, end=end_time, freq='10T')
        time_2 = pd.date_range(start=yuan_start_time, end=yuan_end_time, freq='10T')
        if len(time_1) == len(time_2):
            data_ID, data_yuan_ID = read_data_from_sql_k(ID, ID_can, start_time, end_time, ID, ID_can, yuan_start_time,
                                                         yuan_end_time)
            data_yuan_ID[ID_can] = data_yuan_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
            mean_yuan = np.nanmean(data_yuan_ID[ID_can])
            Result = pd.DataFrame()
            Result['Date_Time'] = data_ID['Date_Time']
            Result[ID_can] = data_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
            Result[ID_can] = Result.apply(lambda x: cal_mean_data(x[ID_can], mean_yuan), axis=1)
            Result[ID_can] = Result[ID_can].apply(lambda x: np.around(x, 3))
            # result_json = {}
            # result_json['Date_Time'] = Result['Date_Time'].values.tolist()
            # result_json[ID_can] = Result[ID_can].values.tolist()
            update_Result(Result, ID, ID_can)
        else:
            result_json = {}
            result_json['error'] = 'error'
            json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
            with open(savename, 'w') as f:
                f.write(json_str)


if __name__ == '__main__':
    # type（1：相关性，2：风切变，3：比值，4：前后，5：前后平均）
    # cal_type = int(5)
    import subprocess
    import warnings
    warnings.filterwarnings("ignore")

    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cal_type = int(sys.argv[1])
        if cal_type == 1:
            # type 被插补测风塔编号，插补开始，结束时间，被插补通道名称，插补数据源测风塔编号，插补数据源通道名称，存储名称路径
            # ID = 'M003470'
            # start_time = '2022-05-13 22:30:00'
            # end_time = '2022-05-14 22:30:00'
            # ID_can = '100_WS_AVG'
            # yuan_ID = 'M003470'
            # yuan_ID_can = '90_WS_AVG'
            # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
            ID = sys.argv[2]
            start_time = sys.argv[3].replace('_', ' ')
            end_time = sys.argv[4].replace('_', ' ')
            ID_can = sys.argv[5]
            yuan_ID = sys.argv[6]
            yuan_ID_can = sys.argv[7]
            savename = sys.argv[8]
            data_ID, data_yuan_ID = read_data_from_sql(ID, ID_can, yuan_ID, yuan_ID_can)
            corr_interpolatin(data_ID, ID_can, data_yuan_ID, yuan_ID_can, start_time, end_time, savename)
            # json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
            # with open(savename, 'w') as f:
            #     f.write(json_str)
        elif cal_type == 2:
            # type 被插补测风塔编号 插补开始 结束时间 被插补通道名称 插补数据源通道名称（可多选，塔号为被插补测风塔编号）存储名称路径
            # ID = 'M003470'
            # start_time = '2022-05-07 22:30:00'
            # end_time = '2022-05-14 22:30:00'
            # ID_can = '100_WS_AVG'
            # cans = '90_WS_AVG,70_WS_AVG,50_WS_AVG'
            # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
            ID = sys.argv[2]
            start_time = sys.argv[3].replace('_', ' ')
            end_time = sys.argv[4].replace('_', ' ')
            ID_can = sys.argv[5]
            cans = sys.argv[6]
            # savename = sys.argv[7]
            if ID_can in cans:
                list = cans.split(',')
                list.remove(ID_can)
                cans = ','.join(list)
            height = [int(x.split('_')[0]) for x in cans.split(',')]
            yuan_ID_can_id = min(height, key=lambda x: abs(x - int(ID_can.split('_')[0])))
            data = read_data_from_sql_cols(ID, ID_can, cans, start_time, end_time)
            shear_interpolatin(data, ID_can, height, yuan_ID_can_id, start_time, end_time)
            # json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
            # with open(savename, 'w') as f:
            #     f.write(json_str)
        elif cal_type == 3:
            # type 被插补测风塔编号 插补开始 插补结束时间 被插补通道名称 插补数据源测风塔编号 插补数据源通道名称 插补数据源开始 插补数据源结束时间，比值K 存储名称路径
            # ID = 'M003470'
            # start_time = '2022-05-13 22:30:00'
            # end_time = '2022-05-14 22:30:00'
            # ID_can = '100_WS_AVG'
            # yuan_ID = 'M003470'
            # yuan_ID_can = '90_WS_AVG'
            # yuan_start_time = '2022-05-13 22:30:00'
            # yuan_end_time = '2022-05-14 22:30:00'
            # k = 0.9
            # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
            ID = sys.argv[2]
            start_time =sys.argv[3].replace('_', ' ')
            end_time = sys.argv[4].replace('_', ' ')
            ID_can = sys.argv[5]
            yuan_ID = sys.argv[6]
            yuan_ID_can = sys.argv[7]
            yuan_start_time = sys.argv[8].replace('_', ' ')
            yuan_end_time = sys.argv[9].replace('_', ' ')
            k = float(sys.argv[10])
            savename = sys.argv[11]

            time_1 = pd.date_range(start=start_time, end=end_time, freq='10T')
            time_2 = pd.date_range(start=yuan_start_time, end=yuan_end_time, freq='10T')
            if len(time_1) == len(time_2):
                data_ID, data_yuan_ID = read_data_from_sql_k(ID, ID_can, start_time, end_time, yuan_ID, yuan_ID_can, yuan_start_time, yuan_end_time)
                Result = pd.DataFrame()
                Result['Date_Time'] = data_ID['Date_Time']
                Result[ID_can] = data_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
                Result[yuan_ID_can] = data_yuan_ID[yuan_ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
                Result[ID_can] = Result.apply(lambda x: cal_k_data(x[ID_can], x[yuan_ID_can], k), axis=1)
                Result[ID_can] = Result[ID_can].apply(lambda x: np.around(x, 3))
                result_json = {}
                result_json['Date_Time'] = Result['Date_Time'].values.tolist()
                result_json[ID_can] = Result[ID_can].values.tolist()
                update_Result(Result, ID, ID_can)
            else:
                result_json = {}
                result_json['error'] = 'error'
                json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
                with open(savename, 'w') as f:
                    f.write(json_str)
        elif cal_type == 4:
            # type 被插补测风塔编号 插补开始 插补结束时间 被插补通道名称 插补数据源开始 插补数据源结束时间 存储名称路径
            # ID = 'M003470'
            # start_time = '2022-05-13 22:30:00'
            # end_time = '2022-05-14 22:30:00'
            # ID_can = '100_WS_AVG'
            # yuan_start_time = '2022-05-12 22:30:00'
            # yuan_end_time = '2022-05-13 22:30:00'
            # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
            ID = sys.argv[2]
            start_time = sys.argv[3].replace('_', ' ')
            end_time = sys.argv[4].replace('_', ' ')
            ID_can = sys.argv[5]
            yuan_start_time = sys.argv[6].replace('_', ' ')
            yuan_end_time = sys.argv[7].replace('_', ' ')
            savename = sys.argv[8]
            time_1 = pd.date_range(start=start_time, end=end_time, freq='10T')
            time_2 = pd.date_range(start=yuan_start_time, end=yuan_end_time, freq='10T')
            if len(time_1) == len(time_2):
                data_ID, data_yuan_ID = read_data_from_sql_k(ID, ID_can, start_time, end_time, ID, ID_can, yuan_start_time, yuan_end_time)
                Result = pd.DataFrame()
                Result['Date_Time'] = data_ID['Date_Time']
                Result[ID_can] = data_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
                Result['yuan'] = data_yuan_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
                Result[ID_can] = Result.apply(lambda x: cal_copy_data(x[ID_can], x['yuan']), axis=1)
                Result[ID_can] = Result[ID_can].apply(lambda x: np.around(x, 3))
                # result_json = {}
                # result_json['Date_Time'] = Result['Date_Time'].values.tolist()
                # result_json[ID_can] = Result[ID_can].values.tolist()
                update_Result(Result, ID, ID_can)
            else:
                result_json = {}
                result_json['error'] = 'error'
                json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
                with open(savename, 'w') as f:
                    f.write(json_str)
        else:
            # type 被插补测风塔编号 插补开始 插补结束时间 被插补通道名称 插补数据源开始 插补数据源结束时间 存储名称路径
            # ID = 'M003470'
            # start_time = '2022-05-13 22:30:00'
            # end_time = '2022-05-14 22:30:00'
            # ID_can = '100_WS_AVG'
            # yuan_start_time = '2022-05-12 22:30:00'
            # yuan_end_time = '2022-05-13 22:30:00'
            # savename = '/home/xiaowu/share/202311/测风塔系统/接口/imputation.json'
            ID = sys.argv[2]
            start_time = sys.argv[3].replace('_', ' ')
            end_time = sys.argv[4].replace('_', ' ')
            ID_can = sys.argv[5]
            yuan_start_time = sys.argv[6].replace('_', ' ')
            yuan_end_time = sys.argv[7].replace('_', ' ')
            savename = sys.argv[8]
            time_1 = pd.date_range(start=start_time, end=end_time, freq='10T')
            time_2 = pd.date_range(start=yuan_start_time, end=yuan_end_time, freq='10T')
            if len(time_1) == len(time_2):
                data_ID, data_yuan_ID = read_data_from_sql_k(ID, ID_can, start_time, end_time, ID, ID_can, yuan_start_time,
                                                             yuan_end_time)
                data_yuan_ID[ID_can] = data_yuan_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
                mean_yuan = np.nanmean(data_yuan_ID[ID_can])
                Result = pd.DataFrame()
                Result['Date_Time'] = data_ID['Date_Time']
                Result[ID_can] = data_ID[ID_can].replace(' ', np.nan).replace('None', np.nan).astype('float')
                Result[ID_can] = Result.apply(lambda x: cal_mean_data(x[ID_can], mean_yuan), axis=1)
                Result[ID_can] = Result[ID_can].apply(lambda x: np.around(x, 3))
                # result_json = {}
                # result_json['Date_Time'] = Result['Date_Time'].values.tolist()
                # result_json[ID_can] = Result[ID_can].values.tolist()
                update_Result(Result, ID, ID_can)
            else:
                result_json = {}
                result_json['error'] = 'error'
                json_str = simplejson.dumps(result_json, indent=4, ignore_nan=True)
                with open(savename, 'w') as f:
                    f.write(json_str)








