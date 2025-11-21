import pandas as pd
import pymysql
import subprocess

host = 'localhost'
port = 3306
user = 'wyh' #用户名
password = 'Wyh123!@#' # 密码
database = 'cefengta'

def ID_cefengta():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
    cursor = conn.cursor()
    # 删除数据
    sql_select = "SELECT ID, MAILUSER, MAILCODE, DATATYPE FROM cefengta.static_information where DATATYPE = '1' or DATATYPE = '2';"
    cursor.execute(sql_select)
    p_value = cursor.fetchall()
    cursor.close()
    conn.close()
    result = pd.DataFrame()
    if len(p_value) > 0:
        result = pd.DataFrame(p_value)
        result.columns = ['ID', 'MAILUSER', 'MAILCODE', 'DATATYPE']
        result = result[(result['MAILUSER'] != '-99') & (result['MAILCODE'] != '-99')]
        result.reset_index(inplace=True, drop=True)
    return result


if __name__ == '__main__':
    result = ID_cefengta()
    # print(result)
    if len(result) > 0:
        for i in range(len(result)):
            cefengta_ID = result.loc[i, 'ID']
            subprocess.run('./insert_dynamic_information %s' % cefengta_ID, shell=True)
