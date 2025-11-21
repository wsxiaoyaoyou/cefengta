import sys
import pymysql
import subprocess
host = 'localhost'
port = 3306
user = 'root' #用户名
password = '123456' # 密码
database = 'cefengta'


def delete_turbine(cefengta_id):
    try:
        conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset='utf8mb4')
        cursor = conn.cursor()
        if cursor.execute("SHOW TABLES LIKE 'channel_configuration';"):
            db_delete_ = "DELETE FROM cefengta.channel_configuration WHERE ID = '%s';" % (cefengta_id)
            cursor.execute(db_delete_)
            conn.commit()
        if cursor.execute("SHOW TABLES LIKE 'dynamic_information';"):
            db_delete_ = "DELETE FROM cefengta.dynamic_information WHERE ID = '%s';" % (cefengta_id)
            cursor.execute(db_delete_)
            conn.commit()
        if cursor.execute("SHOW TABLES LIKE 'data_%s_yuanshi';" % cefengta_id):
            db_delete_ = "DROP TABLE data_%s_yuanshi;" % (cefengta_id)
            cursor.execute(db_delete_)
            conn.commit()
        if cursor.execute("SHOW TABLES LIKE 'data_%s_clean';" % cefengta_id):
            db_delete_ = "DROP TABLE data_%s_clean;" % (cefengta_id)
            cursor.execute(db_delete_)
            conn.commit()
        if cursor.execute("SHOW TABLES LIKE 'static_information';"):
            db_delete_ = "DELETE FROM cefengta.static_information WHERE ID = '%s';" % (cefengta_id)
            cursor.execute(db_delete_)
            conn.commit()
        cursor.close()
        conn.close()
        print('success')
    except:
        print('error')

if __name__ == '__main__':
    # cefengta_id = 'M000001'
    # delete_cefeng_table(cefengta_id)
    #python3 delete_turbine.py M000001
    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_ID = sys.argv[1]
        delete_turbine(cefengta_ID)
