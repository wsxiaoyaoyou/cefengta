import poplib, email, datetime, time, traceback, sys, telnetlib, zipfile, os, shutil
from email.parser import Parser
from email.header import decode_header
from email.utils import parseaddr

import pandas as pd
import pymysql
host = 'localhost'
port = 3306
user = 'root' #用户名
password = '123456' # 密码
database = 'cefengta' #数据库名称

def email_login(email_user, password, pop3_server):
    # 连接到POP3服务器,有些邮箱服务器需要ssl加密，可以使用poplib.POP3_SSL
    server = poplib.POP3_SSL(pop3_server, 995, timeout=100)
    # server = poplib.POP3(pop3_server, 110, timeout=10)
    # server.set_debuglevel(1)
    # # 打印POP3服务器的欢迎文字:
    # print(server.getwelcome().decode('utf-8'))
    # 身份认证:
    server.user(email_user)
    server.pass_(password)
    # 返回邮件数量和占用空间:
    # print('Messages: %s. Size: %s' % server.stat())
    # list()返回所有邮件的编号:
    resp, mails, octets = server.list()
    return mails, server


def decode_str(str_in):
    """字符编码转换"""
    value, charset = decode_header(str_in)[0]
    if charset:
        value = value.decode(charset)
    return value


def save_att_file(msg, save_path_file):
    """附件下载函数"""
    # 判断测风塔ID在Subject就行，不用管附件名
    filename = ''
    for part in msg.walk():
        file_name = part.get_filename()
        if file_name:
            file_name = decode_str(file_name)
            # if cefengta_ID in file_name:
            # print(file_name) 要不要判断塔名在不在文件名里，
            # Subject = decode_str(parseaddr(msg.get('Subject'))[1])
            # save_path_file = save_path_all + Subject + '/'
            if not os.path.exists(save_path_file):
                os.makedirs(save_path_file)
            data = part.get_payload(decode=True)
            if not os.path.exists(save_path_file + file_name):
                att_file = open(save_path_file + file_name, 'wb')
                att_file.write(data)
                att_file.close()
                filename = file_name
    return filename


def main(email_user, password, pop3_server, save_path_all, yesterday, cefengta_ID, log=None):
    mails, server = email_login(email_user, password, pop3_server)
    # 遍历所有邮件
    name_list = []
    for i in range(1, len(mails) + 1):
        resp, lines, octets = server.retr(i)
        msg_content = b'\r\n'.join(lines).decode()
        # 解析邮件:
        msg = Parser().parsestr(msg_content)
        # From = parseaddr(msg.get('from'))[1]  # 发件人
        # To = parseaddr(msg.get('To'))[1]  # 收件人
        # Cc = parseaddr(msg.get_all('Cc'))[1]  # 抄送人
        Subject = decode_str(parseaddr(msg.get('Subject'))[1])  # 主题 邮件名称
        # 获取邮件时间,格式化收件时间
        # 邮件时间格式转换
        date2 = time.strftime("%Y-%m-%d", time.strptime(msg.get("Date")[0:24], '%a, %d %b %Y %H:%M:%S'))
        # 打印相关信息，辅助的
        # print(f'发件人：{From}；收件人：{To}；抄送人：{Cc}；主题：{Subject}；收件时间：{date2}')
        # 主题和日期验证所需邮件
        if (date2 == yesterday) & (cefengta_ID in Subject):
            filename = save_att_file(msg, save_path_all)
            if log != None:
                with open(log + '/' + yesterday + '.log', 'a') as file:
                    file.write('补录日期：' + datetime.date.today().strftime('%Y-%m-%d') + ',' + '补录编号：' + cefengta_ID + ',' + '补录文件：' + filename + '\n')
            if filename != '':
                name_list.append(filename)
    server.quit()
    return name_list


def main_dowmload(savepath, cefengta_ID, email_user, password, date_down=None):
    if email_user.split('@')[1] == '126.com':
        pop3_server = 'pop.126.com'
    else:
        pop3_server = 'pop.163.com'
    # 今天日期
    today = datetime.date.today()
    # 昨天日期
    if date_down == None:
        yesterday = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        yesterday = date_down
    save_path_all = savepath + '/' + cefengta_ID + '/'
    # 下载主程序
    if not date_down == None:
        name_list = main(email_user, password, pop3_server, save_path_all, yesterday, cefengta_ID)
    else:
        name_list = main(email_user, password, pop3_server, save_path_all, yesterday, cefengta_ID, savepath)
    return name_list


def ID_cefengta():
    conn = pymysql.connect(host=host, port=port, user=user, password=password1, database=database, charset='utf8mb4')
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

def insert_data_warn(cefengta_id):
    # # 链接数据库
    table_name = 'alarm_information'
    conn = pymysql.connect(host=host, port=port, user=user, password=password1, database=database, charset='utf8mb4')
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
    today = datetime.date.today()
    # 昨天日期
    yesterday = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    part_1 = "ID,NAME,channelID,warnTime,warnType,status"
    part_2 = "'" + cefengta_id + "','" + cefengtaname + "','" + '所有通道' + "','" + yesterday + "','" + '无数据' + "','" + '告警中' + "'"
    sql_insert = 'REPLACE INTO %s (%s) VALUES (%s);' % (table_name, part_1, part_2)
    cursor.execute(sql_insert)

    conn.commit()
    cursor.close()
    conn.close()

def download_from_email(savepath,date_down=None):

    result = ID_cefengta()
    if len(result) > 0:
        for i in range(len(result)):
            cefengta_ID = result.loc[i, 'ID']
            email_user = result.loc[i, 'MAILUSER']
            # 此处密码是授权码,用于登录第三方邮件客户端
            password = result.loc[i, 'MAILCODE']
            name_list = main_dowmload(savepath, cefengta_ID, email_user, password, date_down)
            datatype = result.loc[i, 'DATATYPE']
            if len(name_list) > 0:
                name_list = (',').join(name_list)
                # 解析nrg数据
                # write_nrg_data, 写入数据库就行 读取前一天数据并清洗
                # start_time = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
                # end_time = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                subprocess.run('./write_nrg_data %s %s %s' % (savepath + '/' + cefengta_ID, name_list, datatype),
                               shell=True)
                # # warning_data 读取前几天数据预警 write_log 数据入库日志
                # subprocess.run('./warning_data %s %s %s' % (cefengta_ID, start_time, end_time), shell=True)
            else:
                # error = 'warning, %s no data' % cefengta_ID
                insert_data_warn(cefengta_ID)
    else:
        print('NO')


if __name__ == '__main__':
    # savepath = '/home/xiaowu/share/202311/测风塔系统/test'
    # 输入邮件地址, 口令和POP3服务器地址:
    # 'sanxialijiang120@163.com' 202307
    import subprocess

    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        savepath = sys.argv[1]
        if len(sys.argv) > 2:
            date_down = sys.argv[2]
        else:
            date_down = None
        result = ID_cefengta()
        if len(result) > 0:
            for i in range(len(result)):
                cefengta_ID = result.loc[i, 'ID']
                email_user = result.loc[i, 'MAILUSER']
                # 此处密码是授权码,用于登录第三方邮件客户端
                password = result.loc[i, 'MAILCODE']
                name_list = main_dowmload(savepath, cefengta_ID, email_user, password, date_down)
                datatype = result.loc[i, 'DATATYPE']
                if len(name_list) > 0:
                    name_list = (',').join(name_list)
                    # 解析nrg数据
                    # write_nrg_data, 写入数据库就行 读取前一天数据并清洗
                    # start_time = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
                    # end_time = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                    subprocess.run('./write_nrg_data %s %s %s' % (savepath + '/' + cefengta_ID, name_list, datatype), shell=True)
                    # # warning_data 读取前几天数据预警 write_log 数据入库日志
                    # subprocess.run('./warning_data %s %s %s' % (cefengta_ID, start_time, end_time), shell=True)
                else:
                    # error = 'warning, %s no data' % cefengta_ID
                    insert_data_warn(cefengta_ID)
        else:
            print('NO')




