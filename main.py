#! /usr/bin/python3
# -*- coding: utf-8 -*-
import os.path
import shutil
import datetime
from flask import Flask, g, jsonify, redirect, url_for, make_response, render_template, send_from_directory
from flask import request, jsonify, Response, stream_with_context
from flask import send_file, session, render_template, redirect
import sql_service
import write_yuanshi_data,read_channel_from_file,download_analysis,delete_turbine,scatter_analysis
import yearmaxwind_analysis_v1,yearmaxwind_analysis_v2,shear_analysis,daily_analysis,generation_analysis
import read_timeseries,data_imputation,data_clean_rule,download_from_email,clean_imputation_data
import frequency_analysis,rose_analysis,turbulence_analysis,weibull_analysis
from flask_cors import CORS
import subprocess
import json
import time
import secrets
import pandas as pd
import simplejson
from datetime import timedelta
import io



path_file = {}
path_file['根目录'] = 'D:\cefengta\\res\mast'
path_file['结果暂存'] = 'D:\cefengta\\res'
# 'D:\cefengta\\res\mast/'

# 实例化，可视为固定格式
app = Flask(__name__)
CORS(app, supports_credentials=True)
app.secret_key = 'login'


@app.route('/', methods=["POST","GET"])
def hello():

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "OK"
    })

@app.route('/tower_server/atm/mast/mastChannelSave', methods=["POST","OPTIONS"])
def channel_save():

    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()
    insert_type = sql_service.channel_save(params['channels'])

    if insert_type == 0:
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    else:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/mast/mastDataSave', methods=["GET","OPTIONS"])
def data_save():

    if request.method == 'OPTIONS':
        return response()

    params = request.args

    file_path = path_file['根目录'] + '\\' + params['ID']
    try:
        write_yuanshi_data.write_yuanshi_data(file_path, params['DATATYPE'])
        # shutil.rmtree(file_path)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/mast/mastDelete', methods=["GET","OPTIONS"])
def mast_delete():
    if request.method == 'OPTIONS':
        return response()
    params = request.args
    file_path = path_file['根目录'] + '\\' + params['ID']
    try:
        if os.path.exists(file_path):
            shutil.rmtree(file_path)
        delete_turbine.delete_turbine(params['ID'])
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    except:
        return jsonify({
            "code": 400,
            "msg": "请求处理失败",
            "data": "null"
        })

@app.route('/tower_server/atm/mast/mastChannelGet', methods=["GET","OPTIONS"])
def channel_get():

    if request.method == 'OPTIONS':
        return response()

    params = request.args
    resdpath = path_file['根目录'] + '\\' + params['ID']
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y%m%d%H%M%S%f') + '_' + params['ID'] + '.json'

    # data = sql_service.channel_get(params) 获取通道配置表
    try:
        # 修改程序及其入参
        read_channel_from_file.read_channel_from_file(params['ID'], resdpath, savename,  params['DATATYPE'])

        with open(savename, 'r', encoding='utf-8') as file:
            # 加载JSON数据
            data = json.load(file)
        data = pd.DataFrame.from_dict(data, orient='index').to_json(orient="records", force_ascii=False)
        data = json.loads(data)
        # os.remove(savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": data
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/analysis/scatter', methods=["GET","OPTIONS"])
def scatterAnalysis():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_scatter.json'
    try:
        # 修改程序及其入参
        if 'WS' in params['type1']:
            can1 = params['type1'].split('_')[0] + 'm风速'
            can2 = params['type2'].split('_')[0] + 'm风速'
        else:
            can1 = params['type1'].split('_')[0] + 'm风向'
            can2 = params['type2'].split('_')[0] + 'm风向'
        scatter_analysis.scatter_analysis(params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), can1, can2, savename)
        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)

            return jsonify({
                "code": 200,
                "msg": "请求已经成功处理",
                "data": token
            })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/analysis/yearmaxwind50', methods=["GET","OPTIONS"])
def yearmaxwind50_analysis():

    if request.method == 'OPTIONS':
        return response()

    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_50yearmaxwind.json'
    can1 = params['channel'].split('_')[0] + 'm风速'
    try:
        # 修改程序及其入参
        if params['analysisType'] == "1":
            # 如果选择这一项要记得数据至少满足一年
            yearmaxwind_analysis_v1.yearmaxwind_analysis_v1(params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), savename)
        elif params['analysisType'] == "2":
            yearmaxwind_analysis_v2.yearmaxwind_analysis_v2(params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), savename)
        else:
            return jsonify({
                "code": 206,
                "msg": "参数错误",
                "data": "null"
            })

        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": token
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/analysis/shearanalysis', methods=["GET","OPTIONS"])
def shearAnalysis():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_shear.json'
    try:
        # 修改程序及其入参
        if 'height' in params.keys():
            shear_analysis.shear_analysis(
            params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), savename, params['height'].replace(',', '_'))
        else:
            shear_analysis.shear_analysis(params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), savename)
        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": token
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/mast/mastDataShow', methods=["POST","OPTIONS"])
def show_mastdata():

    if request.method == 'OPTIONS':
        return response()
    params = request.json
    Items,total = sql_service.show_mastdata(params)

    data = {
        "mastItems": Items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/mast/showMap', methods=["GET","OPTIONS"])
def show_map():

    if request.method == 'OPTIONS':
        return response()
    params = request.args

    mastItems, mastTotal = sql_service.show_map(params)

    data = {
        "mastItems": mastItems,
        "mastTotal": mastTotal
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/mast/showPage', methods=["POST","OPTIONS"])
def showMastPage():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()

    items, total = sql_service.showMastPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/warn/showPage', methods=["POST","OPTIONS"])
def showWarnPage():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()

    items, total = sql_service.showWarnPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/warnTemp/showPage', methods=["POST","OPTIONS"])
def showWarnTempPage():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()

    items, total = sql_service.showWarnTempPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/warn/delete', methods=["GET","OPTIONS"])
def delete_warn():
    if request.method == 'OPTIONS':
        return response()

    sql_service.delete_warn(request.args)


    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/warnTemp/delete', methods=["GET", "OPTIONS"])
def delete_warnTemp():
    if request.method == 'OPTIONS':
        return response()

    sql_service.delete_warnTemp(request.args)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/warn/add', methods=["POST","OPTIONS"])
def add_warn():
    if request.method == 'OPTIONS':
        return response()

    insert_type = sql_service.add_warn(request.json)
    if insert_type == 0:
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    else:
        return jsonify({
            "code": 206,
            "msg": "输入参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/warnTemp/add', methods=["POST","OPTIONS"])
def add_warnTemp():
    if request.method == 'OPTIONS':
        return response()

    insert_type = sql_service.add_warnTemp(request.json)
    if insert_type == 0:
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    else:
        return jsonify({
            "code": 206,
            "msg": "输入参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/warn/edit', methods=["POST","OPTIONS"])
def edit_warn():
    if request.method == 'OPTIONS':
        return response()

    sql_service.edit_warn(request.json)
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/warnTemp/edit', methods=["POST","OPTIONS"])
def edit_warnTemp():
    if request.method == 'OPTIONS':
        return response()

    sql_service.edit_warnTemp(request.json)
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/warn/list', methods=["GET","OPTIONS"])
def list_warn():
    if request.method == 'OPTIONS':
        return response()

    data = sql_service.list_warn()
    data = data.split(',')

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/analysis/dailyanalysis', methods=["GET","OPTIONS"])
def dailyanalysis_analysis():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_dailyanalysis.json'
    can1 = params['channel'].split('_')[0] + 'm风速'
    if params['time'] == '月况':
        time = '月况'
    else:
        time = '年况'
    if params['wtype'] == '风速':
        wtype = '风速'
    else:
        wtype = '风功率'

    try:
        # 修改程序及其入参
        daily_analysis.daily_analysis(params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), time, wtype, savename)
        if params['time'] == '年况':
            with open(savename, 'r') as f:
                data = json.load(f)
            result_data = data['year']
            with open(savename, 'w') as f:
                simplejson.dump(result_data, f)

        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": token
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/analysis/generation', methods=["GET","OPTIONS"])
def generationAnalysis():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_generation.json'
    can1 = params['channel'].split('_')[0] + 'm风速'
    time = params['time']

    try:
        # 修改程序及其入参

        generation_analysis.generation_analysis(params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), '6.25_193', '1.0', time, savename)
        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": token
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/getUpidData', methods=["GET","OPTIONS"])
def getUpidData():

    if request.method == 'OPTIONS':
        return response()
    params = request.args['upid']
    savename= sql_service.get_savename(params)

    if savename == "null":
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": '处理失败'
        })
    try:
        with open(savename, 'r', encoding='utf-8') as file:
            # 加载JSON数据
            data = json.load(file)

        # os.remove(savename)

        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": data
        })
    except:
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": '正在查询'
        })

@app.route('/tower_server/atm/channel/list', methods=["GET","OPTIONS"])
def list_channel():
    if request.method == 'OPTIONS':
        return response()
    params = request.args

    data = sql_service.list_channel(params)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/warn/historyShowPage', methods=["POST","OPTIONS"])
def showWarnHistoryPage():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()

    items, total = sql_service.showWarnHistoryPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/warn/handle', methods=["GET","OPTIONS"])
def warn_handle():
    if request.method == 'OPTIONS':
        return response()
    params = request.args['warnIDs']

    sql_service.warn_handle(params)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/warn/mainPage', methods=["GET","OPTIONS"])
def warn_mainPage():
    if request.method == 'OPTIONS':
        return response()
    params = request.args

    items, total = sql_service.warn_mainPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/warnTemp/mast', methods=["GET","OPTIONS"])
def warnTemp_mast():
    if request.method == 'OPTIONS':
        return response()


    sql_service.warnTemp_mast(request.args)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/analysis/timeseries', methods=["GET","OPTIONS"])
def timeseries_analysis():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_timeseries.json'

    # 通道输入格式修改
    can1 = sql_service.cal_name_CHANNELNAME(params['type'])
    can1 = can1.replace('高度','')
    try:
        # 修改程序及其入参
        read_timeseries.read_timeseries(params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), savename)
        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": token
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/project/list', methods=["GET","OPTIONS"])
def list_project():
    if request.method == 'OPTIONS':
        return response()


    data = sql_service.list_project()
    data = data.split(',')


    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/mast/add', methods=["POST","OPTIONS"])
def add_mast():
    if request.method == 'OPTIONS':
        return response()
    insert_type,data = sql_service.add_mast(request.json)
    if insert_type == 0:
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": data
        })
    else:
        return jsonify({
            "code": 1,  # 这个要确认下code咋定义，
            "msg": "该项目已存在",
            "data": "null"
        })

@app.route('/tower_server/atm/mast/edit', methods=["POST","OPTIONS"])
def edit_mast():
    if request.method == 'OPTIONS':
        return response()
    sql_service.edit_mast(request.json)
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/mast/showPageTrue', methods=["POST","OPTIONS"])
def showTureMastPage():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()

    items, total = sql_service.showMastPageTrue(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/mast/listTrue', methods=["GET","OPTIONS"])
def list_mast():
    if request.method == 'OPTIONS':
        return response()
    params = request.args

    keys,values = sql_service.list_mastTrue(params)
    keys = keys.split(',')
    values = values.split(',')
    data = dict(zip(keys,values))
    # data = json.dumps(data)
    # data = pd.DataFrame.from_dict(data, orient='index').to_json(orient="records", force_ascii=False)
    # data = json.loads(data)


    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/user/add', methods=["POST","OPTIONS"])
def add_user():
    if request.method == 'OPTIONS':
        return response()
    params = request.get_json()
    insert_type = sql_service.add_user(params)
    if insert_type == 0:
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    else:
        return jsonify({
            "code": 1, #这个要确认下code咋定义，
            "msg": "该用户名已存在",
            "data": "null"
        })

@app.route('/tower_server/atm/user/edit', methods=["POST","OPTIONS"])
def edit_user():
    if request.method == 'OPTIONS':
        return response()
    params = request.get_json()
    sql_service.edit_user(params)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/user/showPage', methods=["POST","OPTIONS"])
def showUserPage():

    if request.method == 'OPTIONS':
        return response()
    params = request.get_json()

    items, total = sql_service.showUserPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/user/delete', methods=["GET","OPTIONS"])
def delete_user():
    if request.method == 'OPTIONS':
        return response()
    data = sql_service.delete_user(request.args)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/login', methods=["POST","GET"])
def login():
    if request.method == 'OPTIONS':
        return response()
    params = request.get_json()
    typeid, result = sql_service.select_user(params)
    # username = params['username']
    # password = params['password']

    if typeid == 1 or typeid == 2:
        if 'login_attempts' not in session:
            session['login_attempts'] = 1
        else:
            session['login_attempts'] += 1
            if session['login_attempts'] >= 5:
                return jsonify({
                    "code": 402,
                    "msg": "账户已经锁定，请稍后再试。",
                    "data": "null"
                })
        return jsonify({
            "code": 401,
            "msg": "没有被授权或者授权已经失效",
            "data": "null"
        })

    session['login_attempts'] = 0
    timestamp = int(time.time())
    token = get_token()
    userInfo = {
        "username": result['username'].values[0],
        "realName": result['realName'].values[0],
        "organize":  result['organize'].values[0],
        "userType":  int(result['userType'].values[0])
    }
    sql_service.write_token(result['username'].values[0], token)
    data = {
        "accessToken": token,
        "timeStamp": timestamp,
        "userInfo": userInfo
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/user/info', methods=["GET","OPTIONS"])
def userinfo():
    if request.method == 'OPTIONS':
        return response()
    token = request.headers.get('Authorization')

    typeid, result = sql_service.userinfo(token)

    if typeid ==1:
        data = {
            "username": result['username'].values[0],
            "realName": result['realName'].values[0],
            "organize":  result['organize'].values[0],
            "userType":  int(result['userType'].values[0])
        }

        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": data
        })

    else:
        return jsonify({
            "code": 401,
            "msg": "没有被授权或者授权已经失效"
        })

@app.route('/tower_server/atm/user/organizelist', methods=["GET","OPTIONS"])
def list_organize():
    if request.method == 'OPTIONS':
        return response()
    data = sql_service.list_organize()
    data = data.split(',')

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/interpolation', methods=["POST", "OPTIONS"])
def interpolation():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime(
        '%Y-%m-%d-%H-%M-%S-%f') + '_interpolation.json'
    try:
        # 修改程序及其入参
        if params['type'] == 1:
            data_imputation.data_imputation(
                params['type'], params['ID'], params['startDate'].replace(' ', '_'),
                params['endDate'].replace(' ', '_'),
                params['channelName'], params['sourceID'], params['sourceChannelName'], savename)
        elif params['type'] == 2:
            data_imputation.data_imputation(
                params['type'], params['ID'], params['startDate'].replace(' ', '_'),
                params['endDate'].replace(' ', '_'),
                params['channelName'], params['sourceChannelName'], savename)
        elif params['type'] == 3:
            data_imputation.data_imputation(
                params['type'], params['ID'], params['startDate'].replace(' ', '_'),
                params['endDate'].replace(' ', '_'),
                params['channelName'], params['sourceID'], params['sourceChannelName'],
                params['sourceStartDate'].replace(' ', '_'), params['sourceEndDate'].replace(' ', '_'), params['K'],
                savename)
        elif params['type'] == 4:
            data_imputation.data_imputation(
                params['type'], params['ID'], params['startDate'].replace(' ', '_'),
                params['endDate'].replace(' ', '_'),
                params['channelName'], params['sourceStartDate'].replace(' ', '_'),
                params['sourceEndDate'].replace(' ', '_'),
                savename)
        elif params['type'] == 5:
            data_imputation.data_imputation(
                params['type'], params['ID'], params['startDate'].replace(' ', '_'),
                params['endDate'].replace(' ', '_'),
                params['channelName'], params['sourceStartDate'].replace(' ', '_'),
                params['sourceEndDate'].replace(' ', '_'),
                savename)
        else:

            return jsonify({
                "code": 206,
                "msg": "参数错误",
                "data": "null"
            })

        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)

            return jsonify({
                "code": 200,
                "msg": "请求已经成功处理",
                "data": token
            })
        else:
            return jsonify({
                "code": 200,
                "msg": "请求已经成功处理",
                "data": "null"
            })


    except:

        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/analysis/statistics', methods=["POST","OPTIONS"])
def statistics():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()
    items, total = sql_service.statistics(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route("/tower_server/atm/analysis/download", methods=["POST","OPTIONS"])
def downloadAnalysis():
    if request.method == 'OPTIONS':
        return response()
    params = request.get_json()

    try:
        # 修改程序及其入参
        if params['form'] == 'WT':
            savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime(
                '%Y-%m-%d-%H-%M-%S-%f') + '_interpolation.tim'
            download_analysis.download_analysis(
                params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'),
                params['source'],
                params['form'], savename, params['type1'], params['type2'])
            return send_file(savename, as_attachment=True)
            # return Response(get_binary_io(savename), mimetype='test/plain')
        elif params['form'] == 'WAsP':
            savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime(
                '%Y-%m-%d-%H-%M-%S-%f') + '_interpolation.txt'
            download_analysis.download_analysis(
                params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'),
                params['source'],
                params['form'], savename, params['type1'], params['type2'])
            return send_file(savename, as_attachment=True)
            # return Response(get_binary_io(savename), mimetype='test/plain')
        elif params['form'] == '时间序列':
            savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime(
                '%Y-%m-%d-%H-%M-%S-%f') + '_interpolation.txt'
            download_analysis.download_analysis(
            params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), params['source'],
            params['form'], savename, params['type'])
            return send_file(savename, as_attachment=True)
            # return Response(get_binary_io(savename), mimetype='test/plain')
        elif params['form'] == 'WindSim':
            savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime(
                '%Y-%m-%d-%H-%M-%S-%f') + '_interpolation.tws'
            download_analysis.download_analysis(
            params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), params['source'],
            params['form'], savename, params['type1'], params['type2'])
            return send_file(savename, as_attachment=True)
            # return Response(get_binary_io(savename), mimetype='test/plain')
        elif params['form'] == 'WAsP':
            savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime(
                '%Y-%m-%d-%H-%M-%S-%f') + '_interpolation.tab'
            download_analysis.download_analysis(
            params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), params['source'],
            params['form'], savename, params['type1'], params['type2'])
            return send_file(savename, as_attachment=True)
            # return Response(get_binary_io(savename), mimetype='test/plain')
        else:
            savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_interpolation.xlsx'
            download_analysis.download_analysis(params['ID'], params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), params['source'],params['form'], savename)
            return send_file(savename, as_attachment=True)
            # return Response(get_binary_io(savename), mimetype='application/vnd.ms-excel;charset=utf-8')


    except:
        return jsonify({
            "code": 206,
            "msg": "参数不符合要求",
            "data": "null"
        })

@app.route('/tower_server/atm/verifyTemp/mastManual', methods=["GET", "OPTIONS"])
def mastManualVerify():
    if request.method == 'OPTIONS':
        return response()
    params = request.args
    TID = sql_service.update_verifyTempID(params['ID'], params['verifyRuleIDs'])
    try:

        data_clean_rule.data_clean_rule(params['ID'])
        # sql_service.callback_verifyTempID(params['ID'], TID)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": 'null'
        })
    except:
        # sql_service.callback_verifyTempID(params['ID'], TID)
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/mast/downloadManual', methods=["GET", "OPTIONS"])
def mastManualDownload():
    if request.method == 'OPTIONS':
        return response()

    params = request.args
    start_date = datetime.strptime(params['startDate'],'%Y-%m-%d')
    end_date = datetime.strptime(params['endDate'],'%Y-%m-%d')
    date_list = []
    for x in range((end_date - start_date).days + 1):
        date = start_date + timedelta(days=x)
        date = date.strftime('%Y-%m-%d')
        date_list.append(date)
    try:
        for date in date_list:
            download_from_email.download_from_email(path_file['根目录'], date)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": 'null'
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/analysis/correlation', methods=["POST","OPTIONS"])
def correlation():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()
    data = sql_service.cal_correlation(params['ID'], params['channelName'], params['sourceID'],
                                       params['sourceChannelName'], params['startDate'], params['endDate'])
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/analysis/adjust', methods=["GET", "OPTIONS"])
def adjust():
    if request.method == 'OPTIONS':
        return response()

    params = request.args
    sql_service.adjust_data(params['ID'], params['type'], params['OFF'], params['SCALE'])
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": 'null'
    })

@app.route('/tower_server/atm/analysis/verticalExtra', methods=["GET", "OPTIONS"])
def vertical():
    if request.method == 'OPTIONS':
        return response()

    params = request.args
    sql_service.verticalExtra_data(params['ID'], params['type'], params['startDate'], params['endDate'], params['HEIGHT'])
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": 'null'
    })

@app.route('/tower_server/atm/verify/resultShowPage', methods=["POST","OPTIONS"])
def showVerifyResultPage():

    if request.method == 'OPTIONS':
        return response()
    params = request.get_json()

    items, total = sql_service.showVRPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

@app.route('/tower_server/atm/analysis/oneButton', methods=["POST", "OPTIONS"])
def oneButton():
    if request.method == 'OPTIONS':
        return response()

    params = request.json['ID']

    try:
        clean_imputation_data.clean_imputation_data(params)

        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

@app.route('/tower_server/atm/cleanTemp/mast', methods=["GET","OPTIONS"])
def cleanTemp_mast():
    if request.method == 'OPTIONS':
        return response()


    sql_service.cleanTemp_mast(request.args)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

@app.route('/tower_server/atm/clearRules/describe', methods=["POST","OPTIONS"])
def cleanRules_describe():
    if request.method == 'OPTIONS':
        return response()

    sql_service.rules_verify_subjection(request.json)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

# 修改程序及其入参
@app.route('/tower_server/atm/analysis/frequencyanalysis', methods=["GET","OPTIONS"])
def frequencyAnalysis():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_frequencyanalysis.json'

    can1 = sql_service.cal_name_CHANNELNAME(params['channel'])
    can1 = can1.replace('高度','')
    try:
        # 修改程序及其入参
        frequency_analysis.frequency_analysis(
        params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), savename)
        with open(savename, 'r') as f:
            data = json.load(f)
        result_data = {}
        for key_i in ['WPD', 'wind']:
            xAxisData = []
            seriesData = []
            for key in data[key_i].keys():
                xAxisData.append(key)
                seriesData.append(data[key_i][key])
            result_data_i = {}
            result_data_i['xAxisData'] = xAxisData
            result_data_i['seriesData'] = seriesData
            result_data[key_i] = result_data_i
        with open(savename, 'w') as f:
            simplejson.dump(result_data, f)
        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": token
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

# 修改程序及其入参
@app.route('/tower_server/atm/analysis/getrose', methods=["GET","OPTIONS"])
def getrose():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_getrose.json'

    can1 = sql_service.cal_name_CHANNELNAME(params['channel'])
    can1 = can1.replace('高度','')
    can2 = sql_service.cal_name_CHANNELNAME(params['channe2'])
    can2 = can2.replace('高度', '')
    try:
        # 修改程序及其入参
        if 'bin' in params.keys():
            bin = params['bin']
        else:
            bin = '16'
        rose_analysis.rose_analysis(
        params['ID'], can1, can2, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'),
        params['time'], params['type'], bin, savename)
        if params['type'] == '月况':
            with open(savename, 'r') as f:
                data = json.load(f)
            xAxisData = []
            seriesData = []
            for key in data.keys():
                xAxisData.append(key)
                seriesData.append(data[key])
            result_data={}
            result_data['xAxisData'] = xAxisData
            result_data['seriesData'] = seriesData
            with open(savename, 'w') as f:
                simplejson.dump(result_data, f)
        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": token
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

# 修改程序及其入参
@app.route('/tower_server/atm/analysis/turbulence', methods=["GET","OPTIONS"])
def turbulence():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_turbulence.json'

    can1 = sql_service.cal_name_CHANNELNAME(params['channel'])
    can1 = can1.replace('高度','')
    try:
        # 修改程序及其入参
        # 注：选择风向时需要包含缺省参数，若不包含则缺省参数为16
        if 'bin' not in params.keys():
            turbulence_analysis.turbulence_analysis(params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), '风速', savename)
        else:
            turbulence_analysis.turbulence_analysis(
            params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), '风速', savename, params['bin'])


        if os.path.exists(savename):
            token = get_token()
            write = sql_service.write_job(token, savename)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": token
        })
    except:
        return jsonify({
            "code": 206,
            "msg": "参数错误",
            "data": "null"
        })

# 修改程序及其入参
@app.route('/tower_server/atm/analysis/weibullanalysis', methods=["GET","OPTIONS"])
def weibullAnalysis():

    if request.method == 'OPTIONS':
        return response()
    params = request.args
    savename = path_file['结果暂存'] + '\\' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f') + '_weibullanalysis.json'

    can1 = sql_service.cal_name_CHANNELNAME(params['channel'])
    can1 = can1.replace('高度','')
    # try:
    # 修改程序及其入参
    # 与间隔参数对应的柱状图数值与拟合折线图数值
    if 'bin' in params.keys():
        bin = params['bin']
    else:
        bin = '1'
    weibull_analysis.weibull_analysis(
    params['ID'], can1, params['startDate'].replace(' ', '_'), params['endDate'].replace(' ', '_'), bin, savename)

    if os.path.exists(savename):
        token = get_token()
        write = sql_service.write_job(token, savename)
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": token
    })
    # except:
    #     return jsonify({
    #         "code": 206,
    #         "msg": "参数错误",
    #         "data": "null"
    #     })

# 修改程序
@app.route('/tower_server/atm/mast/delmastchannel', methods=["POST","OPTIONS"])
def delmastchannel():
    if request.method == 'OPTIONS':
        return response()
    params = request.json

    try:
        sql_service.deldelmastchannel(params)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    except:
        return jsonify({
            "code": 400,
            "msg": "请求处理失败",
            "data": "null"
        })

# 修改程序
@app.route('/tower_server/atm/mast/detail', methods=["POST", "OPTIONS"])
def detail():
    if request.method == 'OPTIONS':
        return response()
    params = request.json

    try:
        data = sql_service.detail(params)

        list = []
        list_legen = []
        for i in range(1, 11):
            if data['MONTHWS' + str(i)] != None:
                list.append(data['MONTHWS' + str(i)].split(',')[1:])
                list_legen.append(data['MONTHWS' + str(i)].split(',')[0])
        axis = []
        for i in range(1, 13):
            axis.append(str(int(i)))
        monthws = {"name": "YueJunFengSu",
                   "seriesData":list,
                   "legendData":list_legen,
                   "xAxisData":axis}
        data["monthws"]=monthws

        list = []
        list_legen = []
        for i in range(1, 11):
            if data['DAYWS' + str(i)] != None:
                list.append(data['DAYWS' + str(i)].split(',')[1:])
                list_legen.append(data['DAYWS' + str(i)].split(',')[0])
        axis = []
        for i in range(0, 24):
            axis.append(str(int(i)))
        dayws = {"name": "RiJunFengSu",
                   "seriesData":list,
                   "legendData":list_legen,
                   "xAxisData": axis}
        data["dayws"]=dayws

        list = []
        list_legen = []
        for i in range(1, 11):
            if data['WSDIS' + str(i)] != None:
                list.append(data['WSDIS' + str(i)].split(',')[1:])
                list_legen.append(data['WSDIS' + str(i)].split(',')[0])
        axis = []
        for i in range(1, 51):
            axis.append(str(int(i) * 0.5))
        wsdis = {"name": "WSDIS",
                 "seriesData": list,
                 "legendData": list_legen,
                 "xAxisData": axis}
        data["wsdis"] = wsdis

        list = []
        list_legen = []
        for i in range(1, 11):
            if data['WPDIS' + str(i)] != None:
                list.append(data['WPDIS' + str(i)].split(',')[1:])
                list_legen.append(data['WPDIS' + str(i)].split(',')[0])
        axis = []
        for i in range(1, 51):
            axis.append(str(int(i) * 0.5))
        wpdis = {"name": "WPDIS",
                 "seriesData": list,
                 "legendData": list_legen,
                 "xAxisData": axis}
        data["wpdis"] = wpdis

        list = []
        list_legen = []
        for i in range(1, 4):
            if data['WD' + str(i)] != None:
                list.append(data['WD' + str(i)].split(',')[1:])
                list_legen.append(data['WD' + str(i)].split(',')[0])
        wd = {   "seriesData": list,
                 "xAxisData": list_legen}
        data["wd"] = wd

        list = []
        list_legen = []
        for i in range(1, 4):
            if data['WP' + str(i)] != None:
                list.append(data['WP' + str(i)].split(',')[1:])
                list_legen.append(data['WP' + str(i)].split(',')[0])
        wp = {"seriesData": list,
              "xAxisData": list_legen}
        data["wp"] = wp


        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": data
        })
    except:
        return jsonify({
            "code": 400,
            "msg": "请求处理失败",
            "data": "null"
        })

# 修改程序
@app.route('/tower_server/atm/mast/editmastchannel', methods=["POST", "OPTIONS"])
def editmastchannel():
    if request.method == 'OPTIONS':
        return response()
    params = request.json

    try:
        sql_service.editmastchannel(params)
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    except:
        return jsonify({
            "code": 400,
            "msg": "请求处理失败",
            "data": "null"
        })

@app.route('/tower_server/atm/mast/mastfile', methods=["POST","OPTIONS"])
def mastfile():
    if request.method == 'OPTIONS':
        return response()
    file = request.files['file[0]']
    ID = request.form.get('ID')
    file_path = path_file['根目录'] + '\\' + ID
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    file.save(file_path + '\\' + file.filename)
    return jsonify({
		"code": 200,
		"msg": "请求已经成功处理",
		"data": "null"
    })

# 修改程序
@app.route('/tower_server/atm/rule/delrule', methods=["GET","OPTIONS"])
def delrule():
    if request.method == 'OPTIONS':
        return response()

    sql_service.delete_rule(request.args)


    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

# 修改程序
@app.route('/tower_server/atm/rule/showPage', methods=["POST","OPTIONS"])
def ruleshowPage():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()

    items, total = sql_service.showrulePage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

# 修改程序
@app.route('/tower_server/atm/rule/addrule', methods=["POST","OPTIONS"])
def add_rule():
    if request.method == 'OPTIONS':
        return response()

    insert_type = sql_service.add_rule(request.json)
    if insert_type == 0:
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    else:
        return jsonify({
            "code": 206,
            "msg": "输入参数错误",
            "data": "null"
        })

# 修改程序
@app.route('/tower_server/atm/rule/editrule', methods=["POST","OPTIONS"])
def edit_rule():
    if request.method == 'OPTIONS':
        return response()

    sql_service.edit_rule(request.json)
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

# 修改程序
@app.route('/tower_server/atm/templet/deltemplet', methods=["GET", "OPTIONS"])
def deldeltemplet():
    if request.method == 'OPTIONS':
        return response()

    sql_service.delete_ruleTemp(request.args)

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

# 修改程序
@app.route('/tower_server/atm/templet/showPage', methods=["POST","OPTIONS"])
def showtempletPage():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()

    items, total = sql_service.showruleTempPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

# 修改程序
@app.route('/tower_server/atm/templet/addtemplet', methods=["POST","OPTIONS"])
def add_templet():
    if request.method == 'OPTIONS':
        return response()

    insert_type = sql_service.add_ruleTemp(request.json)
    if insert_type == 0:
        return jsonify({
            "code": 200,
            "msg": "请求已经成功处理",
            "data": "null"
        })
    else:
        return jsonify({
            "code": 206,
            "msg": "输入参数错误",
            "data": "null"
        })

# 修改程序
@app.route('/tower_server/atm/templet/edittemplet', methods=["POST","OPTIONS"])
def edit_templet():
    if request.method == 'OPTIONS':
        return response()

    sql_service.edit_ruleTemp(request.json)
    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": "null"
    })

# 修改程序
@app.route('/tower_server/atm/rule/rulelist', methods=["GET","OPTIONS"])
def list_rule():
    if request.method == 'OPTIONS':
        return response()

    data = sql_service.list_rule()
    data = data.split(',')

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

# 修改程序
@app.route('/tower_server/atm/templet/templetlist', methods=["GET","OPTIONS"])
def list_templet():
    if request.method == 'OPTIONS':
        return response()

    data = sql_service.list_ruleTemp()
    data = data.split(',')

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

# 修改程序
@app.route('/tower_server/atm/mast/channelShowPage', methods=["POST","OPTIONS"])
def showChannelPage():
    if request.method == 'OPTIONS':
        return response()

    params = request.get_json()

    items, total = sql_service.showChannelTempPage(params)

    data = {
        "items": items,
        "total": total
    }

    return jsonify({
        "code": 200,
        "msg": "请求已经成功处理",
        "data": data
    })

def response():
    response = make_response('', 204)
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    response.headers.add('Access-Control-Allow-Headers', '*')
    response.headers.add('Access-Control-Max-Age', 1728000)
    return response

def get_token():
    timestamp = int(time.time())
    token = secrets.token_hex(16)
    token = token + str(timestamp)
    return token

def get_binary_io(savename):
    with open(savename, 'rb') as file:
        bytes_io = file.read()
    return bytes_io


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
    # redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
