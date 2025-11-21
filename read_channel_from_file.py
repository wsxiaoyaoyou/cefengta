import os
import rarfile
import zipfile
from pathlib import Path
import pandas as pd
import simplejson
import numpy as np
import sys
import nrgpy
import pymysql
import shutil
import chardet


def cal_name_CHANNELtype(unit, type):
    if unit == 'm/s':
        can = 'WS'
    elif unit == 'Zm/s':
        can = 'ZWS'
    elif (unit == '°') | (unit == 'Deg') | (unit == 'deg'):
        can = 'WD'
    elif (unit == '°C') | (unit == 'C') | (unit == 'Deg C'):
        can = 'T'
    elif (unit == 'kPa') | (unit == 'hPa') | (unit == 'KPa') | (unit == 'mb') | (unit == 'mmHg')| (unit == 'Pa'):
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
        return can
    else:
        return np.nan


def cal_name_unit(unit, type):
    if (unit == 'm/s') | (unit == 'Zm/s'):
        can = 'ms'
    elif (unit == '°') | (unit == 'Deg') | (unit == 'deg'):
        can = 'angle'
    elif (unit == '°C') | (unit == 'C') | (unit == 'Deg C'):
        can = 'temp'
    elif (unit == 'kPa') | (unit == 'KPa'):
        can = 'kpa'
    elif unit == 'hPa':
        can = 'hPa'
    elif unit == 'Pa':
        can = 'Pa'
    elif unit == 'mb':
        can = 'mb'
    elif unit == 'mmHg':
        can = 'mmHg'
    elif unit == 'V':
        can = 'v'
    elif unit == '%':
        if type != 'other':
            can = 'rh'
        else:

            can = 'quality'
    else:
        can = ''
    if can != '':
        return can
    else:
        return np.nan


def cal_HIGHT_type(HIGHT, type):
    try:
        int(HIGHT)
    except:
        HIGHT = ''
    if (type == 'other') & (HIGHT == ''):
        HIGHT = ''
    else:
        HIGHT=str(int(HIGHT))
    return HIGHT

def read_biaotou_csv_leida(file_path):
    data = pd.read_csv(file_path, skiprows=8, encoding='GB2312')
    if '时间戳' in data.columns:
        type_csv = 'ch'
    else:
        type_csv = 'en'
    R = pd.DataFrame(columns=['ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check'])
    for i, col in enumerate(data.columns):
        R.loc[i, 'ORIGINCHANNEL'] = col
        R.loc[i, 'CHID'] = i
        # 高度
        if 'm' in col:
            try:
                R.loc[i, 'HIGHT'] = str(int(col.split('m')[0]))
            except:
                R.loc[i, 'HIGHT'] = '10'
        else:
            R.loc[i, 'HIGHT'] = '10'
        # 类型、单位
        if '(' in col:
            if type_csv == 'en':
                if 'Speed' in col:
                    if 'AverageverticalSpeed' in col:
                        R.loc[i, 'UNIT'] = 'Zm/s'
                    elif 'horizontal' in col:
                        R.loc[i, 'UNIT'] = 'm/s'
                elif 'Direction' in col:
                    R.loc[i, 'UNIT'] = '°'
                if 'mAverage' in col:
                    R.loc[i, 'type'] = 'AVG'
                elif 'mMin' in col:
                    R.loc[i, 'type'] = 'MIN'
                elif 'mMax' in col:
                    R.loc[i, 'type'] = 'MAX'
                elif 'mMean' in col:
                    if 'vertical' not in col:
                        R.loc[i, 'type'] = 'SD'
            else:
                if '风速' in col:
                    if '垂直风速(' in col:
                        R.loc[i, 'UNIT'] = 'Zm/s'
                    elif '水平风速' in col:
                        R.loc[i, 'UNIT'] = 'm/s'
                elif '风向' in col:
                    R.loc[i, 'UNIT'] = '°'
                elif '有效率' in col:
                    R.loc[i, 'UNIT'] = '%'
                if '值(' not in col:
                    if '有效率' not in col:
                        if '标准差' in col:
                            if '垂直' not in col:
                                R.loc[i, 'type'] = 'SD'
                        else:
                            R.loc[i, 'type'] = 'AVG'
                    else:
                        R.loc[i, 'type'] = 'other'
                elif '最小值' in col:
                    R.loc[i, 'type'] = 'MIN'
                elif '最大值' in col:
                    R.loc[i, 'type'] = 'MAX'
        elif (col == 'temperature') | (col == '温度'):
            R.loc[i, 'UNIT'] = '°C'
            R.loc[i, 'type'] = 'AVG'
        elif (col == 'humidity') | (col == '压力'):
            R.loc[i, 'UNIT'] = 'hPa'
            R.loc[i, 'type'] = 'AVG'
        elif (col == 'pressure') | (col == '湿度'):
            R.loc[i, 'UNIT'] = '%'
            R.loc[i, 'type'] = 'AVG'
        else:
            R.loc[i, 'type'] = 'other'
        R.loc[i, 'OFF'] = 0

        R.loc[i, 'SCALE'] = 0
    for i in range(len(R)):
        f = R.loc[(R['HIGHT'] == R.loc[i, 'HIGHT']) & (R['UNIT'] == R.loc[i, 'UNIT']) & (R['type'] == R.loc[i, 'type'])]
        if len(f) > 1:
            R.loc[i, 'check'] = 'check'
    R['CHANNELNAMETYPE'] = R.apply(lambda x: cal_name_CHANNELtype(x['UNIT'], x['type']), axis=1)
    key = (R['ORIGINCHANNEL'] == 'Time') | (R['ORIGINCHANNEL'] == '时间戳')
    R.loc[key, 'CHANNELNAMETYPE'] = 'time'
    R['UNIT'] = R.apply(lambda x: cal_name_unit(x['UNIT'], x['type']), axis=1)
    R['type'] = R['type'].apply(lambda x: str(x).upper() if str(x)!='other' else str(x))
    R['type'] = R['type'].apply(lambda x: 'other' if x == 'NAN' else x)
    R['HIGHT'] = R.apply(lambda x: cal_HIGHT_type(x['HIGHT'], x['type']), axis=1)
    return R


def read_biaotou_txt_leida(file_path):
    data = pd.read_csv(file_path, encoding='GB2312', skiprows=9, sep='\t')
    R = pd.DataFrame(columns=['ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check'])
    for i, col in enumerate(data.columns):
        R.loc[i, 'ORIGINCHANNEL'] = col
        R.loc[i, 'CHID'] = i
        # 高度
        if 'm' in col:
            try:
                R.loc[i, 'HIGHT'] = str(int(col.split('m')[0]))
            except:
                R.loc[i, 'HIGHT'] = '10'
        else:
            R.loc[i, 'HIGHT'] = '10'
        # 类型、单位
        if '(' in col:
            if '风速' in col:
                if 'z方向风速(' in col:
                    R.loc[i, 'UNIT'] = 'Zm/s'
                elif '水平风速' in col:
                    R.loc[i, 'UNIT'] = 'm/s'
            elif ('偏差' in col) & ('z方向' not in col):
                R.loc[i, 'UNIT'] = 'm/s'
            elif '风向' in col:
                R.loc[i, 'UNIT'] = '°'
            elif '可靠性' in col:
                R.loc[i, 'UNIT'] = '%'
            if '值(' not in col:
                if '可靠性' not in col:
                    if '偏差' in col:
                        if 'z方向' not in col:
                            R.loc[i, 'type'] = 'SD'
                    else:
                        R.loc[i, 'type'] = 'AVG'
                else:
                    R.loc[i, 'type'] = 'other'
            elif '最小值' in col:
                R.loc[i, 'type'] = 'MIN'
            elif '最大值' in col:
                R.loc[i, 'type'] = 'MAX'

        elif col == '外温':
            R.loc[i, 'UNIT'] = '°C'
            R.loc[i, 'type'] = 'AVG'
        elif col == '气压':
            R.loc[i, 'UNIT'] = 'hPa'
            R.loc[i, 'type'] = 'AVG'
        elif col == '湿度':
            R.loc[i, 'UNIT'] = '%'
            R.loc[i, 'type'] = 'AVG'
        else:
            R.loc[i, 'type'] = 'other'
        R.loc[i, 'OFF'] = 0

        R.loc[i, 'SCALE'] = 0
    for i in range(len(R)):
        f = R.loc[(R['HIGHT'] == R.loc[i, 'HIGHT']) & (R['UNIT'] == R.loc[i, 'UNIT']) & (R['type'] == R.loc[i, 'type'])]
        if len(f) > 1:
            R.loc[i, 'check'] = 'check'
    R['CHANNELNAMETYPE'] = R.apply(lambda x: cal_name_CHANNELtype(x['UNIT'], x['type']), axis=1)
    key = (R['ORIGINCHANNEL'] == 'Time') | (R['ORIGINCHANNEL'] == '时间戳')
    R.loc[key, 'CHANNELNAMETYPE'] = 'time'
    R['UNIT'] = R.apply(lambda x: cal_name_unit(x['UNIT'], x['type']), axis=1)
    R['type'] = R['type'].apply(lambda x: str(x).upper() if str(x)!='other' else str(x))
    R['type'] = R['type'].apply(lambda x: 'other' if x == 'NAN' else x)
    R['HIGHT'] = R.apply(lambda x: cal_HIGHT_type(x['HIGHT'], x['type']), axis=1)
    return R

def read_biaotou_csv(file_path):
    data = pd.read_csv(file_path, skiprows=1, nrows=2)
    R = pd.DataFrame(columns=['ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check'])
    for i, col in enumerate(data.columns):
        R.loc[i, 'ORIGINCHANNEL'] = col
        R.loc[i, 'CHID'] = i
        if '_' in col:
            if ('m' in col.split('_')[1]) & (data.iloc[1, i] in ['Avg', 'Std', 'Max', 'Min']):
                R.loc[i, 'HIGHT'] = col.split('_')[1][:-1]
            else:
                R.loc[i, 'HIGHT'] = '10'
            if ('m' in col.split('_')[1]) & (R.loc[i, 'HIGHT'] != col.split('_')[1][:-1]):
                R.loc[i, 'check'] = 'check'
        else:
            R.loc[i, 'type'] = 'other'
        if data.iloc[1, i] in ['Avg', 'Std', 'Max', 'Min']:
            # if data.iloc[0, i] == 'Deg C':
            #     R.loc[i, 'UNIT'] = '°C'
            # elif data.iloc[0, i] == 'Deg':
            #     R.loc[i, 'UNIT'] = '°'
            # else:
            #     R.loc[i, 'UNIT'] = data.iloc[0, i]
            R.loc[i, 'UNIT'] = data.iloc[0, i]
            R.loc[i, 'type'] = data.iloc[1, i]
        R.loc[i, 'OFF'] = 0

        R.loc[i, 'SCALE'] = 0
    for i in range(len(R)):
        f = R.loc[(R['HIGHT'] == R.loc[i, 'HIGHT']) & (R['UNIT'] == R.loc[i, 'UNIT']) & (R['type'] == R.loc[i, 'type'])]
        if len(f) > 1:
            R.loc[i, 'check'] = 'check'
    R['CHANNELNAMETYPE'] = R.apply(lambda x: cal_name_CHANNELtype(x['UNIT'], x['type']), axis=1)
    key = (R['ORIGINCHANNEL'] == 'Date & Time Stamp') | (R['ORIGINCHANNEL'] == 'Timestamp') | \
          (R['ORIGINCHANNEL'] == 'TIMESTAMP')
    R.loc[key, 'CHANNELNAMETYPE'] = 'time'
    R['UNIT'] = R.apply(lambda x: cal_name_unit(x['UNIT'], x['type']), axis=1)
    R['type'] = R['type'].apply(lambda x: str(x).upper() if str(x)!='other' else str(x))
    R['type'] = R['type'].apply(lambda x: 'other' if x == 'NAN' else x)
    R['type'] = R['type'].apply(lambda x: 'SD' if x == 'STD' else x)
    R['HIGHT'] = R.apply(lambda x: cal_HIGHT_type(x['HIGHT'], x['type']), axis=1)
    return R


def read_biaotou_rwd(file_path):
    with open(file_path, 'r') as f:
        data = f.readlines()
    break_line = 0
    for i in range(0, len(data)):
        if data[i][:2] == '20':
            break_line = i - 1
            break
    line = 0
    Result_biaotou = {}
    while line < break_line:
        if data[line].startswith('Channel'):
            result = {}
            CHID = data[line].split('	')[-1].replace('\n', '')
            if CHID in Result_biaotou.keys():
                del Result_biaotou[CHID]
            p = 1
            while (data[line + p][:7] != 'Channel') & (line + p < break_line):
                if data[line + p ].startswith('HIGHT'):
                    result['HIGHT'] = data[line + p].split('	')[1].split('   ')[0].replace('\n', '').replace('m', '')
                elif data[line + p ].startswith('Units'):
                    result['UNIT'] = data[line + p].split('	')[-1].replace('\n', '')
                elif data[line + p].startswith('Offset'):
                    result['OFF'] = data[line + p].split('	')[-1].replace('\n', '')
                elif data[line + p].startswith('Description	'):
                    result['Name'] = data[line + p].split('Description	')[-1].replace('\n', '')
                elif data[line + p].startswith('Scale Factor'):
                    result['SCALE'] = data[line + p].split('	')[-1].replace('\n', '')
                p = p + 1
            else:
                line = line + p
            Result_biaotou[CHID] = result
        else:
            line = line + 1
    R = pd.DataFrame(columns=['ORIGINCHANNEL', 'ORIGINCHANNEL_use', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check'])
    data = pd.read_csv(file_path, skiprows=break_line, nrows=2, sep='\t')
    for i, col in enumerate(data.columns):
        if col.startswith('CH'):
            R.loc[i, 'ORIGINCHANNEL_use'] = col
            if col.endswith('SD'):
                key = col[2:-2]
                R.loc[i, 'type'] = 'SD'
                R.loc[i, 'ORIGINCHANNEL'] = Result_biaotou[key]['Name'].replace(' ', '_') + '_' + 'SD'
            else:
                key = col[2:-3]
                R.loc[i, 'type'] = col[-3:]
                R.loc[i, 'ORIGINCHANNEL'] = Result_biaotou[key]['Name'].replace(' ', '_') + '_' + col[-3:]
            R.loc[i, 'CHID'] = key
            R.loc[i, 'HIGHT'] = Result_biaotou[key]['HIGHT']
            try:
                R.loc[i, 'OFF'] = Result_biaotou[key]['OFF']
                R.loc[i, 'SCALE'] = Result_biaotou[key]['SCALE']
            except:
                lof = 'no off'
            R.loc[i, 'UNIT'] = Result_biaotou[key]['UNIT']
        else:
            R.loc[i, 'ORIGINCHANNEL_use'] = col
            R.loc[i, 'ORIGINCHANNEL'] = col
            R.loc[i, 'type'] = 'other'
    R.loc[R['UNIT'] == '-----', 'type'] = 'other'
    R= R.replace(['------', '-----'], '')
    # R['UNIT'] = R['UNIT'].replace('deg', '°')
    # R['UNIT'] = R['UNIT'].replace(['C'], '°C')
    # R['UNIT'] = R['UNIT'].replace(['Volts'], 'V')
    R.loc[(R['UNIT'] != '') & (R['HIGHT'] == ''), 'HIGHT'] = '10'
    for i in range(len(R)):
        f = R.loc[(R['HIGHT'] == R.loc[i, 'HIGHT']) & (R['UNIT'] == R.loc[i, 'UNIT']) & (R['type'] == R.loc[i, 'type'])]
        if len(f) > 1:
            R.loc[i, 'check'] = 'check'
            R.loc[i, 'OFF'] = '0'
    R['CHANNELNAMETYPE'] = R.apply(lambda x: cal_name_CHANNELtype(x['UNIT'], x['type']), axis=1)
    key = (R['ORIGINCHANNEL'] == 'Date & Time Stamp') | (R['ORIGINCHANNEL'] == 'Timestamp') | \
          (R['ORIGINCHANNEL'] == 'TIMESTAMP')
    R.loc[key, 'CHANNELNAMETYPE'] = 'time'
    R['UNIT'] = R.apply(lambda x: cal_name_unit(x['UNIT'], x['type']), axis=1)
    R['type'] = R['type'].apply(lambda x: str(x).upper() if str(x)!='other' else str(x))
    R['type'] = R['type'].apply(lambda x: 'other' if x == 'NAN' else x)
    R['HIGHT'] = R.apply(lambda x: cal_HIGHT_type(x['HIGHT'], x['type']), axis=1)
    return R


def read_biaotou_rld(file_path):
    with open(file_path, 'r') as f:
        data = f.readlines()
    break_line = 0
    for i in range(0, len(data)):
        if data[i][:2] == '20':
            break_line = i - 1
            break
    line = 0
    Result_biaotou = {}
    while line < break_line:
        if data[line].startswith('Channel'):
            result = {}
            CHID = data[line].split('	')[-1].replace('\n', '')
            if CHID in Result_biaotou.keys():
                del Result_biaotou[CHID]
            p = 1
            while (data[line + p][:7] != 'Channel') & (line + p < break_line):
                # if data[line + p].startswith('HIGHT'):
                #     result['HIGHT'] = data[line + p].split('	')[1].split('   ')[0].replace('\n', '').replace('m', '')
                # elif data[line + p].startswith('Units'):
                #     result['UNIT'] = data[line + p].split('	')[-1].replace('\n', '')
                if data[line + p].startswith('Offset'):
                    result['OFF'] = data[line + p].split('	')[-1].replace('\n', '')
                # elif data[line + p].startswith('Description	'):
                #     result['Name'] = data[line + p].split('Description	')[-1].replace('\n', '')
                elif data[line + p].startswith('Scale Factor'):
                    result['SCALE'] = data[line + p].split('	')[-1].replace('\n', '')
                p = p + 1
            else:
                line = line + p
            Result_biaotou[CHID] = result
        else:
            line = line + 1
    R = pd.DataFrame(columns=['ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check'])
    data = pd.read_csv(file_path, skiprows=break_line, nrows=2, sep='\t')
    for i, col in enumerate(data.columns):
        if col.startswith('Ch'):
            R.loc[i, 'ORIGINCHANNEL'] = col
            R.loc[i, 'CHID'] = col.split('_')[0][2:]
            R.loc[i, 'HIGHT'] = col.split('_')[2].replace('.00m', '')
            try:
                R.loc[i, 'OFF'] = Result_biaotou[col.split('_')[0][2:]]['OFF']
                R.loc[i, 'SCALE'] = Result_biaotou[col.split('_')[0][2:]]['SCALE']
            except:
                lof = 'no off'
            R.loc[i, 'UNIT'] = col.split('_')[-1]
            R.loc[i, 'type'] = col.split('_')[-2]
        else:

            R.loc[i, 'ORIGINCHANNEL'] = col
            R.loc[i, 'type'] = 'other'
    # R.loc[R['UNIT'] == '-----', 'type'] = 'other'
    # R= R.replace(['------', '-----'], '')
    # R['UNIT'] = R['UNIT'].replace('deg', '°')
    # R['UNIT'] = R['UNIT'].replace(['C'], '°C')
    # R['UNIT'] = R['UNIT'].replace(['Volts'], 'V')
    R.loc[(R['UNIT'] != '') & (R['HIGHT'] == ''), 'HIGHT'] = 10
    for i in range(len(R)):
        f = R.loc[(R['HIGHT'] == R.loc[i, 'HIGHT']) & (R['UNIT'] == R.loc[i, 'UNIT']) & (R['type'] == R.loc[i, 'type'])]
        if len(f) > 1:
            R.loc[i, 'check'] = 'check'
            R.loc[i, 'OFF'] = '0'
    R['CHANNELNAMETYPE'] = R.apply(lambda x: cal_name_CHANNELtype(x['UNIT'], x['type']), axis=1)
    key = (R['ORIGINCHANNEL'] == 'Date & Time Stamp') | (R['ORIGINCHANNEL'] == 'Timestamp') | \
          (R['ORIGINCHANNEL'] == 'TIMESTAMP')
    R.loc[key, 'CHANNELNAMETYPE'] = 'time'
    R['UNIT'] = R.apply(lambda x: cal_name_unit(x['UNIT'], x['type']), axis=1)
    R['type'] = R['type'].apply(lambda x: str(x).upper() if str(x)!='other' else str(x))
    R['type'] = R['type'].apply(lambda x: 'other' if x == 'NAN' else x)
    R['HIGHT'] = R.apply(lambda x: cal_HIGHT_type(x['HIGHT'], x['type']), axis=1)
    return R


def read_biaotou_leida(file_path):
    data = pd.read_csv(file_path)
    R = pd.DataFrame(columns=['ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check'])
    for i, col in enumerate(data.columns):
        R.loc[i, 'ORIGINCHANNEL'] = col
        R.loc[i, 'CHID'] = i
        if ('[' in col) & (col != 'precipitation[0-1]'):
            if '_' not in col:
                R.loc[i, 'HIGHT'] = '10'
                R.loc[i, 'UNIT'] = col.split('[')[1].split(']')[0]
                R.loc[i, 'type'] = 'AVG'
            elif 'measure' in col:
                R.loc[i, 'HIGHT'] = col.split('_')[2]
                R.loc[i, 'UNIT'] = col.split('_')[3].split('[')[1].split(']')[0]
                R.loc[i, 'type'] = 'other'
            elif 'horz' in col:
                R.loc[i, 'HIGHT'] = col.split('_')[1]
                R.loc[i, 'UNIT'] = col.split('_')[3].split('[')[1].split(']')[0]
                R.loc[i, 'type'] = col.split('_')[3].split('[')[0]
            elif 'vert' in col:
                R.loc[i, 'HIGHT'] = col.split('_')[1]
                R.loc[i, 'UNIT'] = 'Z' + col.split('_')[3].split('[')[1].split(']')[0]
                R.loc[i, 'type'] = col.split('_')[3].split('[')[0]
            else:
                R.loc[i, 'HIGHT'] = col.split('_')[1]
                R.loc[i, 'UNIT'] = col.split('_')[2].split('[')[1].split(']')[0]
                R.loc[i, 'type'] = col.split('_')[2].split('[')[0]

        else:
            R.loc[i, 'type'] = 'other'
        R.loc[i, 'OFF'] = 0
        R.loc[i, 'SCALE'] = 0
    for i in range(len(R)):
        f = R.loc[(R['HIGHT'] == R.loc[i, 'HIGHT']) & (R['UNIT'] == R.loc[i, 'UNIT']) & (R['type'] == R.loc[i, 'type'])]
        if len(f) > 1:
            R.loc[i, 'check'] = 'check'
    R['CHANNELNAMETYPE'] = R.apply(lambda x: cal_name_CHANNELtype(x['UNIT'], x['type']), axis=1)
    key = (R['ORIGINCHANNEL'] == 'Date & Time Stamp') | (R['ORIGINCHANNEL'] == 'Timestamp') | \
          (R['ORIGINCHANNEL'] == 'TIMESTAMP')
    R.loc[key, 'CHANNELNAMETYPE'] = 'time'
    R['UNIT'] = R.apply(lambda x: cal_name_unit(x['UNIT'], x['type']), axis=1)
    R['type'] = R['type'].apply(lambda x: str(x).upper() if str(x)!='other' else str(x))
    R['type'] = R['type'].apply(lambda x: 'AVG' if x == 'MEAN' else x)
    R['type'] = R['type'].apply(lambda x: 'other' if x == 'NAN' else x)
    R['type'] = R['type'].apply(lambda x: 'SD' if x == 'STD' else x)
    R['HIGHT'] = R.apply(lambda x: cal_HIGHT_type(x['HIGHT'], x['type']), axis=1)
    return R


def read_biaotou_dat(file_path):
    data = pd.read_csv(file_path, skiprows=1, nrows=2)
    R = pd.DataFrame(
        columns=['ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check'])
    for i, col in enumerate(data.columns):
        R.loc[i, 'ORIGINCHANNEL'] = col
        R.loc[i, 'CHID'] = i
        if '_' in col:
            if ('m' in col.split('_')[1]) & (data.iloc[1, i] in ['Avg', 'Std', 'Max', 'Min']):
                R.loc[i, 'HIGHT'] = col.split('_')[1][:-1]
                # print(col)
            else:
                R.loc[i, 'HIGHT'] = '10'
            if ('m' in col.split('_')[1]) & (R.loc[i, 'HIGHT'] != col.split('_')[1][:-1]):
                R.loc[i, 'check'] = 'check'
                R.loc[i, 'HIGHT'] = col.split('_')[1][:-1]
        else:
            R.loc[i, 'type'] = 'other'
        if data.iloc[1, i] in ['Avg', 'Std', 'Max', 'Min']:
            # if data.iloc[0, i] == 'Deg C':
            #     R.loc[i, 'UNIT'] = '°C'
            # elif data.iloc[0, i] == 'Deg':
            #     R.loc[i, 'UNIT'] = '°'
            # else:
            #     R.loc[i, 'UNIT'] = data.iloc[0, i]
            R.loc[i, 'UNIT'] = data.iloc[0, i]
            R.loc[i, 'type'] = data.iloc[1, i]
        R.loc[i, 'OFF'] = 0

        R.loc[i, 'SCALE'] = 0
    for i in range(len(R)):
        f = R.loc[(R['HIGHT'] == R.loc[i, 'HIGHT']) & (R['UNIT'] == R.loc[i, 'UNIT']) & (R['type'] == R.loc[i, 'type'])]
        if len(f) > 1:
            R.loc[i, 'check'] = 'check'
    R['CHANNELNAMETYPE'] = R.apply(lambda x: cal_name_CHANNELtype(x['UNIT'], x['type']), axis=1)
    key = (R['ORIGINCHANNEL'] == 'Date & Time Stamp') | (R['ORIGINCHANNEL'] == 'Timestamp') | \
          (R['ORIGINCHANNEL'] == 'TIMESTAMP')
    R.loc[key, 'CHANNELNAMETYPE'] = 'time'
    R['UNIT'] = R.apply(lambda x: cal_name_unit(x['UNIT'], x['type']), axis=1)
    R['type'] = R['type'].apply(lambda x: str(x).upper() if str(x) != 'other' else str(x))
    R['type'] = R['type'].apply(lambda x: 'other' if x == 'NAN' else x)
    R['type'] = R['type'].apply(lambda x: 'SD' if x == 'STD' else x)
    R['HIGHT'] = R.apply(lambda x: cal_HIGHT_type(x['HIGHT'], x['type']), axis=1)
    return R


def read_biaotou_wnd(file_path):
    import json
    with open(file_path, 'r') as f:
        data = f.readlines()
    Result = {}
    for i in range(len(data[1].split('#'))):
        if '"SensorConfigurations":' in data[1].split('#')[i]:
            ll = json.loads(data[1].split('#')[i])

            for i in range(len(ll)):
                result_i = {}
                for key in ll[i]['SensorConfigurations'][0].keys():
                    result_i[key] = ll[i]['SensorConfigurations'][0][key]
                Result[ll[i]['SensorConfigurations'][0]['Name']] = result_i
    R = pd.DataFrame(
        columns=['ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check'])
    i = 0
    for col in data[3].split(' '):
        col = col.replace('\n', '')
        if col == 'Date':
            R.loc[i, 'ORIGINCHANNEL'] = 'Date_Time'
            R.loc[i, 'CHID'] = i
            R.loc[i, 'CHANNELNAMETYPE'] = 'time'
            R.loc[i, 'type'] = 'other'
            i = i + 1
        elif col != 'Time':
            R.loc[i, 'ORIGINCHANNEL'] = col
            R.loc[i, 'CHID'] = i
            if col in Result.keys():
                R.loc[i, 'OFF'] = Result[col]['Offset']
                R.loc[i, 'SCALE'] = Result[col]['Slope']
            else:
                R.loc[i, 'OFF'] = 0
                R.loc[i, 'SCALE'] = 0
            if 'WS' in col:
                R.loc[i, 'UNIT'] = 'm/s'
            elif 'WD' in col:
                R.loc[i, 'UNIT'] = '°'
            elif 'PR' in col:
                R.loc[i, 'UNIT'] = 'mBar'
            elif 'TEM' in col:
                R.loc[i, 'UNIT'] = '°C'
            elif 'RH' in col:
                R.loc[i, 'UNIT'] = '%'
            if '_' in col:
                if '-' not in col:
                    R.loc[i, 'type'] = 'AVG'
                    R.loc[i, 'HIGHT'] = col.split('_')[-1]
                else:
                    if col.split('-')[1] == 'STDev':
                        R.loc[i, 'type'] = 'SD'
                    elif col.split('-')[1] == 'Max':
                        R.loc[i, 'type'] = 'MAX'
                    elif col.split('-')[1] == 'Min':
                        R.loc[i, 'type'] = 'MIN'
                    R.loc[i, 'HIGHT'] = col.split('_')[-1].split('-')[0]
            elif col == 'Battery':
                R.loc[i, 'type'] = 'AVG'
                R.loc[i, 'HIGHT'] = '0'
                R.loc[i, 'UNIT'] = 'V'

            i = i + 1
    R['UNIT'] = R['UNIT'].replace('mBar', 'mb')
    R['CHANNELNAMETYPE'] = R.apply(lambda x: cal_name_CHANNELtype(x['UNIT'], x['type']), axis=1)
    key = (R['ORIGINCHANNEL'] == 'Date_Time') | (R['ORIGINCHANNEL'] == 'Timestamp')
    R.loc[key, 'CHANNELNAMETYPE'] = 'time'
    R['UNIT'] = R.apply(lambda x: cal_name_unit(x['UNIT'], x['type']), axis=1)
    R['type'] = R['type'].fillna('other')
    R['HIGHT'] = R.apply(lambda x: cal_HIGHT_type(x['HIGHT'], x['type']), axis=1)
    return R


def read_nrg(readpath, savepath, password, nrg_type):
    # date_filter = '2'
    save_path = savepath + '/'
    read_path = readpath + '/'
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    # 测风塔密码
    # password = '202102'
    if nrg_type == 'rwd':
        converter = nrgpy.local_rwd(rwd_dir=read_path, encryption_pin=password, out_dir=save_path)
    else:
        converter = nrgpy.local_rld(rld_dir=read_path, encryption_pass=password, out_dir=save_path)
    converter.convert()


def read_password(cefengta_ID):
    host = 'localhost'
    port = 3306
    user = 'root'  # 用户名
    password = '123456'  # 密码
    database = 'cefengta'
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT CODE FROM cefengta.static_information where ID='%s';" % cefengta_ID)
    password_nrg = cursor.fetchone()
    cursor.close()
    conn.close()
    return password_nrg[0]

def read_channel_from_file(cefengta_ID,dir_path,save_path,datatype):
    file_path_read = ''
    log_error = ''
    print(cefengta_ID,dir_path,save_path,datatype)
    try:
        for file in os.listdir(dir_path):
            if '.' in file:
                if file.split('.')[-1] == 'rar':
                    rar_path = Path(dir_path) / file
                    savepath = Path(dir_path) / 'rar'
                    if not os.path.exists(savepath):
                        os.makedirs(savepath)
                    with rarfile.RarFile(rar_path) as rf:
                        rf.extractall(savepath)
                    if (len(os.listdir(savepath)) == 1) & (
                    os.path.isdir(str(savepath) + '/' + os.listdir(savepath)[0])):
                        shutil.move(str(savepath) + '/' + os.listdir(savepath)[0], dir_path + '/rar1')
                        shutil.rmtree(savepath)
                        os.rename(dir_path + '/rar1', dir_path + '/rar')
                    dir_path_read = dir_path + '/rar'
                elif file.split('.')[-1] == 'zip':
                    zip_path = Path(dir_path) / file
                    savepath = Path(dir_path) / 'zip'
                    if not os.path.exists(savepath):
                        os.makedirs(savepath)
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        # 遍历ZIP文件中的所有文件条目
                        for zip_info in zip_ref.infolist():
                            # 获取文件名
                            file_name = zip_info.filename
                            # 单独提取每个文件到指定目录，不保留文件夹结构
                            if not zip_info.is_dir():  # 确保它是一个文件
                                dir_path_zip = os.path.join(savepath, os.path.basename(file_name))
                                # 提取文件
                                with zip_ref.open(zip_info) as zip_file:
                                    with open(dir_path_zip, 'wb') as new_file:
                                        new_file.write(zip_file.read())
                    dir_path_read = dir_path + '/zip'
                else:
                    dir_path_read = dir_path
        if (os.listdir(dir_path_read)[0].endswith('.RWD')) | (os.listdir(dir_path_read)[0].endswith('.rld')) | (
        os.listdir(dir_path_read)[0].endswith('.rwd')) | (os.listdir(dir_path_read)[0].endswith('.RLD')):
            savepath_nrg = dir_path_read + '/nrg'
            if not os.path.exists(savepath_nrg):
                os.makedirs(savepath_nrg)
            password = read_password(cefengta_ID)
            if password == '':
                log_error = '无密码'
            for file in os.listdir(dir_path_read):
                if file != 'nrg':
                    if (file.endswith('.RWD')) | (file.endswith('.rwd')):
                        nrg_type = 'rwd'
                    else:
                        nrg_type = 'rld'
                    break
            read_nrg(dir_path_read, savepath_nrg, password, nrg_type)
            if getattr(sys, 'frozen', False):  # 判断是exe还是.py程序
                dir = getattr(
                    sys, '_MEIPASS', os.path.dirname(
                        os.path.abspath(__file__)))
                qss_path = dir
            else:
                read = "./"
                for file in os.listdir(read):
                    if '\\' in file:
                        shutil.rmtree(read + file)
            dir_path_read = dir_path_read + '/nrg'
            if len(os.listdir(dir_path_read)) == 0:
                log_error = 'rwd或rld数据解析异常'

        if len(os.listdir(dir_path_read)) > 4:
            file_path = dir_path_read + '/' + os.listdir(dir_path_read)[2]
        else:
            file_path = dir_path_read + '/' + os.listdir(dir_path_read)[0]
        if (file_path.endswith('csv')) | (file_path.endswith('CSV')):
            if datatype == 0:
                R = read_biaotou_csv(file_path)
                txt_type = 'csv'
            else:
                f = open(file_path, 'rb')
                data = f.read()
                with open(file_path, 'r', encoding=chardet.detect(data).get("encoding")) as f:
                    line = f.readline()
                if 'Timestamp' in line:
                    R = read_biaotou_leida(file_path)
                else:
                    R = read_biaotou_csv_leida(file_path)
                txt_type = 'csv'
        elif file_path.endswith('dat'):
            R = read_biaotou_dat(file_path)
            txt_type = 'dat'
        elif file_path.endswith('wnd'):
            R = read_biaotou_wnd(file_path)
            txt_type = 'wnd'
        else:
            if datatype == 2:
                R = read_biaotou_txt_leida(file_path)
                txt_type = 'txt'
            else:
                with open(file_path, 'r') as f:
                    line = f.readline()
                if 'SDR' in line:
                    R = read_biaotou_rwd(file_path)
                    txt_type = 'rwd'
                else:
                    R = read_biaotou_rld(file_path)
                    txt_type = 'rld'
        R['ID'] = cefengta_ID
        if txt_type != 'rwd':
            R = R[['ID', 'ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check']]
        else:
            R = R[
                ['ID', 'ORIGINCHANNEL', 'ORIGINCHANNEL_use', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE',
                 'type', 'check']]
        R.reset_index(inplace=True, drop=True)
        R.loc[R['CHANNELNAMETYPE'] == 'time', 'HIGHT'] = ''
        R.loc[R['HIGHT'] == '0', 'type'] = 'other'
        R.loc[R['HIGHT'] == '0', 'CHANNELNAMETYPE'] = ''
        R.loc[R['HIGHT'] == '0', 'UNIT'] = ''
        # R.to_csv('/home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/testcol11.csv')
        R = R.to_dict(orient="index")
        json_str = simplejson.dumps(R, indent=4, ignore_nan=True)
        print(save_path)
        with open(save_path, 'w') as f:
            f.write(json_str)
    except Exception as e:
        error_json = {}
        if log_error != '':
            error_json['error'] = log_error
        else:
            error_json['error'] = str(e)
        json_str = simplejson.dumps(error_json, indent=4, ignore_nan=True)
        with open(save_path, 'w') as f:
            f.write(json_str)

if __name__ == '__main__':
    # file_path = '/home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/沾化三期/5430201902220001.csv'
    # cefengta_ID = 'M005430'
    # save_path = '/home/xiaowu/share/202404/运达测风塔数据/测试数据/表头们/test5430_CSV.json'
    # file_path = '/home/xiaowu/share/202404/运达测风塔数据/测试数据/txt文件/8182_20170609-20180705.txt'
    # cefengta_ID = 'M008182'
    # save_path = '/home/xiaowu/share/202404/运达测风塔数据/测试数据/表头们/test8182.json'
    # file_path = '/home/xiaowu/share/202404/运达测风塔数据/测试数据/txt文件/000001_QLXNY-YUSHEXIAN_meas_2023.06.08-2023.11.23.txt'
    # cefengta_ID = 'M000001'
    # save_path = '/home/xiaowu/share/202404/运达测风塔数据/测试数据/表头们/test000001.json'
    # python3 read_channel_from_file.py M000001 /home/xiaowu/share/202404/运达测风塔数据/测试数据/txt文件 /home/xiaowu/share/202404/运达测风塔数据/测试数据/表头们/test000001.json
    # python3 read_channel_from_file.py M008182 /home/xiaowu/share/202404/运达测风塔数据/测试数据/txt文件 /home/xiaowu/share/202404/运达测风塔数据/测试数据/表头们/test8182.json
    # python3 read_channel_from_file.py L001329 /home/xiaowu/share/202404/运达测风塔数据/测试数据/激光雷达文件 /home/xiaowu/share/202404/运达测风塔数据/测试数据/表头们/test1329.json
    #python3 read_channel_from_file.py M005430 /home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/M005430 /home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/testcol.json
    #python3 read_channel_from_file.py M005430 /home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/沾化三期 /home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/testcol.json
    import subprocess

    result = subprocess.run(' ls -l /dev/disk/by-uuid/ | grep sdb1', stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    if '67E3-17ED' in result.stdout.decode('utf-8'):
        cefengta_ID = sys.argv[1]
        dir_path = sys.argv[2]
        save_path = sys.argv[3]
        datatype = int(sys.argv[4])
        file_path_read = ''
        log_error = ''
        try:
            for file in os.listdir(dir_path):
                if '.' in file:
                    if file.split('.')[-1] == 'rar':
                        rar_path = Path(dir_path) / file
                        savepath = Path(dir_path) / 'rar'
                        if not os.path.exists(savepath):
                            os.makedirs(savepath)
                        with rarfile.RarFile(rar_path) as rf:
                            rf.extractall(savepath)
                        if (len(os.listdir(savepath)) == 1) & (os.path.isdir(str(savepath) + '/' + os.listdir(savepath)[0])):
                            shutil.move(str(savepath) + '/' + os.listdir(savepath)[0], dir_path + '/rar1')
                            shutil.rmtree(savepath)
                            os.rename(dir_path + '/rar1', dir_path + '/rar')
                        dir_path_read = dir_path + '/rar'
                    elif file.split('.')[-1] == 'zip':
                        zip_path = Path(dir_path) / file
                        savepath = Path(dir_path) / 'zip'
                        if not os.path.exists(savepath):
                            os.makedirs(savepath)
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            # 遍历ZIP文件中的所有文件条目
                            for zip_info in zip_ref.infolist():
                                # 获取文件名
                                file_name = zip_info.filename
                                # 单独提取每个文件到指定目录，不保留文件夹结构
                                if not zip_info.is_dir():  # 确保它是一个文件
                                    dir_path_zip = os.path.join(savepath, os.path.basename(file_name))
                                    # 提取文件
                                    with zip_ref.open(zip_info) as zip_file:
                                        with open(dir_path_zip, 'wb') as new_file:
                                            new_file.write(zip_file.read())
                        dir_path_read = dir_path + '/zip'
                    else:
                        dir_path_read = dir_path
            if (os.listdir(dir_path_read)[0].endswith('.RWD')) | (os.listdir(dir_path_read)[0].endswith('.rld')) | (os.listdir(dir_path_read)[0].endswith('.rwd')) | (os.listdir(dir_path_read)[0].endswith('.RLD')):
                savepath_nrg = dir_path_read + '/nrg'
                if not os.path.exists(savepath_nrg):
                    os.makedirs(savepath_nrg)
                password = read_password(cefengta_ID)
                if password == '':
                    log_error = '无密码'
                for file in os.listdir(dir_path_read):
                    if file != 'nrg':
                        if (file.endswith('.RWD')) | (file.endswith('.rwd')):
                            nrg_type = 'rwd'
                        else:
                            nrg_type = 'rld'
                        break
                read_nrg(dir_path_read, savepath_nrg, password, nrg_type)
                if getattr(sys, 'frozen', False):  # 判断是exe还是.py程序
                    dir = getattr(
                        sys, '_MEIPASS', os.path.dirname(
                            os.path.abspath(__file__)))
                    qss_path = dir
                else:
                    read = "./"
                    for file in os.listdir(read):
                        if '\\' in file:
                            shutil.rmtree(read + file)
                dir_path_read = dir_path_read + '/nrg'
                if len(os.listdir(dir_path_read)) == 0:
                    log_error = 'rwd或rld数据解析异常'

            if len(os.listdir(dir_path_read)) > 4:
                file_path = dir_path_read + '/' + os.listdir(dir_path_read)[2]
            else:
                file_path = dir_path_read + '/' + os.listdir(dir_path_read)[0]

            if (file_path.endswith('csv')) | (file_path.endswith('CSV')):
                if datatype == 0:
                    R = read_biaotou_csv(file_path)
                    txt_type = 'csv'
                else:
                    f = open(file_path, 'rb')
                    data = f.read()
                    with open(file_path, 'r', encoding=chardet.detect(data).get("encoding")) as f:
                        line = f.readline()
                    if 'Timestamp' in line:
                        R = read_biaotou_leida(file_path)
                    else:
                        R = read_biaotou_csv_leida(file_path)
                    txt_type = 'csv'
            elif file_path.endswith('dat'):
                R = read_biaotou_dat(file_path)
                txt_type = 'dat'
            elif file_path.endswith('wnd'):
                R = read_biaotou_wnd(file_path)
                txt_type = 'wnd'
            else:
                if datatype == 2:
                    R = read_biaotou_txt_leida(file_path)
                    txt_type = 'txt'
                else:
                    with open(file_path, 'r') as f:
                        line = f.readline()
                    if 'SDR' in line:
                        R = read_biaotou_rwd(file_path)
                        txt_type = 'rwd'
                    else:
                        R = read_biaotou_rld(file_path)
                        txt_type = 'rld'
            R['ID'] = cefengta_ID
            if txt_type != 'rwd':
                R = R[['ID', 'ORIGINCHANNEL', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check']]
            else:
                R = R[['ID', 'ORIGINCHANNEL', 'ORIGINCHANNEL_use', 'CHANNELNAMETYPE', 'CHID', 'HIGHT', 'OFF', 'UNIT', 'SCALE', 'type', 'check']]

            R.reset_index(inplace=True, drop=True)
            R.loc[R['CHANNELNAMETYPE'] == 'time', 'HIGHT'] = ''
            R.loc[R['HIGHT'] == '0', 'type'] = 'other'
            R.loc[R['HIGHT'] == '0', 'CHANNELNAMETYPE'] = ''
            R.loc[R['HIGHT'] == '0', 'UNIT'] = ''
            # R.to_csv('/home/xiaowu/share/202404/运达测风塔数据/测试数据/csv文件/testcol11.csv')
            R = R.to_dict(orient="index")
            json_str = simplejson.dumps(R, indent=4, ignore_nan=True)
            with open(save_path, 'w') as f:
                f.write(json_str)
        except Exception as e:
            error_json = {}
            if log_error != '':
                error_json['error'] = log_error
            else:
                error_json['error'] = str(e)
            json_str = simplejson.dumps(error_json, indent=4, ignore_nan=True)
            with open(save_path, 'w') as f:
                f.write(json_str)