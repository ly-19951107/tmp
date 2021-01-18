# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/5/8 19:17
@Author   : Jarvis
"""
import pandas as pd
import datetime


def find_copy_data(data: pd.DataFrame):
    data = data[['localtime', 'deviceid', '齿轮箱油温_avg']]
    data['localtime'] = pd.to_datetime(data['localtime'], format="%Y-%m-%d %H:%M:%S")
    wtg_names = set(data['deviceid'])
    df = pd.DataFrame()
    for w in wtg_names:
        _data = data[data['deviceid'] == w][['localtime', '齿轮箱油温_avg']]
        _data.set_index(['localtime'], inplace=True)
        _data.rename(columns={'齿轮箱油温_avg': w}, inplace=True)
        df = pd.merge(df, _data, how='outer', left_index=True, right_index=True)
    df.dropna(axis=1, thresh=0.5, inplace=True)
    wtg_names = list(df.columns)
    wtg_names.sort()
    copy_info = pd.DataFrame(columns=['start_time', 'end_time', 'device_id'])
    for i in range(len(wtg_names)):
        if i == len(wtg_names) - 1:
            continue
        if wtg_names[i][:2] != wtg_names[i+1][:2]:
            continue
        col_name = wtg_names[i] + '_' + wtg_names[i+1]
        df[col_name] = abs(df[wtg_names[i]] - df[wtg_names[i+1]])
        info = trend_analysis(pd.DataFrame(df[col_name]), col_name)
        if len(info) != 0:
            for d in info:
                copy_info = copy_info.append(pd.DataFrame(d, index=[0]))

    copy_info = data_agg(copy_info)
    copy_info['hours'] = copy_info['end_time'] - copy_info['start_time']
    copy_info['hours'] = copy_info['hours'].astype('timedelta64[h]')
    return copy_info


def trend_analysis(df: pd.DataFrame, col: str):
    df_h = df.resample('h', label='left').mean()
    df_h = df_h[df_h[col] < 0.05]
    df_h = df_h.reset_index(drop=False)
    if len(df_h) < 2:
        return []
    min_time = min(list(df_h['localtime']))
    max_time = max(list(df_h['localtime']))
    time_list = list(df_h['localtime'])
    time_list.sort()
    start_time = min_time
    last_time = min_time
    t = 1
    info = []
    while True:
        next_time = last_time + datetime.timedelta(hours=5)
        num = len(df_h[(df_h['localtime'] <= next_time) & (df_h['localtime'] > last_time)])
        if num >= 3:
            t += 1
            last_time = next_time
        else:
            if t > 1:
                if next_time not in time_list:
                    time_list.append(next_time)
                    time_list.sort()
                    t_time = time_list[time_list.index(next_time) - 1]
                    time_list.remove(next_time)
                    time_list.sort()
                else:
                    t_time = next_time
                result = {
                    "start_time": start_time,
                    "end_time": t_time,
                    "device_id": col
                }
                info.append(result)
            if next_time > max_time:
                break
            if next_time not in time_list:
                time_list.append(next_time)
                time_list.sort()
            if len(time_list) < (time_list.index(next_time) + 2):
                break
            next_time = time_list[time_list.index(next_time) + 1]
            start_time = next_time
            last_time = next_time
            t = 1
        if next_time > max_time:
            break
    return info


def data_agg(info: pd.DataFrame):
    wtg_names = set(info['device_id'].values)
    result_info = pd.DataFrame(columns=['start_time', 'end_time', 'device_id'])
    if len(wtg_names) == 0:
        return result_info
    for id_ in wtg_names:
        data = info[info['device_id'] == id_]
        data.sort_values(by='start_time', inplace=True)
        if data.shape[0] == 1:
            result_info = result_info.append(data)
            continue

        start_time = data.iloc[0]['start_time']
        end_time = data.iloc[0]['end_time']
        for i in range(1, len(data)):
            time1 = data.iloc[i]['start_time']
            time2 = data.iloc[i]['end_time']
            if int((time1-end_time).days) <= 7:
                end_time = time2
            else:
                result_info = result_info.append(pd.DataFrame({"start_time": start_time,
                                                               "end_time": end_time,
                                                               "device_id": id_}, index=[0]))
                start_time = time1
                end_time = time2
            if i == len(data) - 1:
                result_info = result_info.append(pd.DataFrame({"start_time": start_time,
                                                               "end_time": end_time,
                                                               "device_id": id_},
                                                              index=[0]))
    return result_info
