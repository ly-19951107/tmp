# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code
@Time     : 2020/4/21 14:41
@Author   : Jarvis
"""
import pandas as pd
from main_code.paths import project
import os
import joblib
import numpy as np
import datetime
from main_code.data_base import get_mysql_conn
import uuid


def warning_fault2(task_id: str, data1: pd.DataFrame, data3: pd.DataFrame, info: list, logger, use_tmp: int):
    """对于一个风场的数据进行故障预测"""
    logger.info("最大风能捕获区温度预测...")
    if data1.empty:
        machine_id_list1 = set()
        data1 = pd.DataFrame()
        logger.info("最大风能捕获区无数据")
    else:
        data1['localtime'] = pd.to_datetime(data1['localtime'], format='%Y-%m-%d %H:%M:%S')
        machine_id_list1 = set(list(data1['deviceid']))
        result_one = []
        for id_ in machine_id_list1:
            data = data1[data1['deviceid'] == id_]
            site = data['siteid'].iloc[0]
            site_cn = data['site_cn'].iloc[0]
            type_id = data['type_id'].iloc[0]
            wtg_desc = data['wtg_desc'].iloc[0]
            wtg_mc = data['wtg_mc'].iloc[0]
            province = data['province'].iloc[0]
            temp1 = predict_temp(data, "one", task_id, use_tmp)
            df = pd.DataFrame({
                "localtime": data['localtime'],
                "site_id": site,
                "site_cn": site_cn,
                "type_id": type_id,
                "machine_id": id_,
                "wtg_desc": wtg_desc,
                "wtg_mc": wtg_mc,
                "province": province,
                "real_temp": data['齿轮箱油温_avg'],
                "pred_temp": temp1
            })
            result_one.append(df)
        if result_one:
            data1 = pd.concat(result_one, ignore_index=True)
            data1['loss'] = data1['real_temp'] - data1['pred_temp']
        else:
            data1 = pd.DataFrame()

    logger.info("恒定功率区温度预测...")
    if data3.empty:
        machine_id_list3 = set()
        data3 = pd.DataFrame()
        logger.info("恒定功率区无数据")
    else:
        data3['localtime'] = pd.to_datetime(data3['localtime'], format='%Y-%m-%d %H:%M:%S')
        machine_id_list3 = set(list(data3['deviceid']))
        result_three = []
        for id_ in machine_id_list3:
            data = data3[data3['deviceid'] == id_]
            site = data['siteid'].iloc[0]
            site_cn = data['site_cn'].iloc[0]
            type_id = data['type_id'].iloc[0]
            wtg_desc = data['wtg_desc'].iloc[0]
            wtg_mc = data['wtg_mc'].iloc[0]
            province = data['province'].iloc[0]
            temp3 = predict_temp(data, "three", task_id, use_tmp)
            df = pd.DataFrame({
                "localtime": data['localtime'],
                "site_id": site,
                "site_cn": site_cn,
                "type_id": type_id,
                "machine_id": id_,
                "wtg_desc": wtg_desc,
                "wtg_mc": wtg_mc,
                "province": province,
                "real_temp": data['齿轮箱油温_avg'],
                "pred_temp": temp3
            })
            result_three.append(df)
        if result_three:
            data3 = pd.concat(result_three, ignore_index=True)
            data3['loss'] = data3['real_temp'] - data3['pred_temp']
        else:
            data3 = pd.DataFrame()

    if all([data1.empty, data3.empty]):  # 两工况都无数据
        return 0
    elif not any([data1.empty, data3.empty]):  # 两工况都有数据
        all_data = pd.concat([data1, data3])
    else:  # 只有一个有数据
        if data1.empty:
            all_data = data3
        else:
            all_data = data1
    logger.info("温度预测完成，开始进行故障预警...")
    fault_res = []
    machine_id_list = machine_id_list1.union(machine_id_list3)
    for id_ in machine_id_list:
        data = all_data[all_data['machine_id'] == id_]
        data_cp = data.copy()
        if data.empty:
            continue
        # site_id = data['site_id'].iloc[0]
        wtg_desc = data['wtg_desc'].iloc[0]
        site_id = data['site_id'].iloc[0]
        site_cn = data['site_cn'].iloc[0]
        wtg_mc = data['wtg_mc'].iloc[0]
        province = data['province'].iloc[0]
        type_id = data['type_id'].iloc[0]
        start_time = temp_out_gauge(task_id, data, info, use_tmp)
        if start_time:
            fault_res.append(pd.DataFrame({
                "site_id": site_id,
                "site_cn": site_cn,
                "type_id": type_id,
                "wtg_id": id_,
                "wtg_desc": wtg_desc,
                "wtg_mc": wtg_mc,
                "province": province,
                "is_fault": 1,
                "warning_time": [start_time],
                "msg": ["齿轮箱油温过高"]
            }))
            mid_data_save(task_id, data_cp, id_, logger)
        else:
            fault_res.append(pd.DataFrame({
                "site_id": site_id,
                "site_cn": site_cn,
                "type_id": type_id,
                "wtg_id": id_,
                "wtg_desc": wtg_desc,
                "wtg_mc": wtg_mc,
                "province": province,
                "is_fault": 0,
                "warning_time": None,
                "msg": ["无故障"]
            }))
    if not fault_res:
        return 0
    else:
        return pd.concat(fault_res, ignore_index=True)


def mid_data_save(task_id, data, id_, logger):
    wtg_desc = data['wtg_desc'].iloc[0]
    site_id = data['site_id'].iloc[0]
    site_cn = data['site_cn'].iloc[0]
    wtg_mc = data['wtg_mc'].iloc[0]
    province = data['province'].iloc[0]
    conn = get_mysql_conn(logger)
    with conn.cursor() as cr:
        for i in range(data.shape[0]):
            value_time = data.iloc[i]['localtime']
            true_temp = data.iloc[i]['real_temp']
            pred_temp = data.iloc[i]['pred_temp']
            loss = data.iloc[i]['loss']
            detail_id = str(uuid.uuid1())
            sql = f"""
                insert into tb_loss_gearbox_oil_temp (`id`,`task_id`,`site`,`site_cn`,`wtg_id`,`wtg_desc`,`value_time`,
                    `true_temp`,`pred_temp`,`loss`,`yyyymmdd`,`province`, `wtg_mc`)
                values ('{detail_id}','{task_id}','{site_id}','{site_cn}','{id_}','{wtg_desc}','{value_time}',
                    {true_temp},{pred_temp},{loss},'{value_time.strftime('%Y%m%d')}','{province}', '{wtg_mc}')
            """
            cr.execute(sql)
    conn.commit()
    conn.close()


def predict_temp(data: pd.DataFrame, state: str, task_id: str, use_tmp: int):
    """对预测的数据进行标准化，然后预测其油温"""
    if use_tmp:
        mms_model_name = f"{state}_mms_tmp_{task_id}.m"
        rf_model_name = f"{state}_rf_tmp_{task_id}.m"
    else:
        mms_model_name = f"{state}_mms_v1.m"
        rf_model_name = f"{state}_rf_v1.m"

    data = data.iloc[:, 8:]
    x = data.drop('齿轮箱油温_avg', axis=1)
    path = os.path.join(project.feature_dir, mms_model_name)
    model = joblib.load(path)
    x = model.transform(x)
    path = os.path.join(project.model_dir, rf_model_name)
    model = joblib.load(path)
    y_predict = model.predict(x)
    return y_predict


def temp_out_gauge(task_id, data: pd.DataFrame, info: list, use_tmp: int):
    """"""
    if data.empty:
        return None
    loss1 = loss_data(task_id, "one", use_tmp)
    loss3 = loss_data(task_id, "three", use_tmp)
    loss_thresh = [min(loss1[0], loss3[0]), max(loss1[1], loss3[1])]
    data.set_index(['localtime'], inplace=True)
    data1 = data.resample('h', label='left').count()['machine_id']
    data2 = data.resample('h', label='left').mean()
    data2['count'] = data1
    data2 = data2.reset_index(drop=False)
    data2.sort_values(by='localtime')
    data2.fillna(0)
    data2['fault_mark'] = 0
    index = data2[data2['loss'] > loss_thresh[1]].index
    data2.loc[list(index), ['fault_mark']] = 1
    data2 = data2[(data2['fault_mark'] == 1) & (data2['count'] >= 3)]
    if len(data) < info[5]:
        return None
    for i in range(data2.shape[0]):
        start_time = data2.iloc[i]['localtime']
        end_time = data2.iloc[i]['localtime'] + datetime.timedelta(days=int(info[4]))
        out_days = len(data2[(data2['localtime'] >= start_time) & (data2['localtime'] <= end_time)])
        if out_days >= info[5]:
            return start_time
    return None


def loss_data(task_id: str, state: str, use_tmp: int):
    if use_tmp:
        csv_name = f"{state}_tmp_{task_id}.csv"
    else:
        csv_name = f'{state}_v1.csv'
    data = pd.read_csv(os.path.join(project.threshold_dir, csv_name))
    loss = data['loss_data']
    return get_thresh_value(loss)


def get_thresh_value(data: pd.DataFrame.values, rate=3):
    """3sigma准则的上下边界"""
    mean = np.mean(data)
    std = np.std(data)
    upper = mean + rate * std
    lower = mean - rate * std
    return lower, upper
