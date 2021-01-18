# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/21 11:07
@Author   : Jarvis
"""
from main_code.data_base import get_data_from_hive, get_wtg_id_list, get_mysql_conn, get_value_scope, \
    insert_data_missing_info
import pandas as pd
import numpy as np
import os
from main_code.paths import project
import datetime

REMOVE_COPY = 0
if REMOVE_COPY:
    from main_code.data_copy import find_copy_data


def process_retrain_data(task_id: str, info: list, logger, fault_data: int):
    """处理重训练任务对应的数据"""
    return process_data(task_id, info, logger, 'train', fault_data)


def process_predict_data(task_id: str, info: list, logger, use_tmp: int):
    """处理预测任务对应的数据"""
    return process_data(task_id, info, logger, 'predict', use_tmp)


def handle_outliers_and_build_feature(data: pd.DataFrame, scope: dict):
    """处理异常值。异常值包括错误值以及超出指标范围的值"""
    # step 1: 固定异常值的处理
    data.replace(-902, np.nan, inplace=True)
    index = pd.date_range(start=min(data['localtime']), end=max(data['localtime']), freq='10min')
    index = pd.DataFrame(index=index).reset_index().rename(columns={"index": "localtime"})
    data = pd.merge(index, data, on='localtime', how='left', sort=False)
    data = data[data['deviceid'].notna()].reset_index(drop=True)

    # step 2：增加字段
    data = data.sort_values(['localtime']).reset_index(drop=True)
    cols = data.columns
    for col in cols:
        if col not in ['localtime', 'deviceid', 'siteid', 'wtg_desc', 'wtg_mc', 'site_cn', 'province',
                       'type_id', '1#桨叶片角度(桨距角)_avg']:
            data[col + '_last_1'] = data[col].shift(1)
    for i in ['齿轮箱高速轴驱动端轴承温度_avg', '齿轮箱高速轴驱动端轴承温度_avg_last_1',
              '齿轮箱油温_avg', '齿轮箱油温_avg_last_1']:
        data = data.dropna(subset=[i], axis=0).reset_index(drop=True)

    # step 3：指标运行范围的处理
    data = data[data['风速_avg'] <= scope['wind_speed'][2]]
    data = data[data['发电机有功功率_avg'] > scope['active_power'][0]]
    data = data[data['1#桨叶片角度(桨距角)_avg'] < scope['position'][1]]
    data = data[data['舱外温度_avg'] < scope['out_temp'][1]]
    return data


def fault_data_filter(data: pd.DataFrame, wtg_id: str):
    conn = get_mysql_conn()
    with conn.cursor() as cr:
        cr.execute(
            f"select start_time, end_time from tb_marked_data where wtg_id='{wtg_id}' and fault_type=1 and start_time not like '0000%' and end_time not like '0000%'")
        res = cr.fetchall()
        if not res:
            return data
        for i in res:
            start_time = i[0] - datetime.timedelta(days=30)
            end_time = i[1] + datetime.timedelta(days=1)
            data = data[~((data['localtime'] > start_time) & (data['localtime'] < end_time))]
    data = data.sort_values(['localtime']).reset_index(drop=True)
    return data


def state_one(data, scope):
    """最大风能捕获区的数据"""
    # 主要判断依据是桨距角
    # data = data[(data['1#桨叶片角度(桨距角)_avg'] >= 0) & (data['1#桨叶片角度(桨距角)_avg'] < 1)]

    # data = data[data['发电机转速_avg'] < scope['gen_speed'][1]]
    # data = data[data['发电机转速_avg'] >= scope['gen_speed'][0]]
    # data = data[data['风速_avg'] < scope['wind_speed'][1]]
    # data.drop(['1#桨叶片角度(桨距角)_avg'], axis=1, inplace=True)
    # data = data[(data['齿轮箱油温_avg'] > 0) & (data['齿轮箱油温_avg'] < 71.15)]
    data = data[(data['发电机转速_avg'] >= 1100) & (data['发电机转速_avg'] <= 1700)]
    return data


def state_three(data, scope):
    """恒定功率区"""
    # 主要判断依据是桨距角
    # data = data[data['1#桨叶片角度(桨距角)_avg'] >= 1]

    # data = data[(data['风速_avg'] >= scope['wind_speed'][1])]
    # data = data[(data['发电机转速_avg'] <= scope['gen_speed'][2]) & (data['发电机转速_avg'] >= scope['gen_speed'][0])]
    # data.drop(['1#桨叶片角度(桨距角)_avg'], axis=1, inplace=True)
    # data = data[(data['齿轮箱油温_avg'] > 47.3) & (data['齿轮箱油温_avg'] < 73.739)]
    data = data[data['发电机转速_avg'] > 1700]
    return data


def get_site_and_type(site_list: list, type_list: list) -> dict:
    """根据台账表获取每个风场所包含的机型，并与指定的机型取交集

    :param site_list: list of sites
    :param type_list: list of types
    :return: dict of site and its types
    """
    site_types = {}
    conn = get_mysql_conn()
    cr = conn.cursor()
    for site in site_list:
        sql = f"select distinct type from pf_longyuan_farm2type where site = '{site}'"
        cr.execute(sql)
        types = cr.fetchall()
        types = list(set(map(lambda x: x[0], types)).intersection(set(type_list)))
        if types:
            site_types[site] = types
    return site_types


def process_data(task_id: str, info: list, logger, mode: str, use_tmp: int, fault_data: int = 0):
    if mode == 'train':
        start_time, end_time = info[3:5]
    else:
        # 如果是预测任务，则首先将详情表与结果表同任务下的数据清空
        conn = get_mysql_conn(logger)
        with conn.cursor() as cr:
            cr.execute(f"delete from tb_task_result_detail where `task_id`='{task_id}'")
            cr.execute(f"delete from tb_fault_results where `task_id`='{task_id}'")
            conn.commit()
        conn.close()
        start_time, end_time = info[2:4]

    site_id, type_id, wtg_id = info[6:9]

    if not site_id:
        logger.info("未指定风场，默认计算河北尚义麒麟山风电场")
        site_id_list = ['17828a2b062e6000']
    else:
        site_id_list = list(site_id.split(','))
        logger.info(f"指定了风场，共{len(site_id_list)}个")
    if not type_id:
        logger.info('未指定机型，对`UP82-1500`型号的风机进行训练')
        type_id_list = ['UP82-1500']
    else:
        type_id_list = list(type_id.split(','))
        logger.info(f"指定了机型，共{len(type_id_list)}种")

    logger.info("将机型与对应型号进行匹配...")
    site_types = get_site_and_type(site_id_list, type_id_list)
    logger.info("匹配完成")
    if not site_types:
        logger.info("当前风场无可用机型")
        return 0
    logger.info(f"实际共需计算{len(site_types)}个风场")

    all_data1 = []
    all_data3 = []
    site_no = 1
    columns = ['localtime', 'deviceid', 'siteid', 'wtg_desc', 'wtg_mc', 'site_cn', 'province',
               'type_id', '发电机有功功率_avg', '发电机转速_avg', '舱外温度_avg', '风速_avg', '风轮转速_avg', '齿轮箱油温_avg',
               '齿轮箱高速轴驱动端轴承温度_avg', '机舱与风向夹角_avg', '主轴承温度_avg', '偏航角度(扭缆角度）_avg',
               '发电机非驱动端轴承温度_avg', '发电机驱动端轴承温度_avg', '齿轮箱高速轴非驱动端轴承温度_avg',
               '发电机有功功率_avg_last_1', '发电机转速_avg_last_1', '舱外温度_avg_last_1',
               '风速_avg_last_1', '风轮转速_avg_last_1', '齿轮箱油温_avg_last_1',
               '齿轮箱高速轴驱动端轴承温度_avg_last_1', '机舱与风向夹角_avg_last_1', '主轴承温度_avg_last_1',
               '偏航角度(扭缆角度）_avg_last_1', '发电机非驱动端轴承温度_avg_last_1',
               '发电机驱动端轴承温度_avg_last_1', '齿轮箱高速轴非驱动端轴承温度_avg_last_1'],
    is_not_data = True
    for site in site_types:
        logger.info(f"当前计算第{site_no}/{len(site_types)}个风场...")
        type_no = 1
        for type_ in site_types[site]:
            logger.info(f"当前计算第{type_no}/{len(site_types[site])}个机型...")
            logger.info(f"获取风机id...")
            scope = get_value_scope(type_, logger)
            wtg_id_list = get_wtg_id_list(site, type_)
            logger.info("获取风机id完成")
            wtg_no = 1
            all_data_ = []
            for id_ in wtg_id_list:
                logger.info(f"当前进度：风场：{site_no}/{len(site_types)}-机型：{type_no}/{len(site_types[site])}-"
                            f"风机：{wtg_no}/{len(wtg_id_list)}")
                data = get_data_from_hive(site_id=site, wtg_id=id_,
                                          start_time=start_time.strftime('%Y%m%d'),
                                          end_time=end_time.strftime('%Y%m%d'), logger=logger)
                if data is None:
                    logger.info(f"    WARNING:>>无法连接Hive数据库")
                    return 0
                if mode == 'train':
                    inf = data_missing_check(task_id, id_, info, data, 1)
                else:
                    inf = data_missing_check(task_id, id_, info, data, 2)
                if inf is not None:
                    insert_data_missing_info(inf, logger)
                if data.empty:
                    logger.info(f'    WARNING:>>风机{id_}在指定的时间范围内未获取到数据！')
                    wtg_no += 1
                    continue
                if mode == 'train' and fault_data:
                    logger.info("    >>进行故障数据过滤")
                    data = fault_data_filter(data, id_)
                logger.info("    >>异常值处理与增加特征")
                data = handle_outliers_and_build_feature(data, scope)
                if data.empty:
                    logger.info(f'    WARNING:>>风机{id_}处理后无有效数据！')
                    wtg_no += 1
                    continue
                data.fillna(0, inplace=True)
                data.insert(4, 'type_id', type_)
                all_data_.append(data)
                logger.info(f"    <<{id_}完成！")
                wtg_no += 1
            if not all_data_:
                logger.info(f"没有获取到`{type_}`的有效数据")
                type_no += 1
                continue
            data = pd.concat(all_data_, ignore_index=True)

            logger.info(f"{site_no}/{len(site_types)}-{type_no}/{len(site_types[site])}-`{type_}`正在进行工况划分...")
            data1 = state_one(data, scope)
            all_data1.append(data1)
            data3 = state_three(data, scope)
            all_data3.append(data3)
            logger.info(f"`{type_}`工况划分完成...")
            type_no += 1
        site_no += 1
        if mode == 'predict':
            # 如果是预测任务，如果选择多个风场，则处理完一个预测一个，不再等待全部处理后再预测
            logger.info(f"风场{site}数据处理完成，开始进行故障预测...")
            from main_code.prediction import warning_fault2
            from main_code.data_base import save_fault
            # 对于预测任务而言，当一个风场的所有数据处理完成后就进行预测，而不再进行缓存
            # 因此，需要将`all_data1`和`all_data3`恢复为空，以存储下一个风场的数据
            if not all_data1:  # 如果所有机型都没有工况1中的数据
                data1 = pd.DataFrame(columns=columns)
            else:
                data1 = pd.concat(all_data1)
            if not all_data3:  # 如果所有机型都没有工况3中的数据
                data3 = pd.DataFrame(columns=columns)
            else:
                data3 = pd.concat(all_data3)

            all_data1, all_data3 = [], []

            res = warning_fault2(task_id, data1, data3, info, logger, use_tmp)
            if isinstance(res, int):
                logger.info("当前风场无数据，无预测结果\n")
                continue
            else:
                is_not_data = False
                logger.info("故障预警完成，开始结果入库...")
                save_fault(res, task_id, logger)
                logger.info(f"风场`{site}`的预测结果入库完成。\n")
    if mode == 'predict':
        logger.info("所有风场数据预测完成\n")
        if is_not_data:
            return 3
        return 1
    else:
        num_d = 0
        logger.info("所有风场处理完成，正在进行拼接...")
        if all_data1:
            data1 = pd.concat(all_data1)
        else:
            num_d += 1
            logger.info("最大风能捕获区无数据")
            data1 = pd.DataFrame(columns=columns)
        if all_data3:
            data3 = pd.concat(all_data3)
        else:
            num_d += 1
            logger.info("恒定功率区无数据")
            data3 = pd.DataFrame(columns=columns)
        logger.info("拼接完成，正在保存...")
        if num_d == 2:
            return 3
        if REMOVE_COPY:
            logger.info("数据镜像情况识别与处理...")
            data_copy_result = find_copy_data(data1)
            if len(data_copy_result) != 0:
                for i in range(len(data_copy_result)):
                    start_time, end_time, wtg_name = data_copy_result.iloc[i]['start_time'], \
                                                     data_copy_result.iloc[i]['end_time'], \
                                                     data_copy_result.iloc[i]['device_id']
                    wtg1 = wtg_name[: str(wtg_name).index('_')]
                    wtg2 = wtg_name[str(wtg_name).index('_') + 1:]
                    data1 = data1[~((data1['deviceid'] == wtg1) & (data1['localtime'] >= start_time) &
                                    (data1['localtime'] <= end_time))]
                    data1 = data1[~((data1['deviceid'] == wtg2) & (data1['localtime'] >= start_time) &
                                    (data1['localtime'] <= end_time))]
            data_copy_result = find_copy_data(data3)
            if len(data_copy_result) != 0:
                for i in range(len(data_copy_result)):
                    start_time, end_time, wtg_name = data_copy_result.iloc[i]['start_time'], \
                                                     data_copy_result.iloc[i]['end_time'], \
                                                     data_copy_result.iloc[i]['device_id']
                    wtg1 = wtg_name[: str(wtg_name).index('_')]
                    wtg2 = wtg_name[str(wtg_name).index('_') + 1:]
                    data3 = data3[~((data3['deviceid'] == wtg1) & (data3['localtime'] >= start_time) &
                                    (data3['localtime'] <= end_time))]
                    data3 = data3[~((data3['deviceid'] == wtg2) & (data3['localtime'] >= start_time) &
                                    (data3['localtime'] <= end_time))]
        col_threshold = train_data_threshold(pd.concat([data1, data3], ignore_index=True))
        data1 = col_data_filter(data1, col_threshold)
        data3 = col_data_filter(data3, col_threshold)
        if data1 is None:
            data1 = pd.DataFrame(columns=columns)
        if data3 is None:
            data3 = pd.DataFrame(columns=columns)
        if data1.shape[0] == 0 and data3.shape[0] == 0:
            return 3
        data1.to_csv(os.path.join(project.preprocessed_dir, f'{mode}_data_1_{task_id}.csv'),
                     index=False, encoding='utf-8')
        data3.to_csv(os.path.join(project.preprocessed_dir, f'{mode}_data_3_{task_id}.csv'),
                     index=False, encoding='utf-8')
        logger.info('保存完成')
        return 1


def threshold_value(loss_data, rate=3):
    data_mean = np.mean(loss_data)
    data_std = np.std(loss_data)
    up_limit = data_mean + rate * data_std
    lower_limit = data_mean - rate * data_std
    return [lower_limit, up_limit]


def train_data_threshold(df_data):
    cols = df_data.columns.tolist()
    d_threshold = dict()
    for c in cols:
        if ("温度" in c) and ("last_1" not in c):
            d_threshold[c] = threshold_value(df_data[c].values)
    return d_threshold


def col_data_filter(df_data, d_threshold: dict):
    cols = df_data.columns.tolist()
    keys = d_threshold.keys()
    for c in cols:
        if c in keys:
            threshold_val = d_threshold.get(c)
            df_data = df_data[(df_data[c] > threshold_val[0]) & (df_data[c] < threshold_val[1])]
    return df_data


def data_missing_check(task_id, machine_id, info: list, machine_data: pd.DataFrame, task_type: int):
    columns = machine_data.columns.tolist()
    if task_type == 1:
        start_time, end_time = info[3:5]
    else:
        start_time, end_time = info[2:4]
    columns_missing = []
    for c in columns:
        if machine_data[c].isnull().all():
            columns_missing.append(c)
    if len(columns_missing) == 0:
        return None
    elif machine_data.shape[0] == 0:
        info = dict()
        info['task_id'] = task_id
        info['machine_id'] = machine_id
        info['start_time'] = start_time
        info['end_time'] = end_time
        info['wtg_desc'] = ''
        info['site_id'] = ''
        info['site_cn'] = ''
        info['wtg_mc'] = ''
        info['province'] = ''
        info['missing_columns'] = ",".join(columns_missing)
        info['task_type'] = task_type
        return info
    else:
        # 获取基本信息
        info = dict()
        info['task_id'] = task_id
        info['machine_id'] = machine_id
        info['start_time'] = start_time
        info['end_time'] = end_time
        info['wtg_desc'] = machine_data['wtg_desc'].iloc[0]
        info['site_id'] = machine_data['siteid'].iloc[0]
        info['site_cn'] = machine_data['site_cn'].iloc[0]
        info['wtg_mc'] = machine_data['wtg_mc'].iloc[0]
        info['province'] = machine_data['province'].iloc[0]
        info['missing_columns'] = ",".join(columns_missing)
        info['task_type'] = task_type
        return info
