# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/21 11:04
@Author   : Jarvis
"""
from main_code.config import db_info, hive_info
from main_code.paths import project
import pymysql
from pyhive import hive
import pandas as pd
from main_code.sql_file import hive_data_sql
import uuid
import time
import os
import datetime


def get_mysql_conn(logger=None):
    try:
        conn = pymysql.connect(**db_info)
    except pymysql.MySQLError as e:
        print(e)
        if logger:
            logger.error(e)
        return None
    return conn


def get_hive_conn(logger=None):
    try:
        conn = hive.Connection(**hive_info)
    except Exception as e:
        print(e)
        if logger:
            logger.error(e)
        return None
    return conn


def get_predict_task_info(task_id: str, logger):
    conn = get_mysql_conn(logger)
    cr = conn.cursor()
    if not cr.execute(f"select para_id from tb_tasks where id='{task_id}'"):
        logger.warning(f'无效的`task_id`:{task_id}')
        return None
    else:
        res = cr.fetchone()
        update_status(task_id, 1)
        # 获取关键信息
        tab = 'tb_model_predict_para'
        cr.execute(f"select model_id, version_id, start_time, end_time, days, fault_times, convert(site_id USING utf8) "
                   f"as site_id, type_id, wtg_id"
                   f" from {tab} t where t.id='{res[0]}'")
        model_id, version_id, start_time, end_time, days, fault_times, site_id, type_id, wtg_id = cr.fetchone()
        cr.execute(f"select model_name_en from tb_model_info t where t.id = '{model_id}'")
        model_name = cr.fetchone()[0]
        cr.execute(f"select version_no from tb_model_version t where t.id='{version_id}'")
        version_no = cr.fetchone()[0]
        return [model_name, version_no, start_time, end_time, days, fault_times, site_id, type_id, wtg_id, version_id]


def get_train_task_info(task_id: str, logger):
    """"""
    conn = get_mysql_conn(logger)
    cr = conn.cursor()
    if not cr.execute(f"select para_id from tb_tasks where id='{task_id}'"):
        logger.warning(f'无效的`task_id`:{task_id}')
        return None
    else:
        res = cr.fetchone()
        update_status(task_id, 1, conn)
        # 获取关键信息
        tab = 'tb_model_retrain_para'
        cr.execute(f"select model_id, start_time, end_time, description, convert(site_id USING utf8) as site_id, "
                   f"type_id, wtg_id"
                   f" from {tab} t where t.id='{res[0]}'")
        model_id, start_time, end_time, desc, site_id, type_id, wtg_id = cr.fetchone()  # 此时得到的model_id是该重训练任务得到的重训练模型的id，需转化
        cr.execute(f"select id from tb_model_info t where t.model_name_en in ("
                   f"select model_name_en from tb_model_info t2 where t2.id='{model_id}')")
        # 下面的model_id为预测任务可调用的模型的id
        model_id = list(set(map(lambda x: x[0], cr.fetchall())) - {model_id})[0]
        cr.execute(f"select max(version_no) from tb_model_version where model_id='{model_id}'")
        version_no = cr.fetchone()[0]
        cr.execute(f"select model_name_en from tb_model_info t where t.id = '{model_id}'")
        model_name = cr.fetchone()[0]
        return [model_id, model_name, version_no, start_time, end_time, desc, site_id, type_id, wtg_id]


def get_data_from_hive(site_id, wtg_id, start_time, end_time, logger):
    conn = get_hive_conn(logger)
    if conn is None:
        return None
    else:
        data = pd.read_sql(hive_data_sql % (site_id, wtg_id, start_time, end_time), conn)
        conn.close()
        return data


def save_fault(fault: pd.DataFrame, task_id: str, logger):
    """将故障预警结果存入数据库"""
    conn = get_mysql_conn(logger)
    cr = conn.cursor()

    for i in range(fault.shape[0]):
        result_id, predict_time = str(uuid.uuid1()), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        line = fault.iloc[i]
        site_id, site_cn, type_id, wtg_id, wtg_desc, wtg_mc, province, is_fault, warning_time, msg = line
        if is_fault:
            sql = f"""insert into tb_fault_results(`id`,`task_id`,`site_id`, `site_name`,`type_id`,`wtg_id`, `wtg_desc`, 
            `wtg_mc`, `province`, `warning_time`, `run_time`, `msg`,`is_fault`) 
            values ('{result_id}','{task_id}','{site_id}', '{site_cn}','{type_id}','{wtg_id}', '{wtg_desc}',  
            '{wtg_mc}',  '{province}','{warning_time}','{predict_time}','{msg}',{is_fault})
            """
#        else:
#            sql = f"""
#                insert into tb_fault_results(`id`,`task_id`,`site_id`, `site_name`,`type_id`,`wtg_id`, `wtg_desc`, 
#                `wtg_mc`, `province`, `run_time`, `msg`,`is_fault`) 
#                values ('{result_id}','{task_id}','{site_id}', '{site_cn}','{type_id}','{wtg_id}', '{wtg_desc}', 
#                '{wtg_mc}', '{province}', '{predict_time}','{msg}',{is_fault})
#            """
            cr.execute(sql)
    conn.commit()
    conn.close()


def get_wtg_id_list(site_id: str, type_id: str):
    """根据风场id和型号获取所有风机的id"""
    conn = get_hive_conn()
    with conn.cursor() as cr:
        cr.execute(f"select distinct wtg_id from wind_stg_fact_wtg_10m_orc where "
                   f"site='{site_id}' and f2='{type_id}'")
        res = cr.fetchall()
    conn.close()
    return [i[0] for i in res] if res is not None else []


def check_time(start_time, end_time, site):
    """校验训练数据的时间范围与故障样本的时间范围是否有交集"""
    conn = get_mysql_conn()
    cr = conn.cursor()
    cr.execute(f"select start_time, end_time from tb_marked_data where fault_type=1 and site_id='{site}' "
               f"and start_time not like '0000%' and end_time not like '0000%'")
    res = cr.fetchall()
    flag = 0
    if isinstance(start_time, str):
        start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    if isinstance(end_time, str):
        end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    for i in res:
        s = i[0]
        e = i[1]
        print(s)
        if isinstance(s, str):
            s = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        if isinstance(e, str):
            e = datetime.datetime.strptime(e, '%Y-%m-%d %H:%M:%S')
        if s and e:
            if end_time < s:
                continue
            elif e < start_time:
                continue
            else:
                flag = 1
    return flag


def update_status(task_id: str, status, conn=None):
    """将任务重置为0/1/2"""
    if not conn:
        conn = get_mysql_conn()
    with conn.cursor() as cr:
        cr.execute(f"update tb_tasks set `status`={status} where id='{task_id}'")
    conn.commit()


def rm_version(version_no: int):
    """删除某个指定的版本"""
    conn = get_mysql_conn()
    with conn.cursor() as cr:
        cr.execute(f"delete from tb_model_version where model_id='ac740815-ec9a-4c17-8dd5-6b5dcbab3a67' and "
                   f"version_no={version_no}")
    try:
        os.remove(os.path.join(project.feature_dir, f"one_mms_v{version_no}.m"))
        os.remove(os.path.join(project.feature_dir, f"three_mms_v{version_no}.m"))
        os.remove(os.path.join(project.model_dir, f"one_rf_v{version_no}.m"))
        os.remove(os.path.join(project.model_dir, f"three_rf_v{version_no}.m"))
    except FileNotFoundError:
        pass


def get_value_scope(type_id: str, logger):
    """获取指定风机型号的某些运行指标的正常的取值范围"""
    scope = {
        "wind_speed": [3, 10.8, 25],  # 风速：[切入,额定,切出]
        "gen_speed": [977, 1750, 1965],  # 发电机转速：[启动转速,额定转速,最大转速]
        "active_power": [0, 9999],  # 有功功率
        "position": [0, 25],  # 桨距角
        "out_temp": [-20, 80]  # 舱外温度
    }
    conn = get_mysql_conn()
    with conn.cursor() as cr:
        cr.execute(f"select cws, rws, cows, egsrpm, egrpm "
                   f"from pf_longyuan_windparams where model='{type_id}' limit 1")
        res = cr.fetchone()
        try:
            if res[0]:
                scope['wind_speed'][0] = res[0]
            if res[1]:
                scope['wind_speed'][1] = float(res[1])
            if res[2]:
                scope['wind_speed'][2] = float(res[2].split('/')[0])
            if res[3]:
                scope['gen_speed'][0] = float(res[3].split('~')[0])
            if res[4]:
                scope['gen_speed'][1] = int(res[3].split('/')[0])
        except Exception as e:
            logger.error(e)
            print(e)
    conn.close()
    return scope

def insert_data_missing_info(info: dict, logger):
    conn = get_mysql_conn(logger)
    cr = conn.cursor()
    sql = f"insert into tb_data_missing_info(task_id, machine_id, start_time, end_time, wtg_desc, site_id, " \
        f"site_cn, wtg_mc, province, missing_columns, task_type) values ('{info['task_id']}', " \
        f"'{info['machine_id']}', '{info['start_time']}', '{info['end_time']}', '{info['wtg_desc']}', " \
        f"'{info['site_id']}', '{info['site_cn']}', '{info['wtg_mc']}', '{info['province']}', " \
        f"'{info['missing_columns']}', {info['task_type']})"
    print(sql)
    cr.execute(sql)
    conn.commit()
    cr.close()
    conn.close()

