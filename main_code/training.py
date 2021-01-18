# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/21 13:50
@Author   : Jarvis
"""
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import joblib
import os
from main_code.paths import project
from main_code.data_base import get_mysql_conn
import uuid


def train_model(task_id: str, info: list, logger):
    """模型训练"""
    one = train_state_one(task_id, logger)
    if not one:
        return 0
    three = train_state_three(task_id, logger)
    if not three:
        return 0
    # 不再向数据库中更新状态
    '''
    conn = get_mysql_conn(logger)
    with conn.cursor() as cr:
        if cr.execute(f"select t.site_name from tb_model_retrain_para t where t.id=("
                      f"select t2.para_id from tb_tasks t2 where t2.id='{task_id}')"):
            site_name = cr.fetchone()[0]
        else:
            site_name = None

    id_ = str(uuid.uuid1())
    model_id = info[0]
    ver = info[2]
    desc = info[5]
    conn.cursor().execute(f"insert into tb_model_version(`id`, `model_id`, `version_no`, `description`, `site_id`,"
                          f"`type_id`, `site_name`)"
                          f" values ('{id_}', '{model_id}', {ver}, '{desc}', '{info[6]}', '{info[7]}', '{site_name}')")
    conn.cursor().execute(f"update tb_tasks set `status`=2 where id='{task_id}'")
    conn.commit()
    conn.close()
    '''
    return 1


def train_state_one(task_id: str, logger):
    logger.info("最大风能捕获区的模型训练...")
    data = pd.read_csv(os.path.join(project.preprocessed_dir, f'train_data_1_tmp_{task_id}.csv'),
                       header=0, encoding='utf-8')
    if data.empty:
        logger.warning("最大风能捕获区无数据，无法进行训练")
        return 0
    logger.info("数据归一化")
    x, y = select_feature(data, "one", task_id)
    logger.info("模型训练及保存")
    train_and_save(x, y, "one", task_id)
    return 1


def train_state_three(task_id: str, logger):
    logger.info('恒定功率区的模型训练')
    data = pd.read_csv(os.path.join(project.preprocessed_dir, f"train_data_3_tmp_{task_id}.csv"),
                       header=0, encoding='utf-8')
    if data.empty:
        logger.warning("恒定功率区无数据，无法进行训练")
        return 0
    logger.info("数据归一化")
    x, y = select_feature(data, "three", task_id)
    logger.info("模型训练及保存")
    train_and_save(x, y, "three", task_id)
    return 1


def select_feature(data: pd.DataFrame, state: str, task_id: str):
    data = data.iloc[:, 8:]
    y = data['齿轮箱油温_avg']
    x = data.drop('齿轮箱油温_avg', axis=1)
    mms = MinMaxScaler()
    mms.fit(x)
    joblib.dump(mms, os.path.join(project.feature_dir, f"{state}_mms_tmp_{task_id}.m"))
    x = mms.transform(x)
    return x, y


def train_and_save(x: pd.DataFrame, y: pd.DataFrame, state: str, task_id: str):
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=.4)
    rf = RandomForestRegressor(n_estimators=10, oob_score=True, random_state=10, n_jobs=-1)
    rf.fit(x_train, y_train.values.ravel())
    joblib.dump(rf, os.path.join(project.model_dir, f"{state}_rf_tmp_{task_id}.m"))
    y_test_predict = rf.predict(x_test)
    loss = pd.DataFrame(y_test.to_list() - y_test_predict, columns=['loss_data'])
    loss.to_csv(os.path.join(project.threshold_dir, f"{state}_tmp_{task_id}.csv"),
                index=False, encoding='utf-8')
