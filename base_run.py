# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code
@Time     : 2020/5/16 10:18
@Author   : Jarvis
"""
from main_code.gen_logger import get_logger
from main_code.data_base import get_train_task_info, get_predict_task_info, check_time, update_status, \
    rm_version
from main_code.data_processing import process_retrain_data, process_predict_data, get_site_and_type
from main_code.training import train_model
import argparse
import datetime


def train(task_key, info_list, site, type_, logger):
    """执行预测任务时，首先基于基础版本进行训练得到一个临时版本，然后根据此临时版本
    进行预测"""
    pred_start_time = info_list[2]
    # 选取预测任务的数据开始时间的前150天的数据为训练数据
    time_interval = datetime.timedelta(days=150)
    train_start_time = info[11]
    train_end_time = info[12]
    if not info[11] and not info[12]:
        train_start_time = pred_start_time - time_interval
        train_end_time = pred_start_time
    train_info = [None, None, 1, train_start_time, train_end_time, None, info_list[6],
                  info_list[7], info_list[8]]
    fault_data = check_time(train_info[3], train_info[4], train_info[6])
    if not fault_data:
        logger.warning(f"当前时间范围内,{train_info[6]}风场的数据没有故障记录！")
    flag1 = process_retrain_data(task_id, train_info, site, type_, logger, fault_data)
    if flag1 == 1:
        return train_model(task_key, train_info, logger)


def get_site_type(info1, pro):
    site_id, type_id, wtg_id = info1[6:9]
    if not site_id:
        logger.info("未指定风场，默认计算该省份下所有的风场")
        site_id_list = None
    else:
        site_id_list = list(site_id.split(','))
        logger.info(f"指定了风场，共{len(site_id_list)}个")
    if not type_id:
        logger.info('未指定机型，对该风场下所有的风机进行训练')
        type_id_list = None
    else:
        type_id_list = list(type_id.split(','))
        logger.info(f"指定了机型，共{len(type_id_list)}种")

    logger.info("将风场与对应型号进行匹配...")
    site_type = get_site_and_type(pro, site_id_list, type_id_list)
    logger.info("匹配完成")
    if not site_type:
        logger.info("当前省份风场无可用机型")
        return 0
    logger.info(f"实际共需计算{len(site_type)}个风场")
    return site_type


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=str, help='指定任务的id用于训练')
    parser.add_argument('--predict', type=str, help='指定任务的id用于预测')
    parser.add_argument('--rm-version', type=int, help='删除指定的版本')

    args = parser.parse_args()
    # 重训练任务不再单独被调用

    # if args.predict:
    # 模型预测
    task_id = args.predict
    # task_id = '38d7077f-845a-4beb-b3d4-cb6a6d4f3d31'
    logger = get_logger(task_id)
    info = get_predict_task_info(task_id, logger)
    # info = [模型名称,模型的版本号,预测数据集的开始时间,预测数据集的终止时间,观察窗口（天）,最低故障次数,风场id,
    # 机型id,风机id, 模型版本的id, 省份,训练数据集的开始时间,训练数据集的终止时间]
    if not info:
        logger.warning('无法执行预测任务！')
        logger.warning('状态重置！')
        update_status(task_id, 2)
    else:
        province_id_list = info[10].split(',')
        for pro in province_id_list:
            site_types = get_site_type(info, pro)
            if site_types == 0:
                logger.error("处理风机数据出错")
                logger.info("状态重置")
                update_status(task_id, 2)
            else:
                for site in site_types:
                    for type1 in site_types[site]:
                        # 预测前先执行训练任务
                        logger.info(f"当前计算{pro}省份/{site}风场/{type1}机型...")
                        train_res = train(task_id, info, site, type1, logger)
                        if train_res:  # 生成了临时模型
                            logger.info('成功生成临时模型')
                        else:  # 未生成临时模型，则用基础模型预测
                            logger.info('未生成临时模型，调用基础模型')
                        flag = process_predict_data(task_id, info, site, type1, logger, train_res)
                        if flag == 3:
                            update_status(task_id, 3)
                        elif flag:
                            update_status(task_id, 2)
                        else:
                            logger.error("处理风机数据出错")
                            logger.info("状态重置")
                            update_status(task_id, 2)

                    logger.info(f"风场'{site}'预测任务结束")
        logger.info('Complete!')


