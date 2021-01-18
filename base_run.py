# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/5/16 10:18
@Author   : Jarvis
"""
from main_code.gen_logger import get_logger
from main_code.data_base import get_train_task_info, get_predict_task_info, check_time, update_status, \
    rm_version
from main_code.data_processing import process_retrain_data, process_predict_data
from main_code.training import train_model
import argparse
import time


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=str, help='指定任务的id用于训练')
    parser.add_argument('--predict', type=str, help='指定任务的id用于预测')
    parser.add_argument('--rm-version', type=int, help='删除指定的版本')

    args = parser.parse_args()
    if args.train:
        # 模型训练
        task_id = args.train
        logger = get_logger(task_id)
        info = get_train_task_info(task_id, logger)
        # info = [0:原模型的id, 1:原模型的名称, 2:基础版本, 3:训练数据开始时间, 4:训练数据结束时间, 5:模型描述, 6:风场id,
        # 7:机型id, 8:风机id]
        info[2] = time.strftime("%Y%m%d%H%M%S", time.localtime())
        if not info:
            logger.warning('无法执行重训练任务！')
            logger.warning('状态重置！')
            update_status(task_id, 2)
        else:
            # 校验训练数据的时间范围与故障样本的时间范围是否有交集
            fault_data = check_time(info[3], info[4], info[6])
            if not fault_data:
                logger.warning(f"当前时间范围内,{info[6]}风场的数据没有故障记录！")
            flag = process_retrain_data(task_id, info, logger, fault_data)
            if flag == 3:
                update_status(task_id, 3)
            elif not flag:
                logger.error("处理风机数据出错")
                logger.info("状态重置")
                update_status(task_id, 2)
            else:
                flag = train_model(task_id, info, logger)
                if not flag:
                    update_status(task_id, 2)
        logger.info("训练任务结束")

    if args.predict:
        # 模型预测
        task_id = args.predict
        logger = get_logger(task_id)
        info = get_predict_task_info(task_id, logger)
        # info = [模型名称,模型的版本号,预测数据集的开始时间,预测数据集的终止时间,观察窗口（天）,最低故障次数,风场id,机型id,风机id,
        # 模型版本的id]
        if not info:
            logger.warning('无法执行预测任务！')
            logger.warning('状态重置！')
            update_status(task_id, 2)
        else:
            flag = process_predict_data(task_id, info, logger)
            if flag == 3:
                update_status(task_id, 3)
            elif flag:
                update_status(task_id, 2)
            else:
                logger.error("处理风机数据出错")
                logger.info("状态重置")
                update_status(task_id, 2)
        logger.info("预测任务结束")

    if args.rm_version:
        ver = args.rm_version
        rm_version(ver)
