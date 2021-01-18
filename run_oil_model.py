# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/9 17:41
@Author   : Jarvis
"""
import os
import argparse

TRAIN_ERROR_PATH = '/code/errors/train'
PREDICT_ERROR_PATH = '/code/errors/predict'
if not os.path.exists(TRAIN_ERROR_PATH):
    os.makedirs(TRAIN_ERROR_PATH)
if not os.path.exists(PREDICT_ERROR_PATH):
    os.makedirs(PREDICT_ERROR_PATH)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=str, help='指定任务的id用于训练')
    parser.add_argument('--predict', type=str, help='指定任务的id用于预测')
    parser.add_argument('--rm-version', type=int, help='删除指定的版本')

    args = parser.parse_args()
    if args.train:
        # 模型训练
        task_id = args.train
        os.popen(f"nohup python /code/base_run.py --train {task_id} > {TRAIN_ERROR_PATH}/{task_id}.out 2>&1 &")

    if args.predict:
        # 模型预测
        task_id = args.predict
        os.popen(f"nohup python /code/base_run.py --predict {task_id} > {PREDICT_ERROR_PATH}/{task_id}.out 2>&1 &")
