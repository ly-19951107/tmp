# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/5/13 14:45
@Author   : Jarvis
"""
import os
import argparse
import re
from main_code.gen_logger import get_logger


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--kill', type=str, help='指定需要终止的任务id')
    args = parser.parse_args()
    task_id = args.kill
    logger = get_logger(task_id)
    logger.warning(f"任务{task_id}正在被终止")
    processes = os.popen("ps -ef | grep python").readlines()

    for p in processes:
        if len(p) < len(task_id):
            continue
        if p.endswith(task_id + '\n'):
            span = re.search(r"\s\d+\s", p).span()
            num = p[span[0]: span[1]]
            os.popen(f"kill -9 {num}")
            logger.warning(f"任务{task_id}已被终止")
