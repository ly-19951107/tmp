# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/21 11:12
@Author   : Jarvis
"""
import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
import os
from main_code.config import project_root_path
# import time


def get_logger(task_id):
    path = os.path.join(project_root_path, 'logs')
    if not os.path.exists(path):
        os.makedirs(path)
    # today_date = time.strftime("%Y-%m-%d", time.localtime())
    log_file_name = os.path.join(path, task_id + '.log')
    logger = logging.getLogger('rootLogger')
    logger.setLevel(logging.INFO)
    if not logger.handlers:  # 避免重复添加handler
        console = StreamHandler()
        handler = RotatingFileHandler(log_file_name, maxBytes=2*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(filename)s %(funcName)s %(lineno)s | %(message)s ",
                                      datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        console.setFormatter(formatter)
        logger.addHandler(handler)
        # logger.addHandler(console)  # 注释掉此行，以避免在控制台打印日志信息
    return logger
