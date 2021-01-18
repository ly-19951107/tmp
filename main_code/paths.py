# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/21 11:34
@Author   : Jarvis
"""
import os
from main_code.config import project_root_path


class Path:
    def __init__(self, root):
        self.root = root
        self.preprocessed = os.path.join(self.root, 'preprocessed')  # 存储处理完成的数据的目录
        self.feature = os.path.join(self.root, 'features')  # 存储特征抽取器的目录
        self.model = os.path.join(self.root, 'trained_models')  # 存储训练好的模型的目录
        self.threshold = os.path.join(self.root, 'threshold')  # 存储训练后的模型的阈值文件

    @property
    def preprocessed_dir(self):
        return creat_path(self.preprocessed + os.path.sep)

    @property
    def feature_dir(self):
        return creat_path(self.feature + os.path.sep)

    @property
    def model_dir(self):
        return creat_path(self.model + os.path.sep)

    @property
    def threshold_dir(self):
        return creat_path(self.threshold + os.path.sep)


def creat_path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


project = Path(project_root_path)
