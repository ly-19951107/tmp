# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/21 11:05
@Author   : Jarvis
"""
# hive数据库中sql语句
hive_data_sql = f"select wtg_id as `deviceid`, site as `siteid`, datatime as `localtime`, wtg_desc, wtg_mc, site_cn, province," \
    f"wgengenactivepwavg as `发电机有功功率_avg`, wgengenspdavg as `发电机转速_avg`, " \
    f"wnactemoutavg as `舱外温度_avg`, wnacwindspeedavg as `风速_avg`, wtrmrotorspdavg `风轮转速_avg`, " \
    f"wtrmtemgeaoilavg as `齿轮箱油温_avg`, wtrmtemgeamsdeavg as `齿轮箱高速轴驱动端轴承温度_avg`, " \
    f"wnacwindvanedirectionavg as `机舱与风向夹角_avg`, wrotblade1positionavg as `1#桨叶片角度(桨距角)_avg`, " \
    f"wtrmtrmtmpshfbrgavg as `主轴承温度_avg`, wyawtotaltwistavg as `偏航角度(扭缆角度）_avg`, " \
    f"wgentemgennondeavg as `发电机非驱动端轴承温度_avg`, wgentemgendriendavg as `发电机驱动端轴承温度_avg`, " \
    f"wtrmtemgeamsndavg as `齿轮箱高速轴非驱动端轴承温度_avg` " \
    f"from wind_stg_fact_wtg_10m_orc where site = \'%s\' and wtg_id= \'%s\' and " \
    f"yyyymmdd >= \'%s\' and yyyymmdd <= \'%s\'"

# 关系型数据库中sql语句
# 初始化表
tb_model_info = '''
CREATE TABLE `tb_model_info` (
  `id` varchar(255) CHARACTER SET utf8 NOT NULL COMMENT '模型的唯一标识',
  `model_name` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '模型的名称',
  `model_desc` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '模型功能的描述语句',
  `model_name_en` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '模型的英文名称',
  `type` int(2) DEFAULT NULL COMMENT '该模型是用于预测（0）还是重训练（1）',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储模型的基本信息，由于一个模型具有两个功能（预测、重训练），因此在存储时以两个模型的方式进行存储。';
'''
tb_model_version = '''
CREATE TABLE `tb_model_version` (
  `id` varchar(255) NOT NULL,
  `model_id` varchar(255) DEFAULT NULL COMMENT '引用的模型的id',
  `version_no` int(10) DEFAULT NULL COMMENT '模型的版本号',
  `description` varchar(255) DEFAULT NULL COMMENT '版本的描述性信息',
  `check_status` int(2) DEFAULT NULL COMMENT '图表审核的结果，0表示未通过，1表示通过',
  `new_model` int(2) DEFAULT 1 COMMENT '标识该版本是否为新增版本，默认为是（1），0表示成熟版本',
  `result` int(2) DEFAULT 0 COMMENT '标识该版本是否经过审核，默认为0（未审核），1标识已经过审核', 
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储模型版本信息的表。\r\n每一个模型都可以重复训练形成不同的版本。通过模型英文名称+版本号可以唯一确定一个模型。';
'''
tb_tasks = '''
CREATE TABLE `tb_tasks` (
  `id` varchar(255) NOT NULL,
  `model_id` varchar(255) DEFAULT NULL COMMENT '引用的模型的唯一标识',
  `version_id` varchar(255) DEFAULT NULL COMMENT '引用的模型版本表的唯一标识',
  `create_time` datetime DEFAULT NULL COMMENT '本任务的创建时间',
  `status` int(2) DEFAULT '0' COMMENT '任务的状态，分为：0:“未执行”、1:“执行中”、2:“执行完成”',
  `para_id` varchar(255) DEFAULT NULL COMMENT '参数表的id，在调用时指定了是哪一张参数表',
  `task_type` int(2) DEFAULT NULL COMMENT '任务类型，“0”代表是预测任务，“1”是重训练任务',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储基于模型生成的任务的表，通过该表的`para_id`可追溯到执行该任务的具体参数';
'''
tb_fault_results = '''
CREATE TABLE `tb_fault_results` (
  `id` varchar(255) NOT NULL,
  `task_id` varchar(255) DEFAULT NULL COMMENT '引用任务表的唯一主键',
  `wtg_id` varchar(255) DEFAULT NULL COMMENT '机组id',
  `warning_time` datetime DEFAULT NULL COMMENT '设备的预警时间',
  `run_time` datetime DEFAULT NULL COMMENT '任务执行结束的时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储任务执行结果的表';
'''
tb_model_predict_para = '''
CREATE TABLE `tb_model_predict_para` (
  `id` varchar(255) NOT NULL,
  `model_id` varchar(255) DEFAULT NULL COMMENT '引用的模型id',
  `version_id` varchar(255) DEFAULT NULL COMMENT '引用模型的版本表的唯一标识',
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `days` int(10) DEFAULT '5' COMMENT '连续的观察周期',
  `fault_times` int(10) DEFAULT '3' COMMENT '判定为故障时观察周期内发生故障的最小次数',
  `release_state` int(2) DEFAULT NULL COMMENT '发布状态，“0”表示该组参数并未发布成任务，“1”表示已经发布成任务',
  `site_id` varchar(255) DEFAULT NULL COMMENT '指定对哪个风场的的风机进行故障预测，如果不指定风场id，那么对于油温/轴温/本体模型均有各自默认的风场id',
  `wtg_id` blob COMMENT '存储风机的id拼接起来的长字符串，用英文逗号分隔。如果不指定，在油温/轴温/本体模型中均有对应的默认风机id列表',
  `wtg_type` varchar(20) DEFAULT NULL COMMENT '风机的型号。如果未指定，则油温/轴温/本体模型都有对应的默认型号',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储利用模型进行预测时的参数信息';
'''
tb_model_retrain_para = '''
CREATE TABLE `tb_model_retrain_para` (
  `id` varchar(255) NOT NULL,
  `model_id` varchar(255) DEFAULT NULL COMMENT '引用的模型表的唯一标识',
  `start_time` datetime DEFAULT NULL COMMENT '开始时间',
  `end_time` datetime DEFAULT NULL COMMENT '结束时间',
  `release_state` int(2) DEFAULT NULL COMMENT '发布状态，“0”表示该组参数并未发布成任务，“1”表示已经发布成任务',
  `description` varchar(255) DEFAULT NULL COMMENT '生成的模型的新版本的描述',
  `site_id` varchar(255) DEFAULT NULL COMMENT '指定用哪个风场的数据进行训练。如不指定，则油温/轴温/本体模型均有对应的默认风场',
  `wtg_id` blob COMMENT '指定用哪些风机进行训练。如不指定，则油温/轴温/本体模型均有默认的风机列表',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储利用模型进行重训练时的参数信息';
'''
tb_task_result_detail = '''
CREATE TABLE `tb_task_result_detail` (
  `id` varchar(255) NOT NULL,
  `task_id` varchar(255) DEFAULT NULL COMMENT '引用任务的id',
  `wtg_id` varchar(255) DEFAULT NULL COMMENT '引用风机的id',
  `value_time` datetime DEFAULT NULL COMMENT '时间戳',
  `true_temp1` float(5,2) DEFAULT NULL COMMENT '真实的温度(如果模型是轴温模型，则是轴1的温度)',
  `pred_temp1` float(5,2) DEFAULT NULL COMMENT '预测的温度(如果模型是轴温模型，则是轴1的温度)',
  `loss1` float(5,2) DEFAULT NULL COMMENT '温度残差值(如果模型是轴温模型，则是轴1的残差)',
  `true_temp2` float(5,2) DEFAULT NULL COMMENT '真实的温度(如果模型是轴温模型，则是轴2的温度)',
  `pred_temp2` float(5,2) DEFAULT NULL COMMENT '预测的温度(如果模型是轴温模型，则是轴2的温度)',
  `loss2` float(5,2) DEFAULT NULL COMMENT '温度的残差(如果模型是轴温模型，则是轴2的残差)',
  `mark` int(1) DEFAULT NULL COMMENT '标识为轴温模型（1）或者油温模型（0）',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='对于每一个任务（油温/轴温），记录该任务中所有风机的实际温度与预测问题，并计算残差。';
'''
tabs = ["tb_model_info", "tb_model_version", "tb_tasks", "tb_fault_results",
        "tb_model_predict_para", "tb_model_retrain_para", "tb_task_result_detail"]
sqls = {
    "tb_model_info": tb_model_info,
    "tb_model_version": tb_model_version,
    "tb_tasks": tb_tasks,
    "tb_fault_results": tb_fault_results,
    "tb_model_predict_para": tb_model_predict_para,
    "tb_model_retrain_para": tb_model_retrain_para,
    "tb_task_result_detail": tb_task_result_detail
}
