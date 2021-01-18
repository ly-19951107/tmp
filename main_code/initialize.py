# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/21 11:09
@Author   : Jarvis
"""
from main_code.sql_file import tabs, sqls
from main_code.data_base import get_mysql_conn, db_info


insert_sql = "insert into `tb_model_info` VALUES(%s, %s, %s, %s, %s)"
values = [('298e66d5-3420-41d3-8a0e-f4578a962769', '本体模型', '用于重训练生成新的本体预警模型', 'major', '1'),
          ('70041dec-3fb2-4ce0-a4fd-e9bcdd87792d', '轴温模型', '轴温故障预警模型', 'shaft', '0'),
          ('7a760093-3dac-43a3-bc5d-52d10c1640f9', '油温模型', '用于重训练生成新的油温预警模型', 'oil', '1'),
          ('7cb1ca48-e28e-4735-bbc5-a64fae2e9d0a', '轴温模型', '用于重训练生成新的轴温预警模型', 'shaft', '1'),
          ('87164820-d57f-4ab8-999d-ce88a8f98015', '本体模型', '本体故障预警模型', 'major', '0'),
          ('ac740815-ec9a-4c17-8dd5-6b5dcbab3a67', '油温模型', '油温故障预警模型', 'oil', '0')]


def init_table(logger):
    mysql_conn = get_mysql_conn(logger)
    cur = mysql_conn.cursor()
    is_exist_sql = "select table_name from information_schema.tables where table_schema='%s' and table_name='%s'"
    for table in tabs:
        if cur.execute(is_exist_sql % (db_info['db'], table)):
            pass
        else:
            logger.info(f'创建表{table}')
            sql = sqls[table]
            cur.execute(sql)
            if table == 'tb_model_info':
                cur.executemany(insert_sql, values)
            mysql_conn.commit()
