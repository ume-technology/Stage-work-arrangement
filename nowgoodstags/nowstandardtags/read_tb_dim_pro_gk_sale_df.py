# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@Blog: https://www.umeai.top/
@File:read_tb_dim_pro_gk_sale_df.py
@Time:2022/12/21 21:48
@ReadMe: 获取标准的商品信息（产品信息）；和产品维表的作用一致
"""
from functions import *

sql = """
select product_id,product_name from giikin_aliyun.tb_dim_pro_gk_sale_df
"""

# with o.execute_sql(sql).open_reader(tunnel=True) as reader:
#     all_top_apt = reader.to_pandas()
#
# import pickle
#
# with open('../../bigfiles/all_goods_info.pick', 'wb') as f:
#     pickle.dump(all_top_apt, f)
