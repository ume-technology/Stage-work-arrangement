# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:EachHomeTop10ByMonth.py
@Time:2022/11/28 17:38
@todo: 读取的是高度汇总表；拿到的是优化师的第一手相关的数据信息；
       可以指定平台读取目标平台各个家族的Top优化师；
       读取这个数据，主要是将优化师和其产品/商品/广告系列/广告组等数据做关联，以获取优化师投放的商品的情况；
       ----------------------------------------------------------------------------------
       note：
            在~~每个月基于所有家族的的Top优化师s.sql~~文件中，
            不能准确地找到目标优化师s的投放数据；因此在这里采用这个脚本中的sql进行top优化师s相关信息的查询。
       note：
            20221221：目前查询优化师的方案也没有使用这个sql；而是使用了云龙给出的查询优化师的方案；
"""
from functions import *

# todo 读取不同月份下，各个家族的Top10的优化师信息
# sql = """
# select * from (
# select family_name, opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
# select  family_name,opt_name, sum(effect_order_cnt) orders, opt_id
# -- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
#  from giikin_aliyun.tb_dws_ord_order_si_crt_df where pt>='20221101' and pt<='20221127'
#  and befrom='facebook'
#  and opt_name is not null
#  group by  family_name,opt_name) a
#  group by family_name,opt_name,orders) a
#  where rank <= 10
# """

# todo 读取不同月份下，各个家族的Top5的优化师信息
sql = """
select * from (
select family_name,opt_id,opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
select  family_name,opt_id,opt_name, befrom, sum(effect_order_cnt) orders
-- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
 from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221101' and   pt<='20221130' and  befrom='facebook' -- 11月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221001' and   pt<='20221031' and  befrom='facebook' -- 10月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220901' and   pt<='20220930' and  befrom='facebook' -- 9月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220801' and   pt<='20220831' and  befrom='facebook' -- 8月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220701' and   pt<='20220731' and  befrom='facebook' -- 7月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220601' and   pt<='20220630' and  befrom='facebook' -- 6月份
  --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220501' and   pt<='20220531' and  befrom='facebook' -- 5月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220401' and   pt<='20220430' and  befrom='facebook' -- 4月份
--from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220301' and   pt<='20220331' and  befrom='facebook' -- 3月份
 
-- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20211201' and   pt<='20211231' and  befrom='facebook' -- 2021 12月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20211101' and   pt<='20211130' and  befrom='facebook' -- 2021 11月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20211001' and   pt<='20211031' and  befrom='facebook' -- 2021 10月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210901' and   pt<='20210930' and  befrom='facebook' -- 2021 9月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210801' and   pt<='20210831' and  befrom='facebook' -- 2021 8月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210701' and   pt<='20210731' and  befrom='facebook' -- 2021 8月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210601' and   pt<='20210630' and  befrom='facebook' -- 2021 6月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210501' and   pt<='20210531' and  befrom='facebook' -- 2021 5月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210401' and   pt<='20210430' and  befrom='facebook' -- 2021 4月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210301' and   pt<='20210331' and  befrom='facebook' -- 2021 3月份
 and opt_name is not null
 group by  family_name,opt_id,opt_name, befrom) a 
 group by family_name,opt_id,opt_name,orders) a 
 where rank<=5
"""

with o.execute_sql(sql).open_reader(tunnel=True) as reader:
    all_top_apt = reader.to_pandas()

# todo 通过优化师ID找到优化师姓名
# df = all_top_apt.loc[all_top_apt['opt_id'] == '2055', 'opt_name'].to_numpy()[0]
