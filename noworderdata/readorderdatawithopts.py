# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:readorderdatawithopts.py
@Time:2022/12/19 13:00
@Read: 读取优化师的订单数据
       # fixme pymysql 不支持执行上述sql；因为开窗函数的原因
"""
from functions import *

sql = """
select a.statis_month
      ,a.opt_id
      ,a.family_name
      ,b.product_id
      ,b.sale_id
      ,b.card_api
      ,b.ship_country
      ,c.line_name
      ,b.ship_city
      ,sum(is_effective_order) 
from (select statis_month
            ,opt_id
            ,family_name
            ,effect_order_cnt
            ,ROW_NUMBER() OVER(PARTITION BY family_name,statis_month ORDER BY effect_order_cnt desc) AS COL3 
      from (select statis_month
                  ,opt_id
                  ,family_name
                  ,sum(effect_order_cnt) as effect_order_cnt
            from tb_dws_ord_order_si_mi 
            where statis_month >='202207'
            and opt_id <> 0
            group by statis_month, opt_id, family_name
            ) aa
      ) a
left join tb_dwd_ord_gk_order_info_crt_df b
on a.statis_month = b.statis_month and a.opt_id = b.opt_id and a.family_name = b.family_name and b.statis_month >='202207'
left join tb_dim_fin_gk_currency_df c
on b.ship_country = c.line_code 
where a.COL3 >= 5 
group by a.statis_month,a.opt_id,a.family_name,b.product_id,b.sale_id,b.card_api,b.ship_country,b.ship_city,c.line_name limit 100
"""
# cursor = conn.cursor()
# count_ = cursor.execute(sql)
# data_order_Counts_with_opts = pd.read_sql(sql, conn)
