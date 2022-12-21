# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:EachHomeTop10ByMonth_11.py
@Time:2022/11/26 15:23
@Read: # todo 确定月份top优化师 - 2022-11
        基于该数据统计的基础思路：优化师/广告/产品
        1.首先读取了各个家族的Top优化师s的ID数据；（这里有一个依赖是每个月各团队的Top优化师s可能会不同，因此要注意月份）
        2.然后读取了所有投放在FB平台的产品，匹配了这些优化师投放在FB平台的都有哪些产品（无月份依赖）；
        3.最后读取了这些Top优化师投放出来的广告系列都有哪些（无月份依赖）；
"""

from nowaddata.read_gdsc_gk_product_report.read_gdsc_gk_product_report import gk_product_reporter_df, ader_id_groups  # 获取优化师投放的广告涉及的商品的report

# todo 从订单数据观察优化师投放的商品信息
# """ todo
#        1 商品标签中somedemo\dict_info.dict已经包含了商品的广告信息，虽然不全；
#        2 指定月份的订单数据；
#        3 important
#               优化师数据: 每个月表现好的优化师是谁；进而这个优化师在当前月投放的商品都有哪些；
#               然后到某个商品表现好的时候的具体的一些指标：
#               广告素材：
#                   旧时的广告语
#                   【新生成的广告文本】
#               出单情况；
#               推荐【目前存在不全】
#               素材；
#               广告文本的生成
# 1、获取各家族FB平台各月订单量TOP5优化师名下的广告系列id
# 2、所需字段：产品id、商品id、广告系列id、广告系列名称、出单量、订单日期、出单地区
# """
import pickle
import pandas as pd

# todo read tags by graph
with open('../../nowgoodstags/save_mergea_data/unified_joined_new/all.pick', 'rb') as f:
    tags = pickle.load(f)
with open('../../nowgoodstags/save_mergea_data/unified_joined_no_new/all_no.pick', 'rb') as f:
    tags_no = pickle.load(f)

# todo 确定优化师投放的商品   202211月份 - 筛选特定家族的优化师
# from nowaddata.get_opts_info_tb_dws_ord_order_si_crt_df.EachHomeTop10ByMonth import all_top_apt  # 按月度获取优化师的信息
# 火凤凰Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "火凤凰家族"]['opt_id'].to_list()
# 神龙Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "神龙家族"]['opt_id'].to_list()
# 精灵Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "精灵家族"]['opt_id'].to_list()
# 红杉Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "红杉家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
# 金牛Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "金牛家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
# 金狮Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "金狮家族"]['opt_id'].to_list()
# 金蝉Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "金蝉家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
# 雪豹Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "雪豹家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
# 飞马Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "飞马家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
# alltopsid = 火凤凰Top优化师sID_11 + 神龙Top优化师sID_11 + 精灵Top优化师sID_11 + \
#             红杉Top优化师sID_11 + 金牛Top优化师sID_11 + 金狮Top优化师sID_11 + 金蝉Top优化师sID_11 + 雪豹Top优化师sID_11 + 飞马Top优化师sID_11

# todo 该数据文件由navicat执行导出 - 11月度下的订单数据
data = pd.read_csv('../../noworderdata/read_tb_dwd_ord_gk_oder_info_crt_df_standard_facebook.csv')
res = data.dropna(subset=['sum(is_effective_order)'], axis=0)
res['sum(is_effective_order)'] = res['sum(is_effective_order)'].astype('int')
monthgroups_optid_orders = res.groupby(['statis_month', 'opt_id']).groups
all_opts_orders_thismonth = ''
for k, v in monthgroups_optid_orders.items():
    if k[0] == 202211:
        if isinstance(all_opts_orders_thismonth, str):
            all_opts_orders_thismonth = res.loc[v]
        else:
            all_opts_orders_thismonth = pd.concat([all_opts_orders_thismonth, res.loc[v]], axis=0)

# todo 各个线路上月度表现最好的品20个品
# all_opts_orders_thismonth_prtorders_count = all_opts_orders_thismonth.groupby(by=['line_name', 'product_id'])['sum(is_effective_order)'].sum().to_frame()
# all_opts_orders_thismonth_prtorders_count = all_opts_orders_thismonth.groupby(by=['line_name', 'product_id'])['sum(is_effective_order)'].sum()
all_opts_orders_thismonth = all_opts_orders_thismonth.groupby(['line_name', 'product_id']).agg({"sum(is_effective_order)": "sum"})
all_opts_orders_thismonth.reset_index(level=0, inplace=True)
all_opts_orders_thismonth.reset_index(level=0, inplace=True)
each_line_mostpop = {}
for i in all_opts_orders_thismonth.itertuples():
    product_id = getattr(i, 'product_id')
    line_name = getattr(i, 'line_name')
    order_count = i[-1]
    if not each_line_mostpop.get(line_name):
        each_line_mostpop[line_name] = []
        each_line_mostpop[line_name].append({'product_id': product_id, 'count': order_count})
    else:
        each_line_mostpop[line_name].append({'product_id': product_id, 'count': order_count})

orders_sort = {}
for k, v in each_line_mostpop.items():
    orders_sort[k] = sorted(v, key=lambda x: x['count'], reverse=True)

# todo 品类推荐 - 出单量前20的产品
finale_data = {}
for line, prt_orders in orders_sort.items():
    c = 0
    finale_data[line] = []
    for i in prt_orders:
        proid = i['product_id']  # orders
        for ok in tags[5]:
            proId = ok.get('product_id')  # tags
            if proid == proId:
                finale_data[line].append({'product_id': proid, '投放推送': ok})
                break
        c += 1
        if c == 20:
            break
