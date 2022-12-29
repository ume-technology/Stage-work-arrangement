### 文件说明与数据使用方案
- neworderdata: 优化师投放出来的产品的订单信息；
    - ~~自定义查询：read_tb_dwd_ord_gk_oder_info_crt_df.py；这个表的数据目前不再使用；~~；
      由云龙给的navicat的方式查询订单统计信息；
      云龙查询的结果存储在:read_tb_dwd_ord_gk_oder_info_crt_df_standard_facebook.csv
    
- nowaddata: 读取优化师数据信息（属于广告数据的一部分）-按照月度
    - 确认每个月份下所有家族的top优化师数据【基于传入月份信息查询】：
      nowaddata\get_opts_info_tb_dws_ord_order_si_crt_df\EachHomeTop10ByMonth.py
    - 投放出去的产品的日报信息，因为为优化师投放的产品必定对应该产品的信息；这个数据就是产品信息表：
      nowaddata\read_gdsc_gk_product_report
    - important:
      上述优化师基于月份的分析与计算，集中在处理得到优化师投放的商品信息：下面代码是优化师和商品数据计算的重要代码：
      这里处理通用的 product sell report + 有针对性的月度top优化师之间的关系都在这个脚本中；
      nowaddata\get_opts_info_tb_dws_ord_order_si_crt_df\EachHomeTop10ByMonth_11.py
 
- nowgoodstags: 
    - giikin standard tags：注意用来抽取这些tags的数据来源；
    - ner tags
    - merge
    - save_merge_tags: unified merged tags data
      
### 20221221代码调整
wait todo:
    另外还有需要做的是工作是优化师舍弃或者调整的广告系列数据；
    做更多的广告系列数据的对比；
    广告成本数据；
    商品标签数据没有接入知识图谱；因为图谱还有很多工作要做；