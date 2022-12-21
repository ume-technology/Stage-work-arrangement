### 文件说明
- neworderdata: 读取订单信息数据以及该订单的优化师信息数据；
    - 自定义查询：read_tb_dwd_ord_gk_oder_info_crt_df.py；这个表的数据目前不再使用；因为下述文件已经包含了top优化师信息；
    - 标准查询订单数据方案：data_tb_dwd_ord_gk_oder_info_crt_df_standard.csv - 根据外部sql - 按照月度查询
    
- nowaddata: 读取优化师数据信息（属于广告数据的一部分）-按照月度
    - 确认每个月份下所有家族的top优化师：nowaddata\get_opts_info_tb_dws_ord_order_si_crt_df\EachHomeTop10ByMonth.py
      ```python   
        投放出去的产品的日报信息：nowaddata\read_gdsc_gk_product_report
        + 
        上述优化师基于月份的分析与计算：nowaddata\get_opts_info_tb_dws_ord_order_si_crt_df\EachHomeTop10ByMonth_11.py
        important: 这里处理通用的 product sell report + 有针对性的月度top优化师之间的关系都在这些脚本中；
      ```
- 