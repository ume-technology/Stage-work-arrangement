# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:readdata.py
@Time:2022/12/15 16:41
@Read: 
"""
import pickle

with open('../nowgoodstags/nownerdata/middata_from_model_predict/matgoodsinfo_goodsfeatures', 'rb') as f:
    data = pickle.load(f).head(1000)


