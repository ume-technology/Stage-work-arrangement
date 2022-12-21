# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:read_unified_joined.py
@Time:2022/12/15 16:55
@Read: 
"""
import pickle

with open('../bigfiles/unified_joined_new/all.pick', 'rb') as f:
    data = pickle.load(f)
