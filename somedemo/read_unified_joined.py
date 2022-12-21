# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:read_unified_joined.py
@Time:2022/12/15 16:55
@Read: 
"""
import pickle

# with open('joinAD_product.pick', 'rb') as f:
#     joinda = pickle.load(f)
#
# with open('joinAD_product_no.pick', 'rb') as f:
#     joinda_no = pickle.load(f)

# with open('../nowgoodstags/save_mergea_data/unified_joined/0-25000tags.pick', 'rb') as f:
#     data = pickle.load(f)

# with open('dict_info.dict', 'wb') as f:
#     pickle.dump(data[5], f)

# with open('../nowgoodstags/save_mergea_data/unified_joined_no/0-50000.pick', 'rb') as f:
#     data_no = pickle.load(f)

with open('../nowgoodstags/save_mergea_data/unified_joined_new/all.pick','rb') as f:
    data = pickle.load(f)
