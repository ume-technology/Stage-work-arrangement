# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:unified_tags_cp.py
@Time:2022/12/15 17:51
@Read: 多拉一个文件；读取 joined_no
"""
# import re
import pickle

# import jieba
# from openbg500L.read500l import *

# from nowgoodstags.save_mergea_data.unified_tags import read_nodes_base_product

# todo 因为在构建商品标签的过程中使用这个数据时，针对产品类别的确认不合理，因此这里再读pro-dim的数据，补全这个产品类别信息
with open('../nowstandardtags/read_and_prepare_basetagsdata/tb_dim_pro_gk_product_df.pick', 'rb') as fprodim:
    prodim = pickle.load(fprodim)

with open('./joinAD_product_no.pick', 'rb') as f:
    tagswithoutner = pickle.load(f)


# todo 创建 Concept
def read_nodes_base_product(nertags_dimprotags_TAGS):
    # important    concept:里面包含子分类；但是定义concept node时体现不出来各个概念子分类的存在，子分类直接存储在spo中
    # category = []  # cat1/cat2/cat3
    scene = []  # scene/market
    crowd = []  # mom/worker/student/carer
    season = []  # season/age/day  wait day节日
    placeorbody = []  # outdoor/indoor/sky  or  eye legs ....
    brands = []

    rel_category_scene = []
    rel_category_crowd = []
    rel_category_time = []
    rel_category_placeorbody = []
    rel_category_brand = []

    product_info = []
    # for eachprtwithtags in nertags_dimprotags_TAGS[30840:]:
    for eachprtwithtags in nertags_dimprotags_TAGS[:]:
        prt_dict = {}
        prt_dict['商品卖点'] = []
        prt_dict['适配人群或对象'] = []
        prt_dict['商品材质或材料'] = []  # 商品材料
        prt_dict['适用场地或部位'] = []
        prt_dict['商品功能'] = []
        prt_dict['应用时间'] = []
        prt_dict['搭配方案'] = []
        for key in list(eachprtwithtags):
            if eachprtwithtags[key] == 'null ' or eachprtwithtags[key] == -1:
                eachprtwithtags.pop(key)
                continue
            if not eachprtwithtags[key]:
                eachprtwithtags.pop(key)
                continue
            if eachprtwithtags[key] is None:
                eachprtwithtags.pop(key)
                continue
            if isinstance(eachprtwithtags[key], int) or isinstance(eachprtwithtags[key], float):
                prt_dict[key] = eachprtwithtags.pop(key)
                continue
            if key == 'category_lvl3_name_giikin' or key == 'category_lvl3_id_giikin' or key == 'product_name_giikin' or key == '归一化产品' or key == '商品产地【虚假】':
                eachprtwithtags.pop(key)
                continue
            prt_dict[key] = eachprtwithtags.get(key)

        keys = eachprtwithtags.keys()
        # property base property
        each_prt = eachprtwithtags.get('product_name')
        if each_prt is not None:
            each_prt = eachprtwithtags.pop('product_name')
        each_id = eachprtwithtags.get('product_id')
        if each_id is not None:
            each_id = eachprtwithtags.pop('product_id')
        if each_id is None:
            each_id = prodim['product_id']
        if each_prt is None:
            each_prts = prodim.loc[prodim['product_id'] == each_id]
            each_prt = each_prts['product_name'].tolist()[0]
        prt_dict['prt_name'] = each_prt
        prt_dict['prt_id'] = each_id

        # 需要归并故pop自己内部需要归并的标签
        if '元素标签' in keys:
            popv = prt_dict.pop('元素标签')
            if isinstance(popv, str):
                prt_dict['商品卖点'].append(popv)
            else:
                prt_dict['商品卖点'] += set(popv)
        if '特色卖点' in keys:
            popv = prt_dict.pop('特色卖点')
            if isinstance(popv, str):
                prt_dict['商品卖点'].append(popv)
            else:
                prt_dict['商品卖点'] += set(popv)
        if '卖点提取' in keys:
            popv = prt_dict.pop('卖点提取')
            if isinstance(popv, str):
                prt_dict['商品卖点'].append(popv)
            else:
                prt_dict['商品卖点'] += set(popv)
        # 需要归并，故需要pop自己内部的该信息；   property  material
        if '商品材料' in keys:
            popv = prt_dict.pop('商品材料')
            if isinstance(popv, str):
                prt_dict['商品材质或材料'].append(popv)
            else:
                prt_dict['商品材质或材料'] += set(popv)
        if '是否含糖' in keys:
            popv = prt_dict.pop('是否含糖')
            if isinstance(popv, str):
                prt_dict['商品材质或材料'].append(popv)
            else:
                prt_dict['商品材质或材料'] += set(popv)
        if '材质' in keys:
            popv = prt_dict.pop('材质')
            if isinstance(popv, str):
                prt_dict['商品材质或材料'].append(popv)
            else:
                prt_dict['商品材质或材料'] += set(popv)
        if '材质标签' in keys:
            popv = prt_dict.pop('材质标签')
            if isinstance(popv, str):
                prt_dict['商品材质或材料'].append(popv)
            else:
                prt_dict['商品材质或材料'] += set(popv)
        if '材质材料' in keys:
            popv = prt_dict.pop('材质材料')
            if isinstance(popv, str):
                prt_dict['商品材质或材料'].append(popv)
            else:
                prt_dict['商品材质或材料'] += set(popv)

        # 需要归并故pop自己内部需要归并的标签      property functions
        if '功能' in keys:
            popv = prt_dict.pop('功能')
            if isinstance(popv, str):
                prt_dict['商品功能'].append(popv)
            else:
                prt_dict['商品功能'] += set(popv)
        if '功能功效' in keys:
            popv = prt_dict.pop('功能功效')
            if isinstance(popv, str):
                prt_dict['商品功能'].append(popv)
            else:
                prt_dict['商品功能'] += set(popv)
        if '功能标签' in keys:
            popv = prt_dict.pop('功能标签')
            if isinstance(popv, str):
                prt_dict['商品功能'].append(popv)
            else:
                prt_dict['商品功能'] += set(popv)
        if '功能类型' in keys:
            popv = prt_dict.pop('功能类型')
            if isinstance(popv, str):
                prt_dict['商品功能'].append(popv)
            else:
                prt_dict['商品功能'] += set(popv)

        # property pl
        if '搭配品类' in keys:
            popv = prt_dict.pop('搭配品类')
            if isinstance(popv, str):
                prt_dict['搭配方案'].append(popv)
            else:
                prt_dict['搭配方案'] += set(popv)
        # property dp
        if '商品搭配' in keys:
            popv = prt_dict.pop('商品搭配')
            if isinstance(popv, str):
                prt_dict['搭配方案'].append(popv)
            else:
                prt_dict['搭配方案'] += set(popv)

        # node scene
        if '适用场景' in keys:
            if isinstance(eachprtwithtags.get('适用场景'), str):
                scene.append(eachprtwithtags.pop('适用场景'))
            else:
                scene += eachprtwithtags.pop('适用场景')
            for i in set(prt_dict.get('适用场景')):
                rel_category_scene.append([each_prt, i])

        # 需要合并故pop     node crowd
        if '适用人群' in keys:
            popv = prt_dict.pop('适用人群')
            if isinstance(popv, str):
                prt_dict['适配人群或对象'].append(popv)
                crowd.append(popv)
            else:
                prt_dict['适配人群或对象'] += set(popv)
                crowd += set(popv)
        if '人群标签' in keys:
            popv = prt_dict.pop('人群标签')
            if isinstance(popv, str):
                prt_dict['适配人群或对象'].append(popv)
                crowd.append(popv)
            else:
                prt_dict['适配人群或对象'] += set(popv)
                crowd += set(popv)
        if '使用人群' in keys:
            popv = prt_dict.pop('使用人群')
            if isinstance(popv, str):
                prt_dict['适配人群或对象'].append(popv)
                crowd.append(popv)
            else:
                prt_dict['适配人群或对象'] += set(popv)
                crowd += set(popv)
        if '商品适用对象' in keys:
            popv = prt_dict.pop('商品适用对象')
            if isinstance(popv, str):
                prt_dict['适配人群或对象'].append(popv)
                crowd.append(popv)
            else:
                prt_dict['适配人群或对象'] += set(popv)
                crowd += set(popv)
        for i in set(prt_dict['适配人群或对象']):
            rel_category_crowd.append([each_prt, i])

        # 需要合并故pop  node season
        if '季节标签' in keys:
            popv = prt_dict.pop('季节标签')
            if isinstance(popv, str):
                prt_dict['应用时间'].append(popv)
                season.append(popv)
            else:
                prt_dict['应用时间'] += set(popv)
                season += set(popv)
        if '适合季节' in keys:
            popv = prt_dict.pop('适合季节')
            if isinstance(popv, str):
                prt_dict['应用时间'].append(popv)
                season.append(popv)
            else:
                prt_dict['应用时间'] += set(popv)
                season += set(popv)
        if '适用年龄段' in keys:
            popv = prt_dict.pop('适用年龄段')
            if isinstance(popv, str):
                prt_dict['应用时间'].append(popv)
                season.append(popv)
            else:
                prt_dict['应用时间'] += set(popv)
                season += set(popv)
        for i in set(prt_dict.get('应用时间')):
            rel_category_time.append([each_prt, i])

        # node brands
        if '品牌' in keys:
            brand = eachprtwithtags.pop('品牌')
            brands.append(brand)
            rel_category_brand.append([each_prt, brand])

        # 需要合并故pop   node location
        if '适用地点与场景' in keys:
            popv = prt_dict.get('适用地点与场景')
            if isinstance(popv, str):
                prt_dict['适用场地或部位'].append(popv)
                placeorbody.append(popv)
            else:
                prt_dict['适用场地或部位'] += set(popv)
                placeorbody += set(popv)
        if '适用部位' in keys:
            popv = prt_dict.pop('适用部位')
            if isinstance(popv, str):
                prt_dict['适用场地或部位'].append(popv)
                placeorbody.append(popv)
            else:
                prt_dict['适用场地或部位'] += set(popv)
                placeorbody += set(popv)
        for i in set(prt_dict.get('适用场地或部位')):
            rel_category_placeorbody.append([each_prt, i])

        product_info.append(prt_dict)
    return set(scene), set(crowd), set(season), set(placeorbody), set(brands), \
           product_info, rel_category_scene, rel_category_crowd, rel_category_time, rel_category_placeorbody, rel_category_brand


res = read_nodes_base_product(tagswithoutner)
with open('./unified_joined_no/0-50000.pick', 'wb') as f:
    pickle.dump(res, f)
bre = 'breaks'
