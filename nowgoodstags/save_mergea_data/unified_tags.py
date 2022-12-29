# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:unified_tags.py
@Time:2022/12/15 17:27
@Read: 统一NER tags and ori tags
"""
# import re
import pickle

# import jieba
# from openbg500L.read500l import *

# todo 因为在构建商品标签的过程中使用这个数据时，针对产品类别的确认不合理，因此这里再读pro-dim的数据，补全这个产品类别信息
# with open('../../bigfiles/all_goods_info.pick', 'rb') as fprodim:
#     prodim = pickle.load(fprodim)

with open('../../bigfiles/savenerdata/middata_from_model_predict/matgoodsinfo_goodsfeatures', 'rb') as fprodim:
    prodim = pickle.load(fprodim)
    # print(prodim['product_id'].dtype)

with open('../../bigfiles/joinAD_product_no.pick', 'rb') as f:  # merge tags data
    nertags_dimprotags_TAGS = pickle.load(f)
    # alltags = set()
    # for i in nertags_dimprotags_TAGS:
    #     for _ in i.keys():
    #         alltags.add(_)


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
            if key == '适用场景' or key == '搭配方案':
                if isinstance(eachprtwithtags.get(key), list):
                    prt_dict[key] = list(set(eachprtwithtags.get(key)))
                else:
                    prt_dict[key] = eachprtwithtags.get(key)

        for key in list(eachprtwithtags):
            prt_dict[key] = eachprtwithtags.get(key)

        keys = eachprtwithtags.keys()

        each_id = eachprtwithtags.get('product_id')
        if each_id is not None:
            each_id = eachprtwithtags.pop('product_id')
        if each_id is None:
            each_id = prodim['product_id']
            each_id = each_id.tolist()[0]
        each_prt = eachprtwithtags.get('product_name')
        if each_prt is None:
            each_prts = prodim.loc[prodim['product_id'] == each_id]
            if each_prts.empty:
                continue
            each_prt = each_prts['product_name'].tolist()[0]
            prt_dict['product_name'] = each_prt
        if not each_prt:
            bp = 'break'
        # 需要归并故pop自己内部需要归并的标签
        other_sp_list = ['触感', '流行元素', '鞋头形状', '穿着方式', '控制方式', '加热方式', '元素标签', '特色卖点',
                         '卖点提取', '表面处理', '图案', '外袋种类', '小包内部结构', '提拎部件', '加工方式', '设计特色']
        for __ in other_sp_list:
            if __ in keys:
                popv = prt_dict.pop(__)
                if isinstance(popv, str):
                    prt_dict['商品卖点'].append(popv)
                else:
                    prt_dict['商品卖点'] += set(popv)
        # 需要归并，故需要pop自己内部的该信息；   property  material
        other_mat_list = ['面料名称', '主面料成分', '里料成分', '肩带材质', '模杯面料', '里料质地', '是否含糖',
                          '鞋面材质', '鞋底材质', '鞋底工艺', '内里材质', '材质', '材质标签', '材质材料', ]
        for __ in other_mat_list:
            if __ in keys:
                popv = prt_dict.pop(__)
                if isinstance(popv, str):
                    prt_dict['商品材质或材料'].append(popv)
                else:
                    prt_dict['商品材质或材料'] += set(popv)
        # 需要归并故pop自己内部需要归并的标签      property functions
        other_func_list = ['功能', '功能功效', '功能标签', '功能类型']
        for __ in other_func_list:
            if __ in keys:
                popv = prt_dict.pop(__)
                if isinstance(popv, str):
                    prt_dict['商品功能'].append(popv)
                else:
                    prt_dict['商品功能'] += set(popv)
        # 需要归并故pop自己内部需要归并的标签      property functions
        other_suit_list = ['搭配品类', '商品搭配']
        for __ in other_suit_list:
            if __ in keys:
                popv = prt_dict.pop(__)
                if isinstance(popv, str):
                    prt_dict['搭配方案'].append(popv)
                else:
                    prt_dict['搭配方案'] += set(popv)

        # 需要归并故pop自己内部需要归并的标签      property functions  rels
        other_crowd_list = ['适用人群', '风格', '人群标签', '使用人群', '商品适用对象', '式样', ]
        for __ in other_crowd_list:
            if __ in keys:
                popv = prt_dict.pop(__)
                if isinstance(popv, str):
                    prt_dict['适配人群或对象'].append(popv)
                    crowd.append(popv)
                else:
                    prt_dict['适配人群或对象'] += set(popv)
                    crowd += set(popv)

        for i in set(prt_dict['适配人群或对象']):
            rel_category_crowd.append([each_prt, i])

        # 需要归并故pop自己内部需要归并的标签      property functions  rels
        other_season_list = ['季节标签', '适合季节', '适用年龄段', '上市年份季节（上市时间）', '上市时间', '上市年份季节']
        for __ in other_season_list:
            if __ in keys:
                popv = prt_dict.pop(__)
                if isinstance(popv, str):
                    prt_dict['应用时间'].append(popv)
                    season.append(popv)
                else:
                    prt_dict['应用时间'] += set(popv)
                    season += set(popv)

        for i in set(prt_dict.get('应用时间')):
            rel_category_time.append([each_prt, i])

        # 需要归并故pop自己内部需要归并的标签      property functions  rels
        other_loc_list = ['适用地点与场景', '适用部位']
        for __ in other_loc_list:
            if __ in keys:
                popv = prt_dict.pop(__)
                if isinstance(popv, str):
                    prt_dict['适用场地或部位'].append(popv)
                    placeorbody.append(popv)
                else:
                    prt_dict['适用场地或部位'] += set(popv)
                    placeorbody += set(popv)

        for i in set(prt_dict.get('适用场地或部位')):
            rel_category_placeorbody.append([each_prt, i])

        # node scene   rels
        if '适用场景' in keys:
            if isinstance(eachprtwithtags.get('适用场景'), str):
                scene.append(eachprtwithtags.pop('适用场景'))
            else:
                scene += eachprtwithtags.pop('适用场景')
            for i in set(prt_dict.get('适用场景')):
                rel_category_scene.append([each_prt, i])

        # node brands   rels
        if '品牌' in keys:
            brand = eachprtwithtags.pop('品牌')
            brands.append(brand)
            rel_category_brand.append([each_prt, brand])

        for k, v in prt_dict.items():
            if isinstance(v, list) and k != '投放商品':
                prt_dict[k] = list(set(v))
        product_info.append(prt_dict)
        print('logging .  .  . count')
    return set(scene), set(crowd), set(season), set(placeorbody), set(brands), \
           product_info, rel_category_scene, rel_category_crowd, rel_category_time, rel_category_placeorbody, rel_category_brand


res = read_nodes_base_product(nertags_dimprotags_TAGS)
with open('../../bigfiles/unified_joined_no_new/all-no.pick', 'wb') as f:
    pickle.dump(res, f)

bre = 'breaks'
