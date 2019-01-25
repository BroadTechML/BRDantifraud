'''
@Description: In User Settings Edit
@Author: your name
@Date: 2018-12-17 09:24:59
@LastEditTime: 2019-01-02 15:38:50
@LastEditors: Please set LastEditors
'''

# coding: utf-8

import os
import pandas as pd


key_type = {'day':int,'phone':str,'imsi':str,'rat':int,'area_name':int,'in_net_group':int,'user_dinner':str,'user_age':int,'age_group':int,
            'user_grid':str,'user_grid_name':str,'gprs_bytes_type':int,'acct_charge_type':int,'gprs_bytes':float,'acct_charge':float,
            'gprs_2g':float,'gprs_3g':float,'gprs_4g':float,'talklen_2g':float,'talklen_3g':float,'flow_2g':float,'flow_3g':float,
            'flow_4g':float,'type_vip':int,'bj_duration':float,'user_sex':str,'user_birthday':int,'if_4gllb':int,'if_usim':int,'if_wnet':int,
            'if_virtual':int,'v_operator':str,'if_wireless':int,'fix_phone':str,'if_locknet':int,'if_kcuser':int,'kc_id':str,'kcard_id':str,
            'phonesegment7':int,'phonesegment6':int,'tacid':str,'user_status':str,'develop_channel_type':str,'develop_channel_name':str,
            'account':str,'is2g':int,'is3g':int,'is4g':int,'sim':int,'roamtype':int,'used_2g':int,'used_3g':int,'used_4g':int,
            'cell_times':float,'cell_most':str,'hcountry':int,'hprovince':int,'hcity':int,'vcountry':int,'vprovince':int,'vcity':int,
            'online_2g':float,'online_3g':float,'online_4g':float,'off_delay':float,'local':int,'innerprov':int,'roamin':int,
            'international':int,'roamout':int,'abroad':int,'cell_times_2':float,'cell_most_2':float,'cell_times_3':float,'cell_most_3':"int64", # thie feature 
            'cell_talklen_long1':float,'cell_talklen_1':float,'cell_talklen_long2':float,'cell_talklen_2':float,'cell_talklen_long3':float,
            'cell_talklen_3':float,'cell_flow_value1':float,'cell_flow_1':float,'cell_flow_value2':float,'cell_flow_2':float,
            'cell_flow_value3':float,'cell_flow_3':float,'model_id':str,'svc_id':int,'group_id':int,'nps_score':float,'open_date':str,
            'innet_months':float,'mt_type':int,'model_groupid_two':str,'mzie_type':int,'nds_score':float,'user_lv':str}


lst_columns = [
    'day',  # 日期
    'phone',  # 号码
    'imsi',  # IMSI
    'rat',  # 接入网类型
    'area_name',  # 开户地市
    'in_net_group',  # 网龄段
    'user_dinner',  # 用户套餐
    'user_age',  # 用户年龄
    'age_group',  # 用户年龄段
    'user_grid',  # 用户网格id
    'user_grid_name',  # 用户网格名称
    'gprs_bytes_type',  # 流量区间
    'acct_charge_type',  # ARPU区间
    'gprs_bytes',  # 总流量 / (MB)
    'acct_charge',  # 出账费用(元)
    'gprs_2g',  # 2g网络流量
    'gprs_3g',  # 3g网络流量
    'gprs_4g',  # 4g网络流量
    'talklen_2g',  # 2g通话时长
    'talklen_3g',  # 3g通话时长
    'flow_2g',  # 2g流量kb
    'flow_3g',  # 3g流量kb
    'flow_4g',  # 4g流量kb
    'type_vip',  # 用户级别
    'bj_duration',  # 主被叫总时长(小时)
    'user_sex',  # 用户性别
    'user_birthday',  # 用户生日
    'if_4gllb',  # 是否开通4g
    'if_usim',  # 是否usim卡
    'if_wnet',  # 是否为物联网用户
    'if_virtual',  # 是否为虚拟运营商
    'v_operator',  # 虚拟运营商
    'if_wireless',  # 是否为固话用户
    'fix_phone',  # 固话号码
    'if_locknet',  # 是否为锁网用户
    'if_kcuser',  # 是否为网卡用户
    'kc_id',  # 王卡大类
    'kcard_id',  # 王卡小类
    'phonesegment7',  # 7位号段
    'phonesegment6',  # 6位号段
    'tacid',  # TAC
    'user_status',  # 用户状态
    'develop_channel_type',  # 渠道类别名称
    'develop_channel_name',  # 发展渠道名称
    'account',  # 月账期
    'is2g',  # 是否签约2g
    'is3g',  # 是否签约3g
    'is4g',  # 是否签约4g
    'sim',  # sim
    'roamtype',  # 漫游类型
    'used_2g',  # 是否使用2g
    'used_3g',  # 是否使用3g
    'used_4g',  # 是否使用4g
    'cell_times',  # 驻留次数
    'cell_most',  # 拜访小区
    'hcountry',  # 归属国家
    'hprovince',  # 归属省
    'hcity',  # 归属市
    'vcountry',  # 拜访国家
    'vprovince',  # 拜访省
    'vcity',  # 拜访市
    'online_2g',  # 2g驻网时长
    'online_3g',  # 3g驻网时长
    'online_4g',  # 4g驻网时长
    'off_delay',  # 离线时长
    'local',  # 是否本地访问
    'innerprov',  # 是否省内漫游
    'roamin',  # 是否国内漫入
    'international',  # 是否国际漫入
    'roamout',  # 是否国内漫出
    'abroad',  # 是否国际漫出
    'cell_times_2',  # 1天驻留次数top2
    'cell_most_2',  # 一天驻留次数top2小区
    'cell_times_3',  # 1天驻留次数top3
    'cell_most_3',  # 一天驻留次数top3小区
    'cell_talklen_long1',  # 一天通话时长top1
    'cell_talklen_1',  # 一天通话时长top1小区
    'cell_talklen_long2',  # 一天通话时长top2
    'cell_talklen_2',  # 一天通话时长top2小区
    'cell_talklen_long3',  # 一天通话时长top3
    'cell_talklen_3',  # 一天通话时长top3小区
    'cell_flow_value1',  # 一天流量使用top1
    'cell_flow_1',  # 一天流量使用top1小区
    'cell_flow_value2',  # 一天流量使用top2
    'cell_flow_2',  # 一天流量使用top2小区
    'cell_flow_value3',  # 一天流量使用top3
    'cell_flow_3',  # 一天流量使用top3小区
    'model_id',  # 终端型号
    'svc_id',  # SVC_ID
    'group_id',  # 客户群分类
    'nps_score',  # NPS评分
    'open_date',  # 入网时间
    'innet_months',  # 用户网龄
    'mt_type',  # 联通终端类型
    'model_groupid_two',  # 终端系列
    'mzie_type',  # 定制类型
    'nds_score',  # 贬损值
    'user_lv'  # 用户星级
]

def feature_extract(df):
    df.replace('\\N', -1, inplace=True)
    df.fillna(0, inplace=True)
    df = df.astype(key_type, copy=True, errors='ignore')
    df = df.drop(['user_dinner','area_name','user_grid','user_grid_name','model_id','day','tacid',
             'develop_channel_name','account','kc_id','kcard_id','model_groupid_two','cell_most'],axis=1)
    df['user_sex'] = pd.Categorical(df['user_sex']).codes
    df['user_lv'] = pd.Categorical(df['user_lv']).codes
    df['v_operator'] = pd.Categorical(df['v_operator']).codes
    df['user_status'] = pd.Categorical(df['user_status']).codes
    df['develop_channel_type'] = pd.Categorical(df['develop_channel_type']).codes
    df['imsi'] = df['imsi'].apply(lambda x: x[:5])
    df['fix_phone'] = df['fix_phone'].apply(lambda x:x[:5])
    df['open_date'] = df['open_date'].apply(lambda x:x[:4])
    df['imsi'] = df['imsi'].astype('int')
    df['fix_phone'] = df['fix_phone'].astype('int')
    df['open_date'] = df['open_date'].astype('int')
    return df


 


def load_user_data(path='../twow/2018090/'):
    data = pd.DataFrame()
    file_names = os.listdir(path)
    file_names = [fname for fname in file_names if len(fname)==8]
    for fname in file_names:
        temp = pd.read_csv(path+fname, names=lst_columns)
        data = data.append(temp)
    return data
