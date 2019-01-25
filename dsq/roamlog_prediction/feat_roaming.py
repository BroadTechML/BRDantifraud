import warnings
import os
from blaze import *
from pylab import *
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.externals import joblib
import argparse

warnings.filterwarnings('ignore')
np.random.seed(4590)

parser = argparse.ArgumentParser()
parser.add_argument("-day", required=True, type=int)
parser.add_argument("-input_data", required=True, type=str)
parser.add_argument("-output_data", required=True, type=str)
args = parser.parse_args()

def starttime_split(starttime):
    if starttime > 60000 and starttime < 120000:
        return 1
    elif starttime > 120000 and starttime < 180000:
        return 2
    else:
        return 3


def countVec_fit(df, pth_model='./models'):
    # caller province
    caller_province_group_caller = df.groupby(['caller_isdn'])['caller_province'].agg(
        lambda x: " ".join(list(map(str, list(x)))))
    caller_province_text = list(caller_province_group_caller)
    caller_vectorizer = CountVectorizer()
    model_countVec_caller = caller_vectorizer.fit(caller_province_text)

    # called province
    called_province_group_caller = df.groupby(['caller_isdn'])['called_province'].agg(
        lambda x: " ".join(list(map(str, list(x)))))
    called_province_text = list(called_province_group_caller)
    called_vectorizer = CountVectorizer()
    model_countVec_called = caller_vectorizer.fit(called_province_text)

    # save model and return result
    joblib.dump(model_countVec_caller, pth_model + '/model_countVec_caller.model')
    joblib.dump(model_countVec_called, pth_model + '/model_countVec_called.model')


def countVec_load(df, call_type='caller', pth_model='./models'):
    # caller province
    caller_province_group_caller = df.groupby([call_type + '_isdn'])['caller_province'].agg(
        lambda x: " ".join(list(map(str, list(x)))))
    caller_province_text = list(caller_province_group_caller)
    caller_vectorizer = CountVectorizer()
    model_countVec_caller_1 = joblib.load(pth_model + '/model_countVec_caller.model')
    caller_X = model_countVec_caller_1.transform(caller_province_text)
    caller_provinces = model_countVec_caller_1.get_feature_names()
    caller_province_counts = pd.DataFrame(caller_X.toarray(), columns=caller_provinces)
    caller_province_counts.columns = ['caller_province_' + call_type + '_' + col for col in
                                      caller_province_counts.columns]
    caller_province_counts[call_type + '_isdn'] = caller_province_group_caller.index
    # called province
    called_province_group_caller = df.groupby([call_type + '_isdn'])['called_province'].agg(
        lambda x: " ".join(list(map(str, list(x)))))
    called_province_text = list(called_province_group_caller)
    called_vectorizer = CountVectorizer()
    model_countVec_caller_2 = joblib.load(pth_model + '/model_countVec_called.model')
    called_X = model_countVec_caller_2.transform(called_province_text)
    called_provinces = model_countVec_caller_2.get_feature_names()
    called_province_counts = pd.DataFrame(called_X.toarray(), columns=called_provinces)
    called_province_counts.columns = ['called_province_' + call_type + '_' + col for col in
                                      called_province_counts.columns]
    called_province_counts[call_type + '_isdn'] = called_province_group_caller.index
    return caller_province_counts, called_province_counts


def feat_roam_day(df):
    df['caller_isdn'] = df['caller_isdn'].astype(str)
    df['called_isdn'] = df['called_isdn'].astype(str)
    df_20 = df[df['record_type'] == 20]
    df_30 = df[df['record_type'] == 30]

    # 主叫记录数
    df_feat_20 = df_20.groupby(['caller_isdn'])['imei'].count().reset_index().rename(columns={'imei': 'count_20'})
    # 主叫号码前缀
    df_feat_20['head_20_isdn'] = df_feat_20['caller_isdn'].apply(lambda x: str(x)[:3]).astype(int)
    # nunique
    lst_cols_nunique = ['called_isdn', 'caller_province', 'caller_city', 'called_province', 'called_city',
                        'longcall_code',
                        'longcall_code',
                        'isdn_type', 'systypeflag', 'speedflag']
    for col in lst_cols_nunique:
        df_feat_20['nunique_20_' + col] = df_20.groupby(['caller_isdn'])[col].nunique().values
    df_feat_20['sum_20_roming_cost'] = df_20.groupby(['caller_isdn'])['roming_cost'].sum().values
    df_feat_20['max_20_roming_cost'] = df_20.groupby(['caller_isdn'])['roming_cost'].max().values
    df_feat_20['min_20_roming_cost'] = df_20.groupby(['caller_isdn'])['roming_cost'].min().values
    df_feat_20['avg_20_roming_cost'] = df_20.groupby(['caller_isdn'])['roming_cost'].mean().values
    df_feat_20['std_20_roming_cost'] = df_20.groupby(['caller_isdn'])['roming_cost'].std().values
    df_feat_20['max_20_call_starttime'] = df_20.groupby(['caller_isdn'])['call_starttime'].max().values
    df_feat_20['min_20_call_starttime'] = df_20.groupby(['caller_isdn'])['call_starttime'].min().values
    df_20['call_starttime_split'] = df_20['call_starttime'].apply(starttime_split)
    gp_time_split = df_20.groupby(['caller_isdn', 'call_starttime_split'])['imei'].nunique().reset_index()
    gp_time_split_am = gp_time_split[gp_time_split['call_starttime_split'] == 1]
    gp_time_split_pm = gp_time_split[gp_time_split['call_starttime_split'] == 2]
    gp_time_split_ev = gp_time_split[gp_time_split['call_starttime_split'] == 3]
    df_feat_20 = df_feat_20.merge(gp_time_split_am[['caller_isdn', 'imei']], on='caller_isdn', how='left').rename(
        columns={'imei': 'count_20_am'})
    df_feat_20 = df_feat_20.merge(gp_time_split_pm[['caller_isdn', 'imei']], on='caller_isdn', how='left').rename(
        columns={'imei': 'count_20_pm'})
    df_feat_20 = df_feat_20.merge(gp_time_split_ev[['caller_isdn', 'imei']], on='caller_isdn', how='left').rename(
        columns={'imei': 'count_20_ev'})
    print('caller countVec...')
    caller_province_counts, called_province_counts = countVec_load(df_20, 'caller', './models')
    df_feat_20 = df_feat_20.merge(caller_province_counts, on='caller_isdn', how='left')
    df_feat_20 = df_feat_20.merge(called_province_counts, on='caller_isdn', how='left')

    # 被叫记录数
    df_feat_30 = df_30.groupby(['called_isdn'])['imei'].count().reset_index().rename(columns={'imei': 'count_30'})
    # 被叫号码前缀
    df_feat_30['head_30_isdn'] = df_feat_30['called_isdn'].apply(lambda x: str(x)[:3]).astype(int)
    # nunique
    lst_cols_nunique = ['called_isdn', 'caller_province', 'caller_city', 'called_province', 'called_city',
                        'longcall_code',
                        'longcall_code',
                        'isdn_type', 'systypeflag', 'speedflag']
    for col in lst_cols_nunique:
        df_feat_30['nunique_30_' + col] = df_30.groupby(['called_isdn'])[col].nunique().values

    df_feat_30['sum_30_roming_cost'] = df_30.groupby(['called_isdn'])['roming_cost'].sum().values
    df_feat_30['max_30_roming_cost'] = df_30.groupby(['called_isdn'])['roming_cost'].max().values
    df_feat_30['min_30_roming_cost'] = df_30.groupby(['called_isdn'])['roming_cost'].min().values
    df_feat_30['avg_30_roming_cost'] = df_30.groupby(['called_isdn'])['roming_cost'].mean().values
    df_feat_30['std_30_roming_cost'] = df_30.groupby(['called_isdn'])['roming_cost'].std().values
    df_feat_30['max_30_call_starttime'] = df_30.groupby(['called_isdn'])['call_starttime'].max().values
    df_feat_30['min_30_call_starttime'] = df_30.groupby(['called_isdn'])['call_starttime'].min().values
    df_30['call_starttime_split'] = df_30['call_starttime'].apply(starttime_split)
    gp_time_split = df_30.groupby(['called_isdn', 'call_starttime_split'])['imei'].nunique().reset_index()
    gp_time_split_am = gp_time_split[gp_time_split['call_starttime_split'] == 1]
    gp_time_split_pm = gp_time_split[gp_time_split['call_starttime_split'] == 2]
    gp_time_split_ev = gp_time_split[gp_time_split['call_starttime_split'] == 3]
    df_feat_30 = df_feat_30.merge(gp_time_split_am[['called_isdn', 'imei']], on='called_isdn', how='left').rename(
        columns={'imei': 'count_30_am'})
    df_feat_30 = df_feat_30.merge(gp_time_split_pm[['called_isdn', 'imei']], on='called_isdn', how='left').rename(
        columns={'imei': 'count_30_pm'})
    df_feat_30 = df_feat_30.merge(gp_time_split_ev[['called_isdn', 'imei']], on='called_isdn', how='left').rename(
        columns={'imei': 'count_30_ev'})
    print('called countVec...')
    caller_province_counts, called_province_counts = countVec_load(df_30, 'called', './models')
    df_feat_30 = df_feat_30.merge(caller_province_counts, on='called_isdn', how='left')
    df_feat_30 = df_feat_30.merge(called_province_counts, on='called_isdn', how='left')

    # merge
    df_feat_20['phone'] = df_feat_20['caller_isdn']
    df_feat_30['phone'] = df_feat_30['called_isdn']
    df_feat_merge = df_feat_20.merge(df_feat_30, on='phone', how='outer')

    return df_feat_merge


lst_columns = ['record_type', 'roaming_cityid', 'caller_cityid', 'called_cityid', 'intelligentflag', 'link_refrence',
               'longcallflag', 'imsi', 'caller_isdn', 'changflag', 'isdn_type', 'pick_solution', 'called_isdn',
               'srvtype', 'srvcode', 'double_srvtype', 'double_srvcode', 'channel_request', 'channel_use', 'srv_flag',
               'activity_code1', 'extendsrv_code1', 'activity_code2', 'extendsrv_code2', 'activity_code3',
               'extendsrv_code3', 'activity_code4', 'extendsrv_code4', 'activity_code5', 'extendsrv_code5', 'msc',
               'lac', 'cellularflag', 'mobiletypeflag', 'call_date', 'call_starttime', 'call_datetime', 'chargeunit',
               'call_data', 'longcall_code', 'other_code', 'roming_cost', 'longcall_cost', 'other_cost', 'bill_item',
               'systypeflag', 'speedflag', 'billingflag', 'imei', 'reserve', 'roaming_city', 'caller_city',
               'called_city', 'caller_province', 'called_province', 'cell', 'day', 'minute'
               ]

if __name__ == '__main__':
    input_data = args.input_data
    output_data = args.output_data
    day = args.day
    # 做特征
    df_day = pd.read_csv(input_data, names=lst_columns).replace("\\N", np.nan)
    
    df_feat_day = feat_roam_day(df_day)
    # bad_phone_day = bad_phone[bad_phone['shutdown_time'] > day]
    # df_feat_day = df_feat_day.merge(bad_phone_day[['phone', 'label_num']], on='phone', how='left').fillna(0)
    df_feat_day.to_csv(output_data, index=False)