'''
@Author: Wang jinghui
@Date: 2018-12-21 13:20:24
@LastEditTime: 2019-01-15 10:15:00
'''


# coding: utf-8

import os
import gc
import sys
import warnings
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.externals import joblib
#from tqdm import tqdm_notebook,tqdm

map_log_columns = ['servicetype', 'subsvctype', 'rlai', 'rrantype', 'newtmsi',
       'vcountrygt', 'voperator', 'negoflag', 'maincode', 'delay_oper',
       'rmsc', 'rmsc_gt', 'hlr', 'hlr_gt', 'smc', 'smc_gt', 'scp',
       'scp_gt', 'rantype', 'msc', 'msc_gt', 'mscip', 'mgwip', 'bsc',
       'lai', 'cgi', 'tmsi', 'imei', 'imeirantype', 'vprovince', 'vcity',
       'vcountry', 'starttime', 'endtime', 'eventtype', 'subevttype',
       'combflag', 'userard', 'usertype', 'phoneprefix', 'phone', 'imsi',
       'hcountry', 'hprovince', 'hcity', 'roamtype', 'hoperator',
       'result', 'errorcode', 'releasercode', 'failuremsg',
       'failurepremsg', 'tdrid', 'callid', 'test1', 'test2', 'day',
       'minute']

col_names = ['phone','phoneprefix','vcountrygt','vcity','vprovince','hcountry','hprovince','hcity']
 
def load_data(path='../twow/2018090/'):
    data = pd.DataFrame()
    file_names = os.listdir(path)
    file_names = [fname for fname in file_names if len(fname)==8]
    for i, fname in enumerate(file_names):
        temp = pd.read_csv(path+fname, names=map_log_columns)
        data = data.append(temp)
        if (i+1)%20 == 0:
            print('load file %d percent'%(int((i+1)*100/len(file_names))))
    return data

def load_data2(path='../twow/2018090/'):
    data = pd.DataFrame()
    file_names = os.listdir(path)
    file_names = [fname for fname in file_names if len(fname)==8]
    for i, fname in enumerate(file_names):
        temp = pd.read_csv(path+fname, names=col_names)
        data = data.append(temp)
        sys.stdout.write('done %d/%d \r' %(i+1, len(file_names)))
    return data









def data_preprocess(data):
    df = data[['vcountrygt',  'vprovince', 'vcity', 'phoneprefix', 'phone',  'hcountry', 'hprovince', 'hcity']]
    type_dict = {'vcountrygt':int,  'vprovince':int, 'vcity':int,  'phoneprefix':str, 'phone':str,  'hcountry':int, 'hprovince':int, 'hcity':int}
    df.replace('\\N', -1,  inplace=True)
    df.fillna(-1, inplace=True)
    df = df.astype(type_dict,  copy=True, errors='ignore')
    df.phoneprefix = df.phoneprefix.apply(lambda x : x[:4])
    df.starttime = df.starttime.apply(lambda x: x.split(' ')[1].split(':')[0])
    df = df.astype(str)
    return df


def word2vec(df, col_name, model):
    temp_gp = df.groupby('phone')
    temp_df = temp_gp[col_name].agg(lambda x: list(x)).reset_index()
    phones = temp_df.phone.values
    words = temp_df[col_name].apply(lambda x: ' '.join(x)).tolist()
    vector = model.transform(words).toarray()
    columns = [col_name+'_'+str(i) for i in range(vector.shape[1])]
    df = pd.DataFrame(vector, columns=columns)
    df['phone'] = phones
    gc.enable()
    del temp_df, words, vector
    gc.collect()
    return df

def make_features(data_path):
    data = load_data(data_path)
    data = data_preprocess(data)
    vcountry_m = joblib.load('../models/vcountrygt.m')
    vprovince_m = joblib.load('../models/vprovince.m')
    vcity_m = joblib.load('../models/vcity.m')
    starttime_m = joblib.load('../models/starttime.m')
    phoneprefix_m = joblib.load('../models/phoneprefix.m')
    hcountry_m = joblib.load('../models/hcountry.m')
    hprovince_m = joblib.load('../models/hprovince.m')
    hcity_m = joblib.load('../models/hcity.m')

    vcountry_df = word2vec(data, 'vcountrygt', vcountry_m)
    vprovince_df = word2vec(data, 'vprovince', vprovince_m)
    vcity_df = word2vec(data, 'vcity', vcity_m)
    starttime_df = word2vec(data, 'starttime', starttime_m)
    phoneprefix_df = word2vec(data, 'phoneprefix', phoneprefix_m)
    hcountry_df = word2vec(data, 'hcountry', hcountry_m)
    hprovince_df = word2vec(data, 'hprovince', hprovince_m)
    hcity_df = word2vec(data, 'hcity', hcity_m)

    X_df = vcountry_df.merge(vprovince_df, on='phone', how='left')
    X_df = X_df.merge(vcity_df, on='phone', how='left')
    X_df = X_df.merge(starttime_df, on='phone', how='left')
    X_df = X_df.merge(phoneprefix_df, on='phone', how='left')
    X_df = X_df.merge(hcountry_df, on='phone', how='left')
    X_df = X_df.merge(hprovince_df, on='phone', how='left')
    X_df = X_df.merge(hcity_df, on='phone', how='left')
    return X_df