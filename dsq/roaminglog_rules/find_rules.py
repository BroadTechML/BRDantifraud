import pandas as pd
import numpy as np
from sklearn import tree
import pydot, graphviz
from sklearn.externals.six import StringIO
import warnings

warnings.filterwarnings("ignore")


def label_data(hour_data_list, bad_data=None):
    assert isinstance(hour_data_list, (list, set))

    data = []
    feature_names = [
        'caller_isdn', 'count_20', 'head_20_isdn', 'nunique_20_called_isdn', 'nunique_20_caller_province',
        'nunique_20_caller_city', 'nunique_20_called_province',
        'nunique_20_called_city', 'nunique_20_longcall_code', 'nunique_20_isdn_type', 'nunique_20_systypeflag',
        'nunique_20_speedflag', 'sum_20_roming_cost',
        'max_20_roming_cost', 'min_20_roming_cost', 'avg_20_roming_cost', 'std_20_roming_cost', 'max_20_call_starttime',
        'min_20_call_starttime',
        'count_20_am', 'count_20_pm', 'count_20_ev', 'count_30', 'head_30_isdn', 'nunique_30_called_isdn',
        'nunique_30_caller_province', 'nunique_30_caller_city',
        'nunique_30_called_province', 'nunique_30_called_city', 'nunique_30_longcall_code', 'nunique_30_isdn_type',
        'nunique_30_systypeflag', 'nunique_30_speedflag',
        'sum_30_roming_cost', 'max_30_roming_cost', 'min_30_roming_cost', 'avg_30_roming_cost', 'std_30_roming_cost',
        'max_30_call_starttime', 'min_30_call_starttime',
        'count_30_am', 'count_30_pm', 'count_30_ev', 'is_bad']

    bad = pd.read_csv(bad_data, dtype={"phone_num": str, "shutdown_time": int})
    for hour in hour_data_list:
        df = pd.read_csv(hour, dtype={"caller_isdn": str, "called_isdn": str, "phone": str})
        date = int(hour.split("_")[-2])
        one_bad = bad[bad["shutdown_time"] >= date]
        df_labeled = df.merge(one_bad, left_on="caller_isdn", right_on="phone_num", how="left")

        def is_bad(t):
            if t is np.nan:
                return 0
            else:
                return 1

        df_labeled['is_bad'] = df_labeled.label.apply(is_bad)
        data.append(df_labeled.fillna(0)[feature_names])
    data_labeled = pd.concat(data)

    print("data shape: ", data_labeled.shape)
    print("neg-sample vs. pos-sample:\n", data_labeled['is_bad'].value_counts())
    return data_labeled


def tree_rules(X, y, max_depth=4, max_features=None, random_state=23):
    clf = tree.ExtraTreeClassifier(max_depth=max_depth,
                                   max_features=max_features,
                                   random_state=random_state)
    clf.fit(X, y)
    dot_data = tree.export_graphviz(clf, out_file=None, feature_names=X.columns.tolist(),
                                    class_names=['norm', 'abnorm'],
                                    filled=True, rounded=True, special_characters=False, rotate=True)
    try:
        graph = graphviz.Source(dot_data)
        return clf, graph

    except:
        print("graph plotting failed")
        return clf




def tree_to_code(clf, feature_names):
    tree_ = clf.tree_
    feature_name = [
        feature_names[i] if i != tree._tree.TREE_UNDEFINED else "undefined!"
        for i in tree_.feature
    ]
    print('Decision Rules is :')
    s = StringIO()
    def recurse(node, depth):
        rules = ""
        indent = "  " * depth
        if tree_.feature[node] != tree._tree.TREE_UNDEFINED:
            name = feature_name[node]
            threshold = tree_.threshold[node]
            s.write("%s if %s <= %.3f:\n" % (indent, name, threshold))
            recurse(tree_.children_left[node], depth + 1)
            s.write("%s else:  #  if %s > %.3f\n" % (indent, name, threshold))
            recurse(tree_.children_right[node], depth + 1)
        else:
            s.write("%s return %s\n" % (indent, tree_.value[node][0]))
        return rules
    recurse(0, 1)
    rules = s.getvalue()
    print(rules)
    return rules

def validate_rules(data, rule):
    return data.query(rule)['is_bad'].value_counts().todict()


if __name__ == "__main__":
    hours = [
        'L:/roaming_hour_data/df_feat_20190107_0.csv',
        'L:/roaming_hour_data/df_feat_20190107_1.csv',
        'L:/roaming_hour_data/df_feat_20190107_2.csv',
    ]
    bad = 'L:/2019_real_evil_phones.csv'
    data = label_data(hours, bad)
    X = data.iloc[:, 1:-1]
    y = data.iloc[:, -1]
    clf, g = tree_rules(X, y)
    rule = tree_to_code(clf, X.columns.tolist())


