import pandas as pd
import numpy as np
import os, random
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import argparse, warnings
warnings.filterwarnings('ignore')

parser = argparse.ArgumentParser()
parser.add_argument("-model_dir", required=True, type=str)
parser.add_argument("-input_data", required=True, type=str)
parser.add_argument("-output_data", required=True, type=str)
args = parser.parse_args()

def predict(model_dir, input_data, output_data):
    df = pd.read_csv(input_data, low_memory=False)
    preds = []
    c = 0
    for model_path in [os.path.join(model_dir, i) for i in os.listdir(model_dir) \
                    if i.endswith(".model") and i.startswith("xgb")]:
        clf = xgb.XGBClassifier()
        clf.load_model(model_path)
        clf._le = LabelEncoder().fit(np.array([0, 1]))
        y_pred = clf.predict_proba(df.iloc[:, 1:], ntree_limit=clf.n_estimators)
        preds.append(y_pred[:, 1])
        print(c + 1, model_path, "  predicted.")
        c += 1
    preds = np.array(preds).mean(axis=0)
    output_df = pd.DataFrame({"caller_isdn": df.iloc[:, 0], "pred": preds})
    output_df = output_df[output_df.pred > 0.5]
    output_df.to_csv(output_data, index=False)

if __name__ == "__main__":
    model_dir = args.model_dir
    input_data = args.input_data
    output_data = args.output_data
    predict(model_dir, input_data, output_data)