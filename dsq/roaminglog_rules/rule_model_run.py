import pandas as pd
import argparse
import pickle
import warnings
warnings.filterwarnings('ignore')


parser = argparse.ArgumentParser()
parser.add_argument("-input_data", required=True, type=str)
parser.add_argument("-output_data", required=True, type=str)
parser.add_argument("-model", required=True, type=str)
args = parser.parse_args()

def fetch_model(model_path):
	with open(model_path, "rb") as f:
		rule_model = pickle.loads(f.read())
		return rule_model

def predict(input_data, output_data, model):
	if 'rules' in model.keys():
		data = pd.read_csv(input_data).query(model['rules']).fillna(0)
		ID = data['phone'].copy(deep=True)
		del data['phone']
		clf = model['model']
		pred = clf.predict(data)
		pred_proba = clf.predict_proba(data)
		output = pd.DataFrame({"ID": ID, "pred": pred, "pred_proba": pred_proba[:, 1]})
		output.query("pred == 1")[['ID', "pred_proba"]].to_csv(output_data, index=False)
		return output

if __name__ == "__main__":
	model = fetch_model(args.model)
	input_data = args.input_data
	output_data = args.output_data
	predict(input_data, output_data, model)


