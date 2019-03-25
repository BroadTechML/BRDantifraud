import pandas as pd
from sqlalchemy import create_engine

def connect_database():
	result_engine = create_engine("mysql+pymysql://root:Zaq12wsx()@132.225.129.125:3306/rule_model_predict_result")
	evil_engine = create_engine("mysql+pymysql://root:Zaq12wsx()@132.225.129.125:3306/mydata")
	result_conn = result_engine.connect()
	evil_conn = evil_engine.connect()
	return result_conn, evil_conn

def fetch_data(conn, query_expr=None):
	# with conn as conn, conn.begin():
	data = pd.read_sql(query_expr, conn)
	return data

def label_result(result_data, bad_data, left='id', right='phone', how='left'):
	result_labeled = result_data.merge(bad_data, left_on=left, right_on=right, how=how).fillna(0)
	# print("-------------------识别结果---------------------")
	# print(result_labeled.label.value_counts())
	return result_labeled

def validate_result(result_labeled):
	label_counts = result_labeled.label.value_counts().to_dict()
	precision = result_labeled[result_labeled['label'] !=0].shape[0] / result_labeled.shape[0]
	# for k,v in label_counts.items():
	# 	print(k, ":", v)
	print("准确率:", precision)


if __name__ == "__main__":
	rc, ec = connect_database()
	data = []
	for i in range(20190220, 20190229):
		bad_0301 = fetch_data(ec, "select * from evil_num where shutdown_time >= %d" % i) 
		result_0301 = fetch_data(rc, "select * from results_%d" % i)
		result_labeled = label_result(result_0301, bad_0301)
		data.append(result_labeled)
		# print("%d" % i)
		# validate_result(result_labeled)
	data = pd.concat(data)
	data.to_csv(r"C:\Users\Administrator\Desktop\Notebooks\all_results_220-229.csv", index=False)
	
