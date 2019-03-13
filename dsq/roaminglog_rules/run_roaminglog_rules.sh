#! /bin/sh
# set variables
export PATH=$PATH:/home/dengshq/roaminglog_rules/
DAY=$1
HOUR=$2

BASE_PATH="hdfs:///user/hive/warehouse/bss.db/roaminglog/"
PART_EXPR="day=$DAY/minute=$HOUR"
OUTPUT_HDFS="hdfs:///user/dengshq/tmp"
OUTPUT_LOCAL="file:///home/dengshq/data/roaminglog_hour.csv"
OUTPUT_FEATURE_DATA="/home/dengshq/data/roaminglog_hour_features.csv"
RESULT_DATA="/home/dengshq/data/rule_model_predict_result.csv"
MODEL_PKL="/home/dengshq/roaminglog_rules/models/wjh_roam_mr.pkl"


echo "$(date "+%Y/%m/%d %H:%M:%S") INFO kinit authetification"
kinit -kt /home/dengshq/dengshq.keytab dengshq
echo "$(date "+%Y/%m/%d %H:%M:%S") INFO authetification accomplished..."

echo "$(date "+%Y/%m/%d %H:%M:%S") INFO fetch data from hadoop, data stored at $OUTPUT_LOCAL"
spark2-submit pyspark_data_extract.py -input_hdfs_base_path $BASE_PATH -input_hdfs_part_expr $PART_EXPR -output_hdfs_path $OUTPUT_HDFS -output_local_path $OUTPUT_LOCAL

echo "$(date "+%Y/%m/%d %H:%M:%S") INFO feature engineering..."
python3 feat_roaming.py -input_data $OUTPUT_LOCAL -output_data $OUTPUT_FEATURE_DATA

echo "$(date "+%Y/%m/%d %H:%M:%S") INFO run rule model and make predictions..."
python3 rule_model_run.py -input_data $OUTPUT_FEATURE_DATA -output_data $RESULT_DATA -model $MODEL_PKL
echo "$(date "+%Y/%m/%d %H:%M:%S") INFO prediction accomplished."

