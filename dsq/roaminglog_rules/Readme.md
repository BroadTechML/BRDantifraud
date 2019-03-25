# 规则模型运行流程

1. 变量配置

临时添加本目录为环境变量:`export PATH=$PATH:/home/dengshq/roaminglog_rules/ `

设置日期和小时:`DAY="20190321"
HOUR="1700"`

hdfs数据仓库位置：`BASE_PATH="hdfs:///user/hive/warehouse/bss.db/roaminglog/"`

路径表达式:`PART_EXPR="day=\$DAY/minute=\$HOUR"`

输出hdfs位置：`OUTPUT_HDFS="hdfs:///user/dengshq/tmp"`
输出csv本地位置：`OUTPUT_LOCAL="file:///home/dengshq/data/roaminglog_hour.csv"`
输出特征文件：`OUTPUT_FEATURE_DATA="/home/dengshq/data/roaminglog_hour_features.csv"`
预测结果输出位置：`RESULT_DATA="/home/dengshq/data/rule_model_predict_result.csv"`

规则和模型序列化文件：`MODEL_PKL="/home/dengshq/roaminglog_rules/models/wjh_roam_mr.pkl"`

2. 数据提取：

认证：`kinit -kt /home/dengshq/dengshq.keytab dengshq`

提取：`spark2-submit pyspark_data_extract.py -input_hdfs_base_path $BASE_PATH -input_hdfs_part_expr $PART_EXPR -output_hdfs_path $OUTPUT_HDFS -output_local_path $OUTPUT_LOCAL`

3. 特征工程：

`python3 feat_roaming.py -input_data $OUTPUT_LOCAL -output_data ​$OUTPUT_FEATURE_DATA`

4. 模型识别：

`python3 rule_model_run.py -input_data $OUTPUT_FEATURE_DATA -output_data $RESULT_DATA -model $MODEL_PKL`

