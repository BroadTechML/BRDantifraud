# 设定常量
MERGE_DAY=/data1/bss_roaminglog/merge_day
MERGE_DAY_FEAT=/data1/bss_roaminglog/merge_day_feat
THIS_DIR=/home/dengshq/roamlog_prediction
PREDICT_OUTPUT=/data1/bss_roaminglog_predicts

# 求得昨天的日期
yesterday=`date  +"%Y%m%d" -d  "-1 days" | tr -d "\n"`

# 使用spark提取昨天的roamlog数据，并保存为csv格式文件到本地硬盘
kinit -kt /home/dengshq/dengshq.keytab dengshq
spark2-submit $THIS_DIR/pyspark_data_extract.py \
    -input_hdfs_base_path hdfs:///user/hive/warehouse/bss.db/roaminglog/ \
    -input_hdfs_part_expr day=$yesterday/minute=*  \
    -output_hdfs_path hdfs:///user/dengshq/roamlog_tmp  \
    -output_local_path file://$MERGE_DAY/$yesterday.csv
echo "$yesterday roaminglog data extracted."

# 特征工程，输出到特征数据目录
python $THIS_DIR/feat_roaming.py \
    -day $yesterday \
    -input_data $MERGE_DAY/$yesterday.csv \
    -output_data $MERGE_DAY_FEAT/df_feat_$yesterday.csv \

# 进行预测，并输出到预测目录
python $THIS_DIR/roamlog_xgb_predict.py \
    -model_dir=$THIS_DIR/xgb_roamlog_models  \
    -input_data=$MERGE_DAY_FEAT/df_feat_$yesterday.csv \
    -output_data=$PREDICT_OUTPUT/roaminglog_pred_bad_like_$yesterday.csv
