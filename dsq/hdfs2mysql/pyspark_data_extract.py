
import argparse
from pyspark.sql import SparkSession
import os
parser = argparse.ArgumentParser()

parser.add_argument("-input_hdfs_base_path", required=True, type=str)
parser.add_argument("-input_hdfs_part_expr", required=True, type=str)
parser.add_argument("-output_hdfs_path", required=True, type=str)
parser.add_argument("-output_local_path", required=True, type=str)
args = parser.parse_args()
def main(input_dir, par_expr, output_dir, output_local_path):
    indf = spark.read.option("basePath", input_dir).parquet(input_dir + par_expr)
    # indf = spark.sql("select * from %s where day = %d" % (tablename, day))
    indf.write.format("csv").save(output_dir)
    os.system("hadoop fs -getmerge {0} {1}".format(
        output_dir,
        output_local_path
    ))
    os.system("hadoop fs -rm -R {0}".format(
        output_dir
    ))


if __name__ == "__main__":
    ihp = args.input_hdfs_base_path
    ohp = args.output_hdfs_path
    ihpe = args.input_hdfs_part_expr
    olp = args.output_local_path
    spark = SparkSession.builder.appName("xxx").getOrCreate()
    main(ihp, ihpe, ohp, olp)
    print("output accomplished.")




