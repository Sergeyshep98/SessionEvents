import argparse
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    when, lag, unix_timestamp,
                                   first, concat_ws, sum as spark_sum,
                                   col, date_sub, lit, to_date)


def parse_args():
    parser = argparse.ArgumentParser(description="Process session data.")
    parser.add_argument('--new_table', action='store_true', help="Flag to indicate that the table is new.")
    parser.add_argument(
        '--process_date',
        type=str,
        required=True,
        help='Process date in the format YYYY-MM-DD'
    )
    return parser.parse_args()

def create_spark_session():
    return (SparkSession.builder \
        .appName("SessEvents") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.warehouse.dir", "s3a://spark-data/spark-warehouse")
        .config("spark.hadoop.hive.metastore.warehouse.dir", "s3a://spark-data/spark-warehouse")
        .config("spark.jars", "jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.262.jar,jars/delta-core_2.12-2.2.0.jar")\
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.metrics.enabled", "false") \
        .enableHiveSupport() \
        .getOrCreate())


class SessionBuilder2:
    def __init__(self, batch, keys=None, session_timeout=300):
        self.batch = batch
        self.keys = keys if keys else []
        self.session_timeout = session_timeout
        self.window_clause = Window.partitionBy(self.keys).orderBy("timestamp")

    def process(self):
        self.batch = self.batch.dropDuplicates(['user_id', 'event_id', 'product_code', 'timestamp'])
        df = self.batch.withColumn("is_user_action",
                                   when(self.batch.event_id.isin('a', 'b', 'c'), True).otherwise(False))
        df = df.withColumn("prev_timestamp", lag("timestamp").over(self.window_clause))
        df = df.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
        df = df.withColumn(
            "is_new_session",
            when(df.is_user_action & ((df.time_diff.isNull()) | (df.time_diff >= self.session_timeout)), 1).otherwise(0)
        )
        df = df.withColumn("session_group", spark_sum("is_new_session").over(
            self.window_clause.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        df = df.withColumn("session_start", first(when(df.is_new_session == 1, col("timestamp"))).over(
            self.window_clause.partitionBy(self.keys + ["session_group"])))
        df = df.withColumn(
            "session_id",
            when(col("session_start").isNotNull(),
                 concat_ws("#", col("user_id"), col("product_code"), col("session_start")))
        )
        df = df.withColumn("pdate", to_date('timestamp'))
        return df.select("user_id", "event_id", "product_code", "timestamp", "session_start", "session_id", "pdate")


def table_exists(spark, table_name):
    return spark.catalog.tableExists(table_name)


def main(process_date, new):
    spark = create_spark_session()
    table_path = "s3a://spark-data/spark-warehouse/ods.db/sessions_final"
    table_name = "ods.sessions_final"

    spark.sql("CREATE DATABASE IF NOT EXISTS ods")

    df = None

    # To update sessions for existing events, we retrieve users and product codes from the batch
    # and match them with data from the past five days.
    # Additionally, we include user events from six days ago to ensure sessions are correctly assigned
    # for the following day.
    if not new:
        df = spark.read.format("delta").load(table_path)
        df = df.where(
            (col('timestamp') >= date_sub(to_date(lit(process_date)), 5)) |
            ((col('timestamp') == date_sub(to_date(lit(process_date)), 6)) & col('event_id').isin('a', 'b', 'c'))
        )

    df_batch = spark.read.csv(f"s3a://spark-data/raw/{process_date}.csv", header=True, inferSchema=True)
    df_exist = df_batch.select('user_id', 'product_code').distinct()
    df_source = df.alias('df').join(
        df_exist, ['user_id', 'product_code'], 'inner').select('df.user_id', 'df.event_id',
                                                                                           'df.timestamp',
                                                                                           'df.product_code'
                                                               ) if df else df_batch
    sess = SessionBuilder2(batch=df_source.unionAll(df_batch), keys=["user_id", "product_code"]).process()


    #Use merge for touching only changed rows
    if not new:
        sess.createOrReplaceTempView("sess")
        spark.sql(f"""
            MERGE INTO delta.`{table_path}` AS target
            USING sess AS source
            ON target.user_id = source.user_id
            AND target.event_id = source.event_id
            AND target.product_code = source.product_code
            AND target.timestamp = source.timestamp
            AND target.pdate >= '{process_date}'
            and target.pdate = source.pdate
            WHEN MATCHED AND (target.session_id != source.session_id OR (target.session_start IS NULL and source.session_start is not null))
            THEN UPDATE SET *
            WHEN NOT MATCHED
            THEN INSERT *;
            """)
    else:
        sess.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("mergeSchema", "true").partitionBy('pdate').saveAsTable(table_name)

    final = spark.read.format("delta").load(table_path)
    final.orderBy("user_id", "product_code", "timestamp").show(100)


if __name__ == "__main__":
    args = parse_args()
    main(args.process_date, args.new_table)
