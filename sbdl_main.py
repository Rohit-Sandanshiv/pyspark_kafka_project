import sys
import uuid

# from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.functions import struct, col, to_json
from pyspark.sql.types import *
from lib.logger import Log4j
from lib.utils import *
from lib.DataLoader import *
from lib.transformation import *
from lib.kafka_stream import write_kafka
import datetime

if __name__ == '__main__':
    print("python session started...")

    if len(sys.argv) < 2:
        print("Usage: Environment is missing {dev, qa, prod}")

    job_run_env = sys.argv[1].upper()
    load_date = datetime.date.today()
    job_run_id = "SBDL-" + str(uuid.uuid4())

    print(job_run_env)
    print(load_date)
    print(job_run_id)
    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id + " Date: " + str(load_date) + "...")

    print("Initializing Spark Session...")
    spark = getSpark_Session(job_run_env)

    logger = Log4j(spark)
    logger.info("Pyspark Session started...")

    logger.info("Reading SBDL account df...")
    df_accounts = read_accounts(spark)
    df_accounts_transformed = get_account_transformed_df(df_accounts)
    df_accounts_transformed.show()

    logger.info("Reading SBDL Parties df...")
    df_party = read_parties(spark)
    df_party_transformed = get_party_transformed_df(df_party)
    df_party_transformed.show()

    logger.info("Reading SBDL Addresses df...")
    df_address = read_address(spark)
    df_address_transformed = get_address_transformed_df(df_address)
    df_address_transformed.show()

    logger.info("Join Party Relations and Address...")
    party_address_df = join_party_address(df_party_transformed, df_address_transformed)

    logger.info("Join Account and Parties...")
    data_df = join_accounts(df_accounts_transformed, party_address_df)
    data_df.show()

    logger.info("Apply Header and create Event...")
    final_df = apply_header(spark, data_df)
    final_df.show()
    print(final_df.collect())
    print(final_df.select('payload').collect())

    logger.info("Preparing to send data to Kafka...")
    # kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
    #                               to_json(struct("*")).alias("value"))

    kafka_kv_df = final_df.select(
        col("payload.contractIdentifier.newValue").cast("string").alias("key"),  # Key must be string
        to_json(struct(*[col(c) for c in final_df.columns])).alias("value")  # Convert all columns to JSON
    )

    # kafka_kv_df.write \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "sbdl_topic") \
    #     .save()

    write_kafka(kafka_kv_df)

    logger.info("Finished sending data to Kafka...")

    logger.info("Pyspark Session ended...")