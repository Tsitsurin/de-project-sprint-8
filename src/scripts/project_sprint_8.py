import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, IntegerType

import logging
# Настроим логгер
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

import json
with open("credentials.txt") as file:
    c = json.load(file)

TOPIC_IN = c["TOPIC_IN"]
TOPIC_OUT = c["TOPIC_OUT"]
login_kafka = c["login_kafka"]
password_kafka = c["password_kafka"]
conn_settings = c["conn_settings"]
url = c["url"]
dbtable = c["dbtable"]
user = c["user"]
password = c["password"]

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
def create_spark_session():
    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    
    return spark

# читаем из топика Kafka сообщения с акциями от ресторанов 
def read_kafka_stream():
    restaurant_read_stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.scram.ScramLoginModule required username={login_kafka} password={password_kafka};') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('subscribe', TOPIC_IN) \
        .load()
    
    return restaurant_read_stream_df

# Определяем текущее время в UTC
def get_current_timestamp_utc():
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))
    return current_timestamp_utc

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
def filter_stream_data(df, current_timestamp_utc):

    # определяем схему входного сообщения для json
    incomming_message_schema = StructType([
        StructField("restaurant_id",StringType()),
        StructField("adv_campaign_id",StringType()),
        StructField("adv_campaign_content",StringType()),
        StructField("adv_campaign_owner",StringType()),
        StructField("adv_campaign_owner_contact",StringType()),
        StructField("adv_campaign_datetime_start",IntegerType()),
        StructField("adv_campaign_datetime_end",IntegerType()),
        StructField("datetime_created",IntegerType())
    ])

    filtered_read_stream_df = df \
        .select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_key_value")) \
        .select(col("parsed_key_value.*")) \
        .where((col("adv_campaign_datetime_start") < current_timestamp_utc) & (col("adv_campaign_datetime_end") > current_timestamp_utc))
    
    return filtered_read_stream_df



# вычитываем всех пользователей с подпиской на рестораны
def read_subscribers_data(spark):
    subscribers_restaurant_df = spark.read \
                        .format('jdbc') \
                        .option('url', url) \
                        .option('driver', 'org.postgresql.Driver') \
                        .option('dbtable', dbtable) \
                        .option('user', user) \
                        .option('password', password) \
                        .load()
    
    return subscribers_restaurant_df

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
def join_and_transform_data(filtered_data, subscribers_data):
    result_df = filtered_data.join(subscribers_data, on="restaurant_id", how="inner")\
        .withColumn("trigger_datetime_created", lit(current_timestamp_utc))
    return result_df

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def save_to_postgresql_and_kafka(df):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    try:
        df.write \
            .format('jdbc') \
            .option('url', conn_settings['url']) \
            .option('driver', 'org.postgresql.Driver') \
            .option('dbtable', conn_settings['dbtable']) \
            .option('user', conn_settings['user']) \
            .option('password', conn_settings['password']) \
            .save()
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")
    
    try:
        # создаём df для отправки в Kafka. Сериализация в json.
        kafka_df = df.select(f.to_json(f.struct('restaurant_id',
                                                'adv_campaign_id',
                                                'adv_campaign_content',
                                                'adv_campaign_owner',
                                                'adv_campaign_owner_contact',
                                                'adv_campaign_datetime_start',
                                                'adv_campaign_datetime_end',
                                                'client_id',
                                                'datetime_created',
                                                'trigger_datetime_created')).alias('value')
        )

        # отправляем сообщения в результирующий топик Kafka без поля feedback
        kafka_df.writeStream \
                    .outputMode("append") \
                    .format("kafka") \
                    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
                    .options(**kafka_security_options) \
                    .option("topic", TOPIC_OUT) \
                    .option("checkpointLocation", "project_8") \
                    .trigger(processingTime="15 seconds") \
                    .start()
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")

    # очищаем память от df
    df.unpersist()

if __name__ == "__main__":
    spark = create_spark_session()
    restaurant_read_stream_df = read_kafka_stream()
    current_timestamp_utc = get_current_timestamp_utc()
    filtered_data = filter_stream_data(restaurant_read_stream_df, current_timestamp_utc)
    subscribers_data = read_subscribers_data(spark)
    result_df = join_and_transform_data(filtered_data, subscribers_data)
    save_to_postgresql_and_kafka(result_df)
    spark.stop() 