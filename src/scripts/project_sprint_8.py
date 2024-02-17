import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, IntegerType

TOPIC_IN = "student.topic.cohort19.elburgan"
TOPIC_OUT = "student.topic.cohort19.elburgan.out"

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    conn_settings = {
        "url": "jdbc:postgresql://localhost:5432/de",
        "user": "jovyan",
        "password": "jovyan",
        "dbtable": "subscribers_feedback"
    }

    df.write \
        .format('jdbc') \
        .option('url', conn_settings['url']) \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', conn_settings['dbtable']) \
        .option('user', conn_settings['user']) \
        .option('password', conn_settings['password']) \
        .mode("append") \
        .save()
    
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

    # очищаем память от df
    df.unpersist()


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

login = "de-student"
password = "ltcneltyn"

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.scram.ScramLoginModule required username={login} password={password};') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', TOPIC_IN) \
    .load()

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

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
#current_timestamp_utc = int(round(unix_timestamp(current_timestamp()))) # РАЗОБРАТЬСЯ КАК ДЕЛАТЬ ТУТ!!!!!

# Определяем текущее время в UTC
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
    .select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_key_value")) \
    .select(col("parsed_key_value.*")) \
    .where((col("adv_campaign_datetime_start") < current_timestamp_utc) & (col("adv_campaign_datetime_end") > current_timestamp_utc))

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, on="restaurant_id", how="inner")\
    .withColumn("trigger_datetime_created", lit(current_timestamp_utc))

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()