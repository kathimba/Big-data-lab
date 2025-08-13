from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "transactions"  # Change to your Kafka topic

# Read from Kafka as a streaming DataFrame
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka message value is in 'value' column as bytes, cast to string
messages = df.selectExpr("CAST(value AS STRING) as message")

# Optional: Define a schema if your messages are JSON and you want to parse them
# schema = StructType().add("field1", StringType()).add("field2", StringType())
# messages = messages.select(from_json(col("message"), schema).alias("data")).select("data.*")

# Print to console
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
