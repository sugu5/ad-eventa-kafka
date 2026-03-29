from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_csv)
        .option("subscribe", Config.TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.request.timeout.ms", "40000")
        .option("kafka.max.poll.interval.ms", "300000")
        .option("failOnDataLoss", "false")
        .load()
    )
    logger.info("✓ Kafka stream connected")

