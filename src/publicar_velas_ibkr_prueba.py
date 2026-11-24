
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

spark = (
    SparkSession.builder
    .appName("VelasOHLCIBKRPrueba")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

esquema = StructType([
    StructField("symbol", StringType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("timestamp", StringType())
])

# Leer nuevos ticks desde Kafka
datos = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ticks_ibkr_prueba")
    .option("startingOffsets", "latest")
    .load()
)

# Preparar cotizaciones
ticks = (
    datos
    .select(
        F.from_json(F.col("value").cast("string"), esquema).alias("dato"),
        "partition", "offset"
    )
    .select("dato.*", "partition", "offset")
    .withColumn("fecha", F.to_timestamp("timestamp"))
    .withColumn(
        "precio",
        F.round((F.col("bid") + F.col("ask")) / 2, 5)
    )
    .filter(
        (F.col("symbol") == "EURUSD") &
        (F.col("bid") > 0) &
        (F.col("ask") >= F.col("bid")) &
        F.col("fecha").isNotNull()
    )
    .withWatermark("fecha", "10 seconds")
    .withColumn(
        "orden",
        F.struct("fecha", "partition", "offset")
    )
)

# Agrupar cotizaciones en velas de un minuto
velas = (
    ticks
    .groupBy("symbol", F.window("fecha", "1 minute"))
    .agg(
        F.min_by("precio", "orden").alias("open"),
        F.max("precio").alias("high"),
        F.min("precio").alias("low"),
        F.max_by("precio", "orden").alias("close"),
        F.count("*").alias("num_ticks")
    )
    .select(
        "symbol",
        F.date_format(
            F.col("window.start"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'"
        ).alias("inicio"),
        "open", "high", "low", "close", "num_ticks"
    )
)

# Convertir cada vela al JSON requerido
mensajes = velas.select(
    F.col("symbol").alias("key"),
    F.to_json(
        F.struct(
            "symbol", "inicio", "open",
            "high", "low", "close", "num_ticks"
        )
    ).alias("value")
)

# Publicar las velas completas en Kafka
consulta = (
    mensajes.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "velas_ibkr_prueba")
    .option("checkpointLocation", "/checkpoint/velas_ibkr_prueba")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print(
    "Streaming iniciado: ticks_ibkr_prueba -> velas_ibkr_prueba",
    flush=True
)

consulta.awaitTermination()
