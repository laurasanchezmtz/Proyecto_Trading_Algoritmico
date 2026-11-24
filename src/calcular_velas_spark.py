
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

spark = SparkSession.builder.appName("CalcularVelas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Estructura de las cotizaciones
esquema = StructType([
    StructField("symbol", StringType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("timestamp", StringType())
])

# Leer mensajes almacenados en Kafka
datos = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ticks")
    .option("startingOffsets", "earliest")
    .load()
)

# Preparar los precios
ticks = (
    datos
    .select(
        F.from_json(F.col("value").cast("string"), esquema).alias("dato"),
        "partition", "offset"
    )
    .select("dato.*", "partition", "offset")
    .withColumn("fecha", F.to_timestamp("timestamp"))
    .withColumn(
        "precio_medio",
        F.round((F.col("bid") + F.col("ask")) / 2, 5)
    )
    .filter(
        (F.col("symbol") == "EURUSD") &
        (F.col("bid") > 0) &
        (F.col("ask") >= F.col("bid")) &
        F.col("fecha").isNotNull()
    )
)

# Orden cronológico, incluso si dos ticks tienen la misma hora
ticks = ticks.withColumn(
    "orden",
    F.struct("fecha", "partition", "offset")
)

# Agrupar por ventanas de un minuto y calcular OHLC
velas = (
    ticks
    .groupBy("symbol", F.window("fecha", "1 minute"))
    .agg(
        F.min_by("precio_medio", "orden").alias("open"),
        F.max("precio_medio").alias("high"),
        F.min("precio_medio").alias("low"),
        F.max_by("precio_medio", "orden").alias("close"),
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
    .orderBy("inicio")
)

velas.show(truncate=False)

spark.stop()
