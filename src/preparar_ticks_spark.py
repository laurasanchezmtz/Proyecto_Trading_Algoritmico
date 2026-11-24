
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, round, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# Iniciar Spark
spark = SparkSession.builder.appName("PrepararTicks").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Estructura de los ticks definida por el profesor
esquema = StructType([
    StructField("symbol", StringType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("timestamp", StringType())
])

# Leer cotizaciones desde Kafka
datos = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ticks")
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir JSON a columnas
ticks = (
    datos
    .select(from_json(col("value").cast("string"), esquema).alias("dato"))
    .select("dato.*")
    .withColumn("fecha", to_timestamp("timestamp"))
    .withColumn("precio_medio", round((col("bid") + col("ask")) / 2, 5))
    .filter(
        (col("symbol") == "EURUSD") &
        (col("bid") > 0) &
        (col("ask") >= col("bid")) &
        col("fecha").isNotNull()
    )
)

# Mostrar el resultado
consulta = (
    ticks.select("symbol", "fecha", "bid", "ask", "precio_medio")
    .writeStream
    .format("console")
    .option("truncate", False)
    .trigger(availableNow=True)
    .start()
)

consulta.awaitTermination()
spark.stop()
