from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.appName("LeerTicksKafka").getOrCreate()

# Mostrar menos mensajes técnicos en pantalla
spark.sparkContext.setLogLevel("ERROR")

# Leer las cotizaciones del tópico ticks
datos = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ticks")
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir el mensaje a texto
mensajes = datos.selectExpr("CAST(value AS STRING) AS mensaje")

# Mostrar los mensajes recibidos
consulta = (
    mensajes.writeStream
    .format("console")
    .option("truncate", False)
    .trigger(availableNow=True)
    .start()
)

consulta.awaitTermination()
spark.stop()