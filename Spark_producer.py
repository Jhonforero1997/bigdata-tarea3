# ============================================================
# Análisis de Ventas - Dataset Amazon AWS (Spark Batch)
# Basado en el Anexo 2 de la Tarea 3 – Procesamiento de Datos
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ------------------------------------------------------------
# Inicializar la sesión de Spark
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Analisis_Ventas_AmazonAWS") \
    .getOrCreate()

# ------------------------------------------------------------
#  Cargar el archivo CSV desde HDFS
# ------------------------------------------------------------
file_path = "hdfs://localhost:9000/AmazonAWS/data.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# ------------------------------------------------------------
# Exploración inicial del conjunto de datos
# ------------------------------------------------------------
print(" Esquema del DataFrame:")
df.printSchema()

print(" Primeras filas del dataset:")
df.show(10, truncate=False)

# ------------------------------------------------------------
#  Limpieza y validación de los datos
# ------------------------------------------------------------
print(" Conteo de valores nulos por columna:")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Eliminar filas duplicadas
df = df.dropDuplicates()

# ------------------------------------------------------------
#  Conversión y normalización de fechas
# ------------------------------------------------------------
# Usa try_to_timestamp para tolerar diferentes formatos de fecha/hora
df = df.withColumn(
    "InvoiceDate",
    F.expr("""
        coalesce(
            try_to_timestamp(InvoiceDate, "d/M/yyyy H:mm"),
            try_to_timestamp(InvoiceDate, "dd/MM/yyyy HH:mm"),
            try_to_timestamp(InvoiceDate, "M/d/yyyy H:mm")
        )
    """)
)

# Extraer solo la fecha (sin hora)
df = df.withColumn("Fecha", F.to_date("InvoiceDate"))

# Calcular el total de cada línea de factura
df = df.withColumn("Total", F.col("Quantity") * F.col("UnitPrice"))

# ------------------------------------------------------------
#  Estadísticas generales del dataset
# ------------------------------------------------------------
print(" Estadísticas generales del dataset:")
df.describe(["Quantity", "UnitPrice", "Total"]).show()

# ------------------------------------------------------------
#  Análisis específicos de negocio
# ------------------------------------------------------------

#  Ventas totales por país
print(" Ventas totales por país:")
df.groupBy("Country") \
  .agg(F.round(F.sum("Total"), 2).alias("Total_Ventas")) \
  .orderBy(F.desc("Total_Ventas")) \
  .show(10, truncate=False)

#  Productos más vendidos (por cantidad total)
print(" Productos más vendidos:")
df.groupBy("Description") \
  .agg(F.sum("Quantity").alias("Unidades_Vendidas")) \
  .orderBy(F.desc("Unidades_Vendidas")) \
  .show(10, truncate=False)

#  Productos más rentables (por monto total)
print(" Productos más rentables:")
df.groupBy("Description") \
  .agg(F.round(F.sum("Total"), 2).alias("Total_Ganancia")) \
  .orderBy(F.desc("Total_Ganancia")) \
  .show(10, truncate=False)

#  Clientes con mayor volumen de compra
print(" Clientes con mayor volumen de compra:")
df.groupBy("CustomerID") \
  .agg(F.round(F.sum("Total"), 2).alias("Total_Comprado")) \
  .orderBy(F.desc("Total_Comprado")) \
  .show(10, truncate=False)

#  Ventas totales por fecha (sin hora)
print(" Ventas diarias totales:")
df.groupBy("Fecha") \
  .agg(F.round(F.sum("Total"), 2).alias("Ventas_Diarias")) \
  .orderBy("Fecha") \
  .show(20, truncate=False)

# ------------------------------------------------------------
#  Guardar los resultados procesados en HDFS
# ------------------------------------------------------------
output_path = "hdfs://localhost:9000/AmazonAWS/resultados"

df.write.mode("overwrite").option("header", "true").csv(output_path)
print(f" Resultados guardados en: {output_path}")

# ------------------------------------------------------------
#  Finalizar la sesión
# ------------------------------------------------------------
spark.stop()
print(" Análisis finalizado correctamente.")