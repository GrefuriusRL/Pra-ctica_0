import os
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from spark_session import get_spark_session
from jdbc_connector import guardar_en_sql

spark = get_spark_session()
csv_loc = "ibex35_close-2024.csv"


# ==========================================
# Ej1-a
# ==========================================
print("\nEj1-a")

#Carga datos
df_i = spark.read.option("header", True).option("sep", ";").csv(csv_loc)

#Esquema inicial
df_i.printSchema()

#Convertir Fecha a date
df_fechadate = df_i.withColumn("Fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))

#Convertir precios a Double
for col_name in df_fechadate.columns:
    if col_name != "Fecha":
        df_fechadate = df_fechadate.withColumn(col_name, F.col(f"`{col_name}`").cast("double"))

#Esquema modificado
df_fechadate.printSchema()
df_fechadate.show(6)


# ==========================================
# Ej1-b
# ==========================================
print("\nEj1-b")

df_ej1b = df_fechadate

#Convertir MC a nulo
for col_name in df_ej1b.columns:
    new_col_name = col_name.replace(".MC", "")
    df_ej1b = df_ej1b.withColumnRenamed(col_name, new_col_name)

#6 Primeras Modificas
df_ej1b.show(6)


# ==========================================
# Ej1-c
# ==========================================

print("\nEj1-c")

#Definir StructType
custom = StructType([
    StructField("Fecha", StringType(), True),
    StructField("Iberdrola", DoubleType(), True),
    StructField("Repsol", DoubleType(), True),
    StructField("Naturgy", DoubleType(), True),
    StructField("Endesa", DoubleType(), True),
    StructField("Enagas", DoubleType(), True),
    StructField("Red_Electrica", DoubleType(), True),
    StructField("Banco_Santander", DoubleType(), True),
    StructField("BBVA", DoubleType(), True),
    StructField("CaixaBank", DoubleType(), True),
    StructField("Bankinter", DoubleType(), True),
    StructField("Sabadell", DoubleType(), True),
    StructField("Unicaja", DoubleType(), True),
    StructField("Mapfre", DoubleType(), True),
    StructField("ACS", DoubleType(), True),
    StructField("Acciona", DoubleType(), True),
    StructField("Acciona_Energia", DoubleType(), True),
    StructField("Acerinox", DoubleType(), True),
    StructField("ArcelorMittal", DoubleType(), True),
    StructField("Sacyr", DoubleType(), True),
    StructField("Cellnex", DoubleType(), True),
    StructField("Telefonica", DoubleType(), True),
    StructField("Aena", DoubleType(), True),
    StructField("Ferrovial", DoubleType(), True),
    StructField("Grifols", DoubleType(), True),
    StructField("Amadeus", DoubleType(), True),
    StructField("Indra", DoubleType(), True),
    StructField("Inditex", DoubleType(), True),
    StructField("IAG", DoubleType(), True),
    StructField("Logista", DoubleType(), True),
    StructField("Melia", DoubleType(), True),
    StructField("Merlin", DoubleType(), True),
    StructField("Puig", DoubleType(), True),
    StructField("Rovi", DoubleType(), True),
    StructField("Fluidra", DoubleType(), True),
    StructField("Solaria", DoubleType(), True)
])

#Aplicar StructType
df_ej1c = (spark.read
           .option("header", True)
           .option("sep", ";")
           .schema(custom)
           .csv(csv_loc))

#Convertir Fecha
df_ej1c = df_ej1c.withColumn("Fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))

#Mostrar Modificado
df_ej1c.printSchema()
df_ej1c.show(6)


# ==========================================
# Ej2-a
# ==========================================

print("\nEj2-a")
filas_iniciales = df_ej1b.count()

#Eliminar innecesario
df_ej2a = df_ej1b.dropDuplicates().dropna(how="all", subset=[a for a in df_ej1b.columns if a != "Fecha"])
filas_finales = df_ej2a.count()

#Filas eliminadas
filas_eliminadas = filas_iniciales-filas_finales
print("\nFilas eliminadas: ",filas_eliminadas)

#Contar empresas
empresas = [a for a in df_ej2a.columns if a != "Fecha"]
print(f"\nEmpresas disponibles: {len(empresas)}")
print(f"\nEmpresas: {empresas}")


# ==========================================
# Ej2-b
# ==========================================

print("\nEj2-b")
rango_fechas = df_ej2a.select(F.min("Fecha").alias("fecha_inicio"), F.max("Fecha").alias("fecha_fin")).collect()[0]
fecha_inicio = rango_fechas["fecha_inicio"]
fecha_fin = rango_fechas["fecha_fin"]
total_dias = df_ej2a.count()

print("\nDías con información disponible: ",total_dias)
print("\nEl número de días es inferior al año por los fines de semana y festivos pero no es necesario buscar mas datos ya que en esos dias la bolsa no es afectada")
