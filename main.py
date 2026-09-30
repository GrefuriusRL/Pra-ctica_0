import os
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from config.spark_session import get_spark_session
from models.jdbc_connector import guardar_en_sql

spark = get_spark_session()
csv_loc = "./data/ibex35_close-2024.csv"


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

# ==========================================
# Ej3
# ==========================================

print("\nEj3")
#Renombrar Fecha a Dia
df_ej3 = df_ej2a.withColumnRenamed("Fecha", "Dia")
df_ej3.show(10)

# Media, Máximo y Mínimo
for empresa in empresas:
    mmm = df_ej3.select(
        F.avg(F.col(empresa)).alias("Media anual"),
        F.max(F.col(empresa)).alias("Max anual"),
        F.min(F.col(empresa)).alias("Min anual")
    )
    print(f"--- Para {empresa} ---")
    mmm.show()

# Columna Deficiency Notice UNI
col_uni = "UNI" if "UNI" in empresas else empresas[0]
df_ej3 = df_ej3.withColumn(
    "Deficiency Notice UNI",
    F.when(F.col(col_uni) < 1.0, True).otherwise(False)
)
df_ej3.show(100)


# ==========================================
# Ej4
# ==========================================
print("\nEj4")
#Calcula Variación Anual
fecha_min = df_ej3.select(F.min("Dia")).collect()[0][0]
fecha_max = df_ej3.select(F.max("Dia")).collect()[0][0]

for empresa in empresas:
    val_inicial = df_ej3.filter(F.col("Dia") == fecha_min).select(empresa).collect()[0][0]
    val_final = df_ej3.filter(F.col("Dia") == fecha_max).select(empresa).collect()[0][0]
    
    if val_inicial is not None and val_final is not None:
        variacion = ((val_final - val_inicial) / val_inicial) * 100
        
        if variacion <= -15:
            clasificacion = "Bajada Fuerte"
        elif variacion < -1:
            clasificacion = "Bajada"
        elif -1 <= variacion <= 1:
            clasificacion = "Neutra"
        elif variacion < 15:
            clasificacion = "Subida"
        else:
            clasificacion = "Subida Fuerte"
            
        print(f"\nEmpresa: {empresa} -- Variación: {variacion:.2f}% -- Clasificación: {clasificacion}")

# ==========================================
# Ej5
# ==========================================
print("\nEj5")
df_ej5 = df_ej3

for empresa in empresas:
    q1, q2, q3 = df_ej5.approxQuantile(empresa, [0.25, 0.50, 0.75], 0.01)
    
    columna = f"{empresa} Cuartil"
    df_ej5 = df_ej5.withColumn(
        columna,
        F.when(F.col(empresa) <= q1, "q1")
         .when((F.col(empresa) > q1) & (F.col(empresa) <= q2), "q2")
         .when((F.col(empresa) > q2) & (F.col(empresa) <= q3), "q3")
         .otherwise("q4")
    )

print("\nPrimera fila:")
df_ej5.show(1)

print("\nColumna AENA y BBVA:")
cols = ["Dia"] + [c for c in df_ej5.columns if "AENA" in c.upper() or "BBVA" in c.upper()]
df_ej5.select(cols).show(truncate=False)

# ==========================================
# Guardar en Base de Datos SQL via JDBC
# ==========================================
#Almacena los datos del CSV en la tabla Datos2024
guardar_en_sql(df_i, nombre_tabla="Datos2024", modo="overwrite")

#Almacenar los datos tratados
guardar_en_sql(df_ej5, nombre_tabla="Datos2024", modo="overwrite")