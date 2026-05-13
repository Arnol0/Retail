from pyspark.sql import SparkSession
from pyspark.sql.functions import round

# ============================================================
# 1. CONFIGURACIÓN DE SPARK
# ============================================================

spark = SparkSession.builder \
    .appName("RetailBatchAnalysis") \
    .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://retail-mongodb:27017/retail_db"
    ) \
    .getOrCreate()

print("\n" + "=" * 50)
print("EJECUTANDO PROCESAMIENTO BATCH (AA4)")
print("=" * 50)

# ============================================================
# 2. RDD - PROCESAMIENTO DE LOGS
# ============================================================

try:
    path_logs = "/app/data/raw/logs_actividad_tienda.txt"

    logs_rdd = spark.sparkContext.textFile(path_logs)

    errores_rdd = logs_rdd \
        .filter(lambda line: "[ERROR]" in line) \
        .map(lambda line: line.upper())

    print(f"\n[RDD] Registros de error encontrados: {errores_rdd.count()}")

except Exception as e:
    print(f"\n[ERROR RDD] {e}")

# ============================================================
# 3. DATAFRAMES Y SPARK SQL
# ============================================================

try:
    # Cargar archivos CSV
    df_ventas = spark.read.csv(
        "/app/data/raw/transacciones_ventas.csv",
        header=True,
        inferSchema=True
    )

    df_clientes = spark.read.csv(
        "/app/data/raw/clientes.csv",
        header=True,
        inferSchema=True
    )

    # Crear vistas temporales
    df_ventas.createOrReplaceTempView("tabla_ventas")
    df_clientes.createOrReplaceTempView("tabla_clientes")

    # KPI con Spark SQL
    kpi_ventas = spark.sql("""
        SELECT
            c.segmento,
            ROUND(SUM(v.monto_total), 2) AS total_ingresos,
            COUNT(v.id_transaccion) AS cantidad_ventas
        FROM tabla_ventas v
        JOIN tabla_clientes c
            ON v.id_cliente = c.id_cliente
        GROUP BY c.segmento
        ORDER BY total_ingresos DESC
    """)

    print("\n[SQL] KPI DE VENTAS POR SEGMENTO")
    kpi_ventas.show()

except Exception as e:
    print(f"\n[ERROR SQL] {e}")

# ============================================================
# 4. GUARDAR RESULTADOS EN MONGODB
# ============================================================

try:
    print("\n[MongoDB] Guardando resultados...")

    # Guardar KPIs
    kpi_ventas.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", "retail_db") \
        .option("collection", "kpi_ventas_segmento") \
        .save()

    # Convertir RDD a DataFrame
    df_errores = errores_rdd.map(
        lambda x: (x,)
    ).toDF(["log_detalle"])

    # Guardar logs
    df_errores.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", "retail_db") \
        .option("collection", "alertas_criticas") \
        .save()

    print("\n" + "=" * 50)
    print("¡PROCESO COMPLETADO EXITOSAMENTE!")
    print("=" * 50)

except Exception as e:
    print(f"\n[ERROR MONGODB] {e}")

# ============================================================
# 5. CERRAR SPARK
# ============================================================

spark.stop()