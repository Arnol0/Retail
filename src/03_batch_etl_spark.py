"""
Archivo: 03_batch_etl_spark.py

Proyecto:
Retail Chain - Big Data Analytics
"""

from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"

PROCESSED_DIR = BASE_DIR / "data" / "processed"

KPI_DIR = BASE_DIR / "output" / "kpis"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
KPI_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# CREAR SESIÓN SPARK
# ============================================================

def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName("RetailBatchETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def read_csv(spark: SparkSession, filename: str):

    path = str(RAW_DIR / filename)

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .csv(path)
    )


def read_json(spark: SparkSession, filename: str):

    path = str(RAW_DIR / filename)

    return (
        spark.read
        .option("multiline", "true")
        .json(path)
    )


def write_single_csv(df, output_filename: str) -> None:

    final_path = KPI_DIR / output_filename

    temp_dir = KPI_DIR / f"_tmp_{output_filename.replace('.csv', '')}"

    if final_path.exists():
        final_path.unlink()

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(temp_dir))
    )

    part_files = list(temp_dir.glob("part-*.csv"))

    if not part_files:
        raise FileNotFoundError(
            f"No se encontró archivo part en {temp_dir}"
        )

    shutil.move(str(part_files[0]), str(final_path))

    shutil.rmtree(temp_dir)

    print(f"Reporte creado: output/kpis/{output_filename}")


def show_basic_info(name: str, df) -> None:

    print("-" * 80)
    print(f"DataFrame: {name}")
    print(f"Registros: {df.count():,}")
    print(f"Columnas: {len(df.columns)}")
    print(df.columns)


# ============================================================
# PROCESO PRINCIPAL ETL
# ============================================================

def main() -> None:

    print("=" * 80)
    print("Iniciando ETL Batch Retail con Apache Spark")
    print("=" * 80)

    spark = create_spark_session()

    # --------------------------------------------------------
    # EXTRACT
    # --------------------------------------------------------

    print("\nLeyendo datasets retail...")

    clientes_df = read_csv(spark, "clientes.csv")

    ventas_df = read_csv(spark, "transacciones_ventas.csv")

    productos_df = read_json(spark, "inventario_productos.json")

    campanas_df = read_json(spark, "campanas_marketing.json")

    show_basic_info("clientes_df", clientes_df)
    show_basic_info("ventas_df", ventas_df)
    show_basic_info("productos_df", productos_df)
    show_basic_info("campanas_df", campanas_df)

    # ============================================================
    # TRANSFORMACIÓN DE DATOS
    # ============================================================

    print("\nTransformando datos...")

    ventas_df = (
        ventas_df
        .withColumn(
            "cantidad",
            F.col("cantidad").cast("int")
        )
        .withColumn(
            "precio_unitario",
            F.col("precio_unitario").cast("double")
        )
        .withColumn(
            "monto_total",
            F.col("monto_total").cast("double")
        )
        .withColumn(
            "fecha",
            F.to_timestamp("fecha")
        )
    )

    productos_df = (
        productos_df
        .withColumn(
            "precio",
            F.col("precio").cast("double")
        )
        .withColumn(
            "stock_actual",
            F.col("stock_actual").cast("int")
        )
        .withColumn(
            "stock_minimo",
            F.col("stock_minimo").cast("int")
        )
    )

    # ============================================================
    # RENOMBRAR COLUMNAS PARA EVITAR ERRORES
    # ============================================================

    clientes_df = clientes_df.withColumnRenamed(
        "nombre",
        "nombre_cliente"
    )

    productos_df = productos_df.withColumnRenamed(
        "nombre",
        "nombre_producto"
    )

    # ============================================================
    # INTEGRACIÓN DE DATASETS
    # ============================================================

    print("\nIntegrando datasets retail...")

    ventas_enriquecidas_df = (
        ventas_df
        .join(clientes_df, on="id_cliente", how="left")
        .join(productos_df, on="id_producto", how="left")
    )

    print("\nVista previa dataset enriquecido:")

    ventas_enriquecidas_df.select(
        "id_transaccion",
        "nombre_cliente",
        "nombre_producto",
        "categoria",
        "cantidad",
        "monto_total",
        "stock_actual"
    ).show(10, truncate=False)

    # --------------------------------------------------------
    # LOAD - PARQUET
    # --------------------------------------------------------

    print("\nGuardando dataset procesado en Parquet...")

    parquet_path = PROCESSED_DIR / "ventas_enriquecidas.parquet"

    if parquet_path.exists():
        shutil.rmtree(parquet_path)

    (
        ventas_enriquecidas_df
        .write
        .mode("overwrite")
        .parquet(str(parquet_path))
    )

    print(
        "Archivo Parquet creado: "
        "data/processed/ventas_enriquecidas.parquet"
    )

    # --------------------------------------------------------
    # SPARK SQL
    # --------------------------------------------------------

    print("\nCreando vistas temporales Spark SQL...")

    ventas_enriquecidas_df.createOrReplaceTempView(
        "ventas_analytics"
    )

    productos_df.createOrReplaceTempView(
        "productos_analytics"
    )

    campanas_df.createOrReplaceTempView(
        "campanas_analytics"
    )

    # --------------------------------------------------------
    # KPI 1 - TOP PRODUCTOS
    # --------------------------------------------------------

    top_productos_df = spark.sql("""
        SELECT
            categoria,
            COUNT(*) AS total_ventas,
            ROUND(SUM(monto_total), 2) AS ingresos_totales,
            ROUND(AVG(monto_total), 2) AS ticket_promedio
        FROM ventas_analytics
        GROUP BY categoria
        ORDER BY ingresos_totales DESC
    """)

    print("\nKPI: Top productos vendidos")

    top_productos_df.show(truncate=False)

    write_single_csv(
        top_productos_df,
        "top_productos_vendidos.csv"
    )

    # --------------------------------------------------------
    # KPI 2 - VENTAS POR CIUDAD
    # --------------------------------------------------------

    ventas_por_ciudad_df = spark.sql("""
        SELECT
            ciudad,
            COUNT(*) AS total_transacciones,
            ROUND(SUM(monto_total), 2) AS ingresos_totales,
            ROUND(AVG(monto_total), 2) AS ticket_promedio
        FROM ventas_analytics
        GROUP BY ciudad
        ORDER BY ingresos_totales DESC
    """)

    print("\nKPI: Ventas por ciudad")

    ventas_por_ciudad_df.show(truncate=False)

    write_single_csv(
        ventas_por_ciudad_df,
        "ventas_por_ciudad.csv"
    )

    # --------------------------------------------------------
    # KPI 3 - SEGMENTACIÓN CLIENTES
    # --------------------------------------------------------

    segmentacion_clientes_df = spark.sql("""
        SELECT
            segmento,
            COUNT(DISTINCT id_cliente) AS total_clientes,
            ROUND(SUM(monto_total), 2) AS gasto_total,
            ROUND(AVG(monto_total), 2) AS gasto_promedio
        FROM ventas_analytics
        GROUP BY segmento
        ORDER BY gasto_total DESC
    """)

    print("\nKPI: Segmentación de clientes")

    segmentacion_clientes_df.show(truncate=False)

    write_single_csv(
        segmentacion_clientes_df,
        "segmentacion_clientes.csv"
    )

    # --------------------------------------------------------
    # KPI 4 - STOCK CRÍTICO
    # --------------------------------------------------------

    stock_critico_df = spark.sql("""
        SELECT
            id_producto,
            nombre_producto,
            categoria,
            stock_actual,
            stock_minimo,
            proveedor
        FROM productos_analytics
        WHERE stock_actual <= stock_minimo
        ORDER BY stock_actual ASC
    """)

    print("\nKPI: Productos con stock crítico")

    stock_critico_df.show(truncate=False)

    write_single_csv(
        stock_critico_df,
        "productos_stock_critico.csv"
    )

    # --------------------------------------------------------
    # KPI 5 - CAMPAÑAS MARKETING
    # --------------------------------------------------------

    campanas_segmento_df = spark.sql("""
        SELECT
            segmento_objetivo,
            canal,
            COUNT(*) AS total_campanas,
            ROUND(AVG(descuento_porcentaje), 2) AS descuento_promedio
        FROM campanas_analytics
        GROUP BY segmento_objetivo, canal
        ORDER BY total_campanas DESC
    """)

    print("\nKPI: Campañas por segmento")

    campanas_segmento_df.show(truncate=False)

    write_single_csv(
        campanas_segmento_df,
        "campanas_por_segmento.csv"
    )

    # --------------------------------------------------------
    # KPI 6 - TIENDAS CON MÁS INGRESOS
    # --------------------------------------------------------

    tiendas_ingresos_df = spark.sql("""
        SELECT
            tienda,
            COUNT(*) AS total_ventas,
            ROUND(SUM(monto_total), 2) AS ingresos_totales
        FROM ventas_analytics
        GROUP BY tienda
        ORDER BY ingresos_totales DESC
    """)

    print("\nKPI: Tiendas con más ingresos")

    tiendas_ingresos_df.show(truncate=False)

    write_single_csv(
        tiendas_ingresos_df,
        "tiendas_mayores_ingresos.csv"
    )

    # --------------------------------------------------------
    # USO DE RDD
    # --------------------------------------------------------

    print("\nProcesando resumen mediante RDD...")

    ventas_rdd = spark.sparkContext.textFile(
        str(RAW_DIR / "transacciones_ventas.csv")
    )

    header = ventas_rdd.first()

    ventas_por_tienda = (
        ventas_rdd
        .filter(lambda line: line != header)
        .map(lambda line: line.split(",")[7])
        .map(lambda tienda: (tienda, 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    rdd_ventas_tienda_df = spark.createDataFrame(
        ventas_por_tienda,
        ["tienda", "total_ventas"]
    ).orderBy(F.desc("total_ventas"))

    print("\nReporte RDD: Ventas por tienda")

    rdd_ventas_tienda_df.show(truncate=False)

    write_single_csv(
        rdd_ventas_tienda_df,
        "rdd_ventas_por_tienda.csv"
    )

    # --------------------------------------------------------
    # CIERRE
    # --------------------------------------------------------

    print("=" * 80)
    print("ETL Batch Retail finalizado correctamente")
    print("Datos procesados guardados en data/processed/")
    print("KPIs guardados en output/kpis/")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()