import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("RetailBigData_03_SparkSQL") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
# Leer datos limpios desde HDFS (Parquet)
# ─────────────────────────────────────────────
df_ventas     = spark.read.parquet("hdfs://namenode:9000/retail/clean/ventas/")
df_clientes   = spark.read.parquet("hdfs://namenode:9000/retail/clean/clientes/")
df_inventario = spark.read.parquet("hdfs://namenode:9000/retail/clean/inventario/")
df_campanas   = spark.read.parquet("hdfs://namenode:9000/retail/clean/campanas/")

df_ventas.createOrReplaceTempView("ventas")
df_clientes.createOrReplaceTempView("clientes")
df_inventario.createOrReplaceTempView("inventario")
df_campanas.createOrReplaceTempView("campanas")

print("=" * 60)
print("  KPI 1 — TOP 10 PRODUCTOS MÁS VENDIDOS")
print("=" * 60)

top_productos = spark.sql("""
    SELECT
        v.id_producto,
        v.nombre_producto,
        v.categoria,
        SUM(v.cantidad)     AS unidades_vendidas,
        SUM(v.monto_total)  AS ingreso_total,
        ROUND(AVG(v.precio_unitario), 2) AS precio_promedio
    FROM ventas v
    GROUP BY v.id_producto, v.nombre_producto, v.categoria
    ORDER BY unidades_vendidas DESC
    LIMIT 10
""")
top_productos.show(truncate=False)

print()
print("=" * 60)
print("  KPI 2 — VENTAS TOTALES POR TIENDA")
print("=" * 60)

ventas_tienda = spark.sql("""
    SELECT
        tienda,
        COUNT(*)            AS num_transacciones,
        SUM(monto_total)    AS ingreso_total,
        ROUND(AVG(monto_total), 2) AS ticket_promedio,
        SUM(cantidad)       AS unidades_totales
    FROM ventas
    GROUP BY tienda
    ORDER BY ingreso_total DESC
""")
ventas_tienda.show(truncate=False)

print()
print("=" * 60)
print("  KPI 3 — TOP 20 CLIENTES CON MAYOR GASTO")
print("=" * 60)

clientes_vip = spark.sql("""
    SELECT
        v.id_cliente,
        c.nombre,
        c.segmento,
        c.segmento_valor,
        COUNT(*)            AS total_compras,
        SUM(v.monto_total)  AS gasto_total,
        ROUND(AVG(v.monto_total), 2) AS gasto_promedio
    FROM ventas v
    JOIN clientes c ON v.id_cliente = c.id_cliente
    GROUP BY v.id_cliente, c.nombre, c.segmento, c.segmento_valor
    ORDER BY gasto_total DESC
    LIMIT 20
""")
clientes_vip.show(truncate=False)

print()
print("=" * 60)
print("  KPI 4 — VENTAS POR CATEGORÍA Y MES")
print("=" * 60)

ventas_categoria_mes = spark.sql("""
    SELECT
        categoria,
        mes,
        COUNT(*)            AS transacciones,
        SUM(monto_total)    AS ingreso_total
    FROM ventas
    WHERE anio = 2024
    GROUP BY categoria, mes
    ORDER BY categoria, mes
""")
ventas_categoria_mes.show(50, truncate=False)

print()
print("=" * 60)
print("  KPI 5 — ALERTAS DE INVENTARIO BAJO STOCK MÍNIMO")
print("=" * 60)

alertas_inventario = spark.sql("""
    SELECT
        id_producto,
        nombre,
        categoria,
        stock_actual,
        stock_minimo,
        (stock_actual - stock_minimo) AS diferencia,
        estado_stock
    FROM inventario
    WHERE stock_actual < stock_minimo
    ORDER BY diferencia ASC
""")
alertas_inventario.show(truncate=False)
print(f"  ⚠️  Total productos con stock crítico: {alertas_inventario.count()}")

print()
print("=" * 60)
print("  KPI 6 — IMPACTO DE CAMPAÑAS DE MARKETING")
print("=" * 60)

impacto_campanas = spark.sql("""
    SELECT
        c.id_campana,
        c.nombre,
        c.tipo,
        c.segmento_objetivo,
        c.categoria_objetivo,
        c.descuento_pct,
        c.presupuesto_sol,
        c.ventas_generadas,
        ROUND(c.ventas_generadas / c.presupuesto_sol, 2) AS roi_estimado
    FROM campanas c
    ORDER BY roi_estimado DESC
""")
impacto_campanas.show(truncate=False)

# ─────────────────────────────────────────────
# Guardar resultados como CSV para reportes
# ─────────────────────────────────────────────
print()
print("=" * 60)
print("  💾 EXPORTANDO RESULTADOS A HDFS")
print("=" * 60)

top_productos.coalesce(1).write.mode("overwrite").csv(
    "hdfs://namenode:9000/retail/output/top_productos/", header=True
)
ventas_tienda.coalesce(1).write.mode("overwrite").csv(
    "hdfs://namenode:9000/retail/output/ventas_tienda/", header=True
)
clientes_vip.coalesce(1).write.mode("overwrite").csv(
    "hdfs://namenode:9000/retail/output/clientes_vip/", header=True
)
alertas_inventario.coalesce(1).write.mode("overwrite").csv(
    "hdfs://namenode:9000/retail/output/alertas_inventario/", header=True
)

print("  ✅ Reportes guardados en hdfs:///retail/output/")
print()
print("=" * 60)
print("  ✅ Script 03 — Spark SQL completado exitosamente")
print("=" * 60)

spark.stop()
