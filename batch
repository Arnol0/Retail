import subprocess
from pyspark.sql import SparkSession

# ─────────────────────────────────────────────
# 1. Configurar sesión Spark
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("RetailBigData_01_LecturaHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=" * 60)
print("  PASO 1 — CREAR DIRECTORIOS EN HDFS")
print("=" * 60)

# ─────────────────────────────────────────────
# 2. Crear estructura de directorios en HDFS
# ─────────────────────────────────────────────
dirs = [
    "hdfs://namenode:9000/retail/raw/ventas",
    "hdfs://namenode:9000/retail/raw/clientes",
    "hdfs://namenode:9000/retail/raw/inventario",
    "hdfs://namenode:9000/retail/raw/marketing",
    "hdfs://namenode:9000/retail/raw/logs",
]

for d in dirs:
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", d], check=False)
    print(f"  📁 Directorio creado: {d}")

print()
print("=" * 60)
print("  PASO 2 — SUBIR ARCHIVOS A HDFS")
print("=" * 60)

# ─────────────────────────────────────────────
# 3. Subir archivos desde /app/data a HDFS
# ─────────────────────────────────────────────
archivos = [
    ("/app/data/transacciones_ventas.csv",   "hdfs://namenode:9000/retail/raw/ventas/"),
    ("/app/data/clientes.csv",               "hdfs://namenode:9000/retail/raw/clientes/"),
    ("/app/data/inventario_productos.json",  "hdfs://namenode:9000/retail/raw/inventario/"),
    ("/app/data/campanas_marketing.json",    "hdfs://namenode:9000/retail/raw/marketing/"),
    ("/app/data/logs_actividad_tienda.txt",  "hdfs://namenode:9000/retail/raw/logs/"),
]

for local, hdfs_dest in archivos:
    result = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local, hdfs_dest],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"  ✅ Subido: {local.split('/')[-1]} → {hdfs_dest}")
    else:
        print(f"  ❌ Error: {result.stderr}")

print()
print("=" * 60)
print("  PASO 3 — LEER Y VERIFICAR CON SPARK")
print("=" * 60)

# ─────────────────────────────────────────────
# 4. Leer y mostrar schema de cada archivo
# ─────────────────────────────────────────────
df_ventas = spark.read.csv(
    "hdfs://namenode:9000/retail/raw/ventas/",
    header=True, inferSchema=True
)

df_clientes = spark.read.csv(
    "hdfs://namenode:9000/retail/raw/clientes/",
    header=True, inferSchema=True
)

df_inventario = spark.read.json(
    "hdfs://namenode:9000/retail/raw/inventario/"
)

df_campanas = spark.read.json(
    "hdfs://namenode:9000/retail/raw/marketing/"
)

df_logs = spark.read.text(
    "hdfs://namenode:9000/retail/raw/logs/"
)

print("\n📄 transacciones_ventas.csv")
print(f"   Registros : {df_ventas.count()}")
print(f"   Columnas  : {len(df_ventas.columns)}")
df_ventas.printSchema()

print("\n📄 clientes.csv")
print(f"   Registros : {df_clientes.count()}")
df_clientes.printSchema()

print("\n📄 inventario_productos.json")
print(f"   Registros : {df_inventario.count()}")
df_inventario.printSchema()

print("\n📄 campanas_marketing.json")
print(f"   Registros : {df_campanas.count()}")
df_campanas.printSchema()

print("\n📄 logs_actividad_tienda.txt")
print(f"   Registros : {df_logs.count()}")

print()
print("=" * 60)
print("  ✅ Script 01 completado exitosamente")
print("=" * 60)

spark.stop()
