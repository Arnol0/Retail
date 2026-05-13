Retail Chain: Plataforma Big Data Para Análisis Retail Batch y Streaming
1. Descripción del caso

    Retail Chain es una empresa ficticia de ventas minoristas que opera múltiples tiendas y canales de venta.
    La empresa necesita analizar transacciones históricas y eventos en tiempo real para identificar patrones de compra, productos con mayor demanda, comportamiento de clientes, control de inventario y alertas operativas.

2. Objetivo general

    Construir una solución Big Data utilizando Apache Spark, Python, Kafka y MongoDB para procesar datos batch y streaming, generar indicadores comerciales y almacenar resultados analíticos en una arquitectura escalable.

3. Tecnologías usadas
    Docker
    Python
    Apache Spark / PySpark
    Spark SQL
    RDD
    DataFrames
    Kafka
    MongoDB
    CSV
    JSON
    Parquet
    Matplotlib
4. Resultados esperados
    Ventas por categoría
    Ventas por ciudad
    Segmentación de clientes
    Productos con stock crítico
    Ranking de tiendas con mayores ingresos
    Alertas de inventario en tiempo real
    Reportes CSV
    Archivos Parquet
    Gráficos PNG
    Persistencia en MongoDB
5. Estructura del proyecto
    retail/
    ├── data/
    │   ├── raw/
    │   ├── processed/
    │   └── checkpoints/
    │
    ├── docs/
    ├── notebooks/
    │
    ├── output/
    │   ├── charts/
    │   ├── kpis/
    │   └── streaming/
    │
    ├── src/
    │   ├── 01_generate_retail_data.py
    │   ├── 02_generate_streaming_events.py
    │   ├── 03_batch_etl_spark.py
    │   ├── 04_streaming_spark_kafka.py
    │   ├── 05_visualizations.py
    │   └── 06_export_mongodb.py
    │
    ├── docker-compose.yml
    ├── Dockerfile
    ├── requirements.txt
    └── README.md