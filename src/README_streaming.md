# 📡 Módulo de Streaming — Rapídex

> **Autor:** Grupo 11 — Parte: Streaming Events  
> **Tecnologías:** Apache Kafka + Spark Structured Streaming + Python

---

## 📋 Descripción

Este módulo implementa el procesamiento de eventos en tiempo real para la plataforma **Rapídex**, una empresa ficticia de delivery en Lima. Los eventos son generados por un productor Kafka y procesados en tiempo real por Spark Structured Streaming con micro-batches de 10 segundos.

---

## 🗂️ Archivos

| Archivo | Descripción |
|---|---|
| `crear_topic_kafka.py` | Crea el topic `rapidex-eventos` en Kafka con 3 particiones |
| `kafka_producer.py` | Genera y publica 2,000 eventos simulados en Kafka |
| `spark_streaming.py` | Consume los eventos y aplica procesamiento en tiempo real |

---

## 🔄 Tipos de Eventos

| Tipo | Distribución | Campos específicos |
|---|---|---|
| `PEDIDO_REALIZADO` | 40% | monto_total, tiempo_estimado, num_items |
| `PEDIDO_ENTREGADO` | 35% | monto_total, tiempo_real, calificacion |
| `PEDIDO_RETRASADO` | 15% | minutos_retraso, motivo_retraso |
| `PEDIDO_CANCELADO` | 10% | monto_total, motivo_cancelacion |

---

## 📊 Queries del Streaming

| Query | Descripción | Frecuencia |
|---|---|---|
| Resumen 1 | Total pedidos y monto acumulado por distrito | Cada 10 seg |
| Resumen 2 | Cancelaciones agrupadas por motivo | Cada 30 seg |
| Alerta 1 | Pedidos con retraso mayor a 30 minutos | Cada 10 seg |
| Alerta 2 | Repartidores con calificación promedio < 2.5 | Cada 30 seg |

---

## 🚀 Instrucciones de Ejecución

### 1. Levantar el entorno Docker
```bash
docker-compose up -d
```

### 2. Crear el topic en Kafka
```bash
python streaming/crear_topic_kafka.py
```

### 3. Iniciar Spark Structured Streaming (en una terminal)
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  streaming/spark_streaming.py
```

### 4. Ejecutar el productor Kafka (en otra terminal)
```bash
python streaming/kafka_producer.py
```

---

## 📸 Evidencias Esperadas

- Consola del producer mostrando los 2,000 eventos enviados
- Salida de Spark Streaming con los micro-batches procesados
- Alertas de retraso > 30 minutos apareciendo en tiempo real
- Resumen de pedidos por distrito actualizándose cada 10 segundos
