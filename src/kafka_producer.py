"""
=====================================================
RAPÍDEX - KAFKA PRODUCER DE EVENTOS EN TIEMPO REAL
=====================================================
Autor: Grupo 11 - Módulo Streaming
Descripción: Genera y publica 2,000 eventos simulados
             de pedidos delivery en el topic de Kafka
             'rapidex-eventos'.

Tipos de eventos:
  - PEDIDO_REALIZADO
  - PEDIDO_RETRASADO
  - PEDIDO_CANCELADO
  - PEDIDO_ENTREGADO

Ejecutar: python kafka_producer.py
=====================================================
"""

from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME   = 'rapidex-eventos'
TOTAL_EVENTOS = 2000
DELAY_ENTRE_EVENTOS = 0.1  # segundos entre cada evento

# ─────────────────────────────────────────────
# DATOS SIMULADOS
# ─────────────────────────────────────────────
TIPOS_EVENTO = [
    'PEDIDO_REALIZADO',
    'PEDIDO_RETRASADO',
    'PEDIDO_CANCELADO',
    'PEDIDO_ENTREGADO'
]

DISTRITOS = [
    'Miraflores', 'San Isidro', 'Surco', 'La Molina',
    'Barranco', 'Callao', 'San Borja', 'Lince',
    'Magdalena', 'Pueblo Libre'
]

REPARTIDORES = [f'REP-{i:03d}' for i in range(1, 51)]   # 50 repartidores

MOTIVOS_CANCELACION = [
    'Cliente no disponible',
    'Dirección incorrecta',
    'Pedido duplicado',
    'Tiempo de espera excesivo',
    'Problema con el pago'
]

MOTIVOS_RETRASO = [
    'Tráfico intenso',
    'Lluvia',
    'Accidente vial',
    'Error de ruta',
    'Demora en restaurante'
]

# ─────────────────────────────────────────────
# GENERADOR DE EVENTOS
# ─────────────────────────────────────────────
def generar_evento(tipo: str) -> dict:
    """
    Genera un evento JSON con estructura variable
    según el tipo de evento.
    """
    base = {
        'id_evento'   : str(uuid.uuid4()),
        'tipo'        : tipo,
        'id_pedido'   : f'PED-{random.randint(100000, 999999)}',
        'id_cliente'  : f'CLI-{random.randint(1, 5000):05d}',
        'id_repartidor': random.choice(REPARTIDORES),
        'distrito'    : random.choice(DISTRITOS),
        'timestamp'   : datetime.now().isoformat()
    }

    if tipo == 'PEDIDO_REALIZADO':
        base.update({
            'monto_total'     : round(random.uniform(15.0, 180.0), 2),
            'tiempo_estimado' : random.randint(20, 60),   # minutos
            'num_items'       : random.randint(1, 8)
        })

    elif tipo == 'PEDIDO_ENTREGADO':
        base.update({
            'monto_total'     : round(random.uniform(15.0, 180.0), 2),
            'tiempo_real'     : random.randint(18, 75),   # minutos reales
            'calificacion'    : round(random.uniform(1.0, 5.0), 1)
        })

    elif tipo == 'PEDIDO_RETRASADO':
        base.update({
            'minutos_retraso' : random.randint(5, 45),
            'motivo_retraso'  : random.choice(MOTIVOS_RETRASO)
        })

    elif tipo == 'PEDIDO_CANCELADO':
        base.update({
            'monto_total'         : round(random.uniform(15.0, 180.0), 2),
            'motivo_cancelacion'  : random.choice(MOTIVOS_CANCELACION)
        })

    return base


# ─────────────────────────────────────────────
# MAIN — PUBLICAR EVENTOS EN KAFKA
# ─────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  RAPÍDEX — KAFKA PRODUCER INICIADO")
    print("=" * 55)
    print(f"  Broker  : {KAFKA_BROKER}")
    print(f"  Topic   : {TOPIC_NAME}")
    print(f"  Eventos : {TOTAL_EVENTOS}")
    print("=" * 55)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=3
    )

    contadores = {tipo: 0 for tipo in TIPOS_EVENTO}

    for i in range(1, TOTAL_EVENTOS + 1):
        # Distribución realista de eventos
        tipo = random.choices(
            TIPOS_EVENTO,
            weights=[40, 15, 10, 35],   # % aproximado por tipo
            k=1
        )[0]

        evento = generar_evento(tipo)
        producer.send(TOPIC_NAME, value=evento)
        contadores[tipo] += 1

        if i % 100 == 0:
            print(f"  [{i:4d}/{TOTAL_EVENTOS}] Enviados → "
                  f"REALIZADOS: {contadores['PEDIDO_REALIZADO']} | "
                  f"ENTREGADOS: {contadores['PEDIDO_ENTREGADO']} | "
                  f"RETRASADOS: {contadores['PEDIDO_RETRASADO']} | "
                  f"CANCELADOS: {contadores['PEDIDO_CANCELADO']}")

        time.sleep(DELAY_ENTRE_EVENTOS)

    producer.flush()
    producer.close()

    print("\n" + "=" * 55)
    print("  PRODUCER FINALIZADO — RESUMEN")
    print("=" * 55)
    for tipo, total in contadores.items():
        print(f"  {tipo:<25} : {total:>4} eventos ({total/TOTAL_EVENTOS*100:.1f}%)")
    print(f"  {'TOTAL':<25} : {TOTAL_EVENTOS:>4} eventos")
    print("=" * 55)


if __name__ == '__main__':
    main()
