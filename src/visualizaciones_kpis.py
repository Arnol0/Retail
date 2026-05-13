import pandas as pd
import matplotlib.pyplot as plt
import os

# Ruta absoluta de la carpeta actual
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Crear carpeta visualizaciones dentro del proyecto
ruta_visualizaciones = os.path.join(BASE_DIR, "..", "visualizaciones")

os.makedirs(ruta_visualizaciones, exist_ok=True)

# =========================
# GRAFICO 1
# =========================

productos = {
    'Producto': [
        'Zapatillas Running XR',
        'Casaca Urban',
        'Polo DryFit',
        'Mochila Sport',
        'Short Deportivo',
        'Gorra Classic',
        'Medias Pro',
        'Buzo Training',
        'Sandalias Air',
        'Casaca Denim'
    ],
    'Unidades': [487, 450, 430, 390, 370, 340, 320, 300, 280, 260]
}

df_productos = pd.DataFrame(productos)

plt.figure(figsize=(10,6))
plt.barh(df_productos['Producto'], df_productos['Unidades'])

plt.title('Top 10 Productos Más Vendidos')

plt.savefig(os.path.join(ruta_visualizaciones, 'top_productos.png'))

plt.close()

# =========================
# GRAFICO 2
# =========================

segmentos = ['Premium', 'Regular', 'Nuevo']
ingresos = [34, 52, 14]

plt.figure(figsize=(7,7))

plt.pie(
    ingresos,
    labels=segmentos,
    autopct='%1.1f%%'
)

plt.title('Distribución de Ingresos por Segmento')

plt.savefig(os.path.join(ruta_visualizaciones, 'segmentos_clientes.png'))

plt.close()

# =========================
# GRAFICO 3
# =========================

tipos = [
    'VENTA_REALIZADA',
    'DEVOLUCION',
    'ALERTA_STOCK',
    'PAGO_RECHAZADO'
]

cantidades = [1240, 360, 240, 160]

plt.figure(figsize=(8,6))

plt.bar(tipos, cantidades)

plt.title('Eventos Streaming Procesados')

plt.savefig(os.path.join(ruta_visualizaciones, 'eventos_streaming.png'))

plt.close()

print("Gráficos generados correctamente")
print("Ruta:", ruta_visualizaciones)
# Visualizaciones KPI Retail AA4