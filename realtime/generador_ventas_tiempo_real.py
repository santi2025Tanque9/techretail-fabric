"""
Generador sintético de transacciones POS en tiempo real — TechRetail Corp
Proyecto Final — Bootcamp Data Fabric (Código Facilito)

Envía lotes de 10 eventos de venta cada 2 segundos al Custom endpoint
del Eventstream ES_Ventas_TiempoReal (protocolo compatible con Event Hubs).

SEGURIDAD:
  Las credenciales NO se escriben en este archivo. Se leen desde variables
  de entorno, cargadas desde un archivo .env local (excluido de Git):
    - EVENTSTREAM_CONNECTION_STR  -> Connection string-primary key
    - EVENTSTREAM_NAME            -> Event hub name
  Ver .env.example para la plantilla de configuración.

USO (Git Bash, con el entorno virtual activo):
  python generador_ventas_tiempo_real.py
  (Ctrl+C para detener)
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv


# ---- Credenciales desde variables de entorno (.env) ----
load_dotenv()  # Carga las variables del archivo .env (si existe)

CONNECTION_STR = os.environ.get("EVENTSTREAM_CONNECTION_STR")
EVENTHUB_NAME = os.environ.get("EVENTSTREAM_NAME")

if not CONNECTION_STR or not EVENTHUB_NAME:
    raise SystemExit(
        "Faltan variables de entorno: EVENTSTREAM_CONNECTION_STR y/o EVENTSTREAM_NAME. "
        "Revisá el archivo .env (usá .env.example como plantilla)."
    )

# ---- Valores de referencia (mismos rangos que los CSV del proyecto) ----
TIENDAS = list(range(101, 130))                  # tiendas 101 a 129
EMPLEADOS = list(range(5001, 5146))              # 145 empleados
SKUS = [f"SKU-{n}" for n in range(1001, 1186)]   # 185 productos


def generar_evento():
    return {
        "evento_id": f"EVT-{random.randint(100000, 999999)}",
        "timestamp_evento": datetime.now(timezone.utc).isoformat(),
        "tienda_id": random.choice(TIENDAS),
        "empleado_id": random.choice(EMPLEADOS),
        "sku": random.choice(SKUS),
        "cantidad": random.randint(1, 5),
        "precio_unitario": round(random.uniform(10, 450), 2),
        # Mayoría de ventas normales, con una anomalía ocasional (90%)
        # para poder detectarla luego en KQL / Silver.
        "descuento_pct": random.choices(
            [0, 5, 10, 15, 20, 90],
            weights=[40, 20, 15, 10, 10, 1],
        )[0],
    }


def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME
    )
    with producer:
        while True:
            batch = producer.create_batch()
            for _ in range(10):
                batch.add(EventData(json.dumps(generar_evento())))
            producer.send_batch(batch)
            print(f"[{datetime.now():%H:%M:%S}] Lote de 10 eventos enviado")
            time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProductor detenido.")