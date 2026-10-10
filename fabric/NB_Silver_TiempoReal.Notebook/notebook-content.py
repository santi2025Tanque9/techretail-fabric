# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "10ef4bc0-d845-42c8-8f63-a6e556cd5073",
# META       "default_lakehouse_name": "LakehouseBronze",
# META       "default_lakehouse_workspace_id": "18538499-9f09-4819-8883-e320b733b198",
# META       "known_lakehouses": [
# META         {
# META           "id": "10ef4bc0-d845-42c8-8f63-a6e556cd5073"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "12d7ff8f-5010-4a2b-9af4-32d31c8aeb34",
# META       "known_warehouses": [
# META         {
# META           "id": "12d7ff8f-5010-4a2b-9af4-32d31c8aeb34",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_Silver_TiempoReal — Bronze (streaming) → Silver
# 
# **Proyecto Final — TechRetail Corp | Bootcamp Data Fabric**
# 
# ## ¿Qué hace este notebook?
# Toma los eventos de venta que llegan en tiempo real desde el **Eventstream `ES_Ventas_TiempoReal`** y que se guardan crudos en `bronze.ventas_tiempo_real`, y genera la tabla **`silver.ventas_tiempo_real`** según la sección 6 del mapping del enunciado.
# 
# ```
# generador_ventas_tiempo_real.py → Eventstream ─┬→ KQL Database (EH_TechRetail)      → consultas en vivo
#                                                └→ LakehouseBronze.bronze.ventas_tiempo_real
#                                                         │
#                                                         ▼  (este notebook)
#                                                LakehouseSilver.silver.ventas_tiempo_real
# ```
# 
# ## Reglas del mapping (sección 6)
# | Campo | Regla |
# |---|---|
# | `evento_id`, `tienda_id`, `empleado_id`, `sku`, `cantidad`, `precio_unitario`, `descuento_pct` | Sin transformación (solo tipado) |
# | `timestamp_evento` | Texto ISO 8601 → **Fecha/Hora** |
# | `venta_total` *(nuevo)* | `cantidad × precio_unitario × (1 − descuento_pct/100)` |
# | `es_anomalia` *(nuevo)* | **Verdadero si `descuento_pct ≥ 50`** |
# 
# > **Alcance:** según el enunciado, esta tabla es **complementaria de monitoreo**: no se integra al modelo estrella ni sube a Gold. El historial completo de ventas sigue en `gold.FactVentas` por el flujo batch.
# 
# **Lakehouses necesarios:** `LakehouseBronze` y `LakehouseSilver` agregados al notebook.


# MARKDOWN ********************

# ## 1. Configuración

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

BRONZE = "LakehouseBronze.bronze"
SILVER = "LakehouseSilver.silver"
TABLA  = "ventas_tiempo_real"

UMBRAL_ANOMALIA = 50   # descuento_pct >= 50  →  es_anomalia = True (regla del enunciado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Lectura de Bronze

# CELL ********************

df_bronze = spark.table(f"{BRONZE}.{TABLA}")

print(f"Filas en Bronze: {df_bronze.count():,}")
df_bronze.printSchema()
display(df_bronze.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Transformación Bronze → Silver
# Decisiones:
# - **Selección explícita de columnas**: solo las 8 del contrato del generador. Así, si el Eventstream agrega columnas de sistema, no "contaminan" Silver.
# - **`timestamp_evento`**: el generador lo envía como texto ISO 8601 en UTC (`2026-10-10T15:24:54.462420+00:00`); se convierte a `TIMESTAMP`.
# - **`EventEnqueuedUtcTime`** (si existe) se conserva como `_fecha_ingesta` para trazabilidad: permite medir la latencia entre que ocurre la venta y que llega a Fabric.
# - **Tipos de dinero en `DECIMAL`**, igual que en el resto de Silver (evita errores de redondeo de `double`).
# - **Duplicados**: solo se eliminan **duplicados exactos** (misma fila completa). No se deduplica por `evento_id` porque el generador lo crea con `random.randint(100000, 999999)`: con miles de eventos pueden repetirse IDs en ventas distintas (ver hallazgo en la sección 6).

# CELL ********************

columnas_contrato = [
    "evento_id", "timestamp_evento", "tienda_id", "empleado_id",
    "sku", "cantidad", "precio_unitario", "descuento_pct",
]

tiene_enqueued = "EventEnqueuedUtcTime" in df_bronze.columns

df_silver = (
    df_bronze
    .select(*columnas_contrato,
            *(["EventEnqueuedUtcTime"] if tiene_enqueued else []))
    .dropDuplicates()
    # --- tipado (sin transformación de negocio) ---
    .withColumn("evento_id",       F.trim(F.col("evento_id")))
    .withColumn("timestamp_evento", F.col("timestamp_evento").cast("timestamp"))
    .withColumn("tienda_id",       F.col("tienda_id").cast(IntegerType()))
    .withColumn("empleado_id",     F.col("empleado_id").cast(IntegerType()))
    .withColumn("sku",             F.trim(F.col("sku")))
    .withColumn("cantidad",        F.col("cantidad").cast(IntegerType()))
    .withColumn("precio_unitario", F.col("precio_unitario").cast(DecimalType(10, 2)))
    .withColumn("descuento_pct",   F.col("descuento_pct").cast(IntegerType()))
    # --- columnas calculadas (reglas del enunciado) ---
    .withColumn(
        "venta_total",
        F.round(
            F.col("cantidad") * F.col("precio_unitario") * (1 - F.col("descuento_pct") / 100),
            2,
        ).cast(DecimalType(12, 2)),
    )
    .withColumn("es_anomalia", F.col("descuento_pct") >= F.lit(UMBRAL_ANOMALIA))
)

if tiene_enqueued:
    df_silver = df_silver.withColumnRenamed("EventEnqueuedUtcTime", "_fecha_ingesta")

df_silver = df_silver.withColumn("_fecha_procesamiento", F.current_timestamp())

df_silver.printSchema()
display(df_silver.orderBy(F.col("timestamp_evento").desc()).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Validaciones
# Mismo criterio que el resto de Silver: si alguna regla falla, el notebook se detiene **antes** de escribir.

# CELL ********************

total_bronze = df_bronze.count()
total_silver = df_silver.count()

nulos_obligatorios = {
    c: df_silver.filter(F.col(c).isNull()).count()
    for c in ["evento_id", "timestamp_evento", "tienda_id", "empleado_id",
              "sku", "cantidad", "precio_unitario", "descuento_pct", "venta_total"]
}

# es_anomalia debe coincidir exactamente con la regla descuento_pct >= 50
inconsistencias_anomalia = df_silver.filter(
    F.col("es_anomalia") != (F.col("descuento_pct") >= UMBRAL_ANOMALIA)
).count()

ventas_negativas = df_silver.filter(F.col("venta_total") < 0).count()
descuentos_fuera_rango = df_silver.filter(~F.col("descuento_pct").between(0, 100)).count()

print(f"Filas Bronze: {total_bronze:,} | Filas Silver: {total_silver:,} "
      f"| Duplicados exactos quitados: {total_bronze - total_silver:,}")
print("Nulos en columnas obligatorias:", nulos_obligatorios)
print(f"Inconsistencias es_anomalia: {inconsistencias_anomalia}")
print(f"venta_total negativa: {ventas_negativas}")
print(f"descuento_pct fuera de 0-100: {descuentos_fuera_rango}")

assert total_silver > 0, "Silver quedó vacío: ¿el Eventstream está escribiendo en Bronze?"
assert all(v == 0 for v in nulos_obligatorios.values()), "Hay nulos en columnas obligatorias (¿timestamp mal parseado?)"
assert inconsistencias_anomalia == 0, "es_anomalia no respeta la regla descuento_pct >= 50"
assert ventas_negativas == 0, "Hay ventas con total negativo"
assert descuentos_fuera_rango == 0, "Hay descuentos fuera del rango 0-100"
print("Todas las validaciones OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Escritura en Silver
# Carga completa (`overwrite`): cada ejecución reconstruye la tabla desde Bronze. Es **idempotente**: correrlo dos veces da el mismo resultado.
# 
# > Mejora posible en producción: procesar de forma incremental con *Spark Structured Streaming* o leyendo solo eventos nuevos por `_fecha_ingesta`. Para el volumen del proyecto, la carga completa es suficiente y más simple de auditar.

# CELL ********************

(
    df_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable(f"{SILVER}.{TABLA}")
)

print(f"Escrito {SILVER}.{TABLA}: {spark.table(f'{SILVER}.{TABLA}').count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Resultado: detección de anomalías
# Esto es lo que responde al problem statement: *"Como gerente de tienda, no me entero de un descuento del 90% hasta el día siguiente"*.

# CELL ********************

df_out = spark.table(f"{SILVER}.{TABLA}")

print("Resumen general:")
display(
    df_out.agg(
        F.count("*").alias("eventos"),
        F.sum(F.col("es_anomalia").cast("int")).alias("anomalias"),
        F.round(F.avg(F.col("es_anomalia").cast("int")) * 100, 2).alias("pct_anomalias"),
        F.sum("venta_total").alias("venta_total"),
        F.min("timestamp_evento").alias("primer_evento"),
        F.max("timestamp_evento").alias("ultimo_evento"),
    )
)

print("Últimas anomalías detectadas (descuento >= 50%):")
display(
    df_out.filter("es_anomalia")
    .select("timestamp_evento", "evento_id", "tienda_id", "empleado_id",
            "sku", "cantidad", "precio_unitario", "descuento_pct", "venta_total")
    .orderBy(F.col("timestamp_evento").desc())
    .limit(20)
)

print("Anomalías por tienda:")
display(
    df_out.filter("es_anomalia")
    .groupBy("tienda_id").count()
    .orderBy(F.col("count").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Hallazgo: `evento_id` no es único
# El generador crea el ID con `random.randint(100000, 999999)` (900.000 valores posibles). Por la "paradoja del cumpleaños", con unos ~1.100 eventos ya es probable que dos ventas distintas compartan ID. Por eso **no se usa `evento_id` como clave para deduplicar**. La celda siguiente lo cuantifica para documentarlo en el informe.
# 
# Recomendación para producción: generar el ID con `uuid.uuid4()` en el productor.

# CELL ********************

ids_repetidos = (
    df_out.groupBy("evento_id").count()
    .filter("count > 1")
)
print(f"evento_id repetidos en ventas distintas: {ids_repetidos.count():,}")
display(ids_repetidos.orderBy(F.col("count").desc()).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
