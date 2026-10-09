# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d4a9a618-60bd-4c95-89d4-f349d74d2197",
# META       "default_lakehouse_name": "LakehouseSilver",
# META       "default_lakehouse_workspace_id": "18538499-9f09-4819-8883-e320b733b198",
# META       "known_lakehouses": [
# META         {
# META           "id": "d4a9a618-60bd-4c95-89d4-f349d74d2197"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_Gold_Hechos
# 
# ## Objetivo
# 
# Construir las tablas de hechos y las tablas agregadas del modelo estrella en `WarehouseGold.gold`.
# 
# | Tabla Gold | Origen | Granularidad (una fila por…) |
# |---|---|---|
# | `FactVentas` | `silver.ventas_enriquecidas` + dimensiones Gold | línea de venta |
# | `real_vs_presupuesto` | `gold.FactVentas` + `silver.presupuesto` | tienda y mes |
# | `ventas_diarias_por_tienda` | `gold.FactVentas` | tienda y día |
# | `FactInventario` | `silver.inventario` + dimensiones Gold | tienda, categoría y mes |


# MARKDOWN ********************

# ## 1. Configuración

# CELL ********************

import com.microsoft.spark.fabric
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

SILVER = "LakehouseSilver.silver"
GOLD   = "WarehouseGold.gold"

print(f"Versión de Spark: {spark.version}")

def leer_silver(tabla):
    return spark.table(f"{SILVER}.{tabla}").drop("_fecha_procesamiento")

def leer_gold(tabla):
    return spark.read.synapsesql(f"{GOLD}.{tabla}")

def escribir_gold(df, tabla):
    df.write.mode("overwrite").synapsesql(f"{GOLD}.{tabla}")
    print(f"✔ {GOLD}.{tabla} escrita")

def fecha_a_key(col):
    """DATE -> entero YYYYMMDD (formato de fecha_key en DimFecha)."""
    return F.date_format(col, "yyyyMMdd").cast("int")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # A. `FactVentas`
# 
# Cada venta de Silver se conecta con las dimensiones reemplazando su **ID natural** por la **clave surrogada** correspondiente:
# 
# ```
# silver.ventas_enriquecidas            gold.FactVentas
# ──────────────────────────            ───────────────
# fecha_venta  2025-03-15        ──►    fecha_key     20250315
# tienda_id    109               ──►    tienda_key    9          (DimTienda)
# sku          SKU-1108          ──►    producto_key  108        (DimProducto)
# empleado_id  5040              ──►    empleado_key  40         (DimEmpleado)
# cliente_id   (vacío)           ──►    cliente_key   -1         (Cliente No Identificado)
# ```
# 
# **Reglas del mapping (paso 8 de la consigna):**
# - Todos los cruces son **Left Outer Join**: ninguna venta se descarta por no encontrar su dimensión
# - Venta sin `cliente_id` → `cliente_key = -1`
# - Si tienda, producto o empleado no tienen coincidencia → `_key` en nulo y la fila se **marca para revisión**
# - `costo_total = cantidad × costo_unitario`, con el costo traído de `DimProducto` durante el cruce
# - Se guardan solo campos numéricos y claves: ningún campo descriptivo (ciudad, categoría, nombre…) vive en la tabla de hechos
# 
# **Decisiones de diseño propias:**
# 
# | Decisión | Motivo |
# |---|---|
# | Columna `requiere_revision` (BOOLEAN) | La consigna pide "marcar la fila para revisión" pero no define dónde. Esta columna lo hace explícito y filtrable |
# | `cliente_id` informado pero inexistente en `DimCliente` → `cliente_key = -1` + `requiere_revision = true` | La consigna no contempla este caso. Así la venta no queda con la relación vacía y además se marca para revisar |
# | Los cruces se hacen en memoria, sin tablas intermedias | Lo exige la consigna: solo se persiste el resultado final |
# 
# > Con los datos actuales todas las ventas encuentran su tienda, producto, empleado y cliente (validado en `NB_Silver_Hechos`), por lo que `requiere_revision` debería ser `false` en todas las filas. La lógica queda preparada para cargas futuras.


# MARKDOWN ********************

# ## A.1 Lectura de ventas y dimensiones
# 
# Las ventas se leen de Silver (Lakehouse) y las dimensiones del Warehouse Gold, porque las claves surrogadas se generaron allí. De cada dimensión solo se traen las columnas necesarias para el cruce.

# CELL ********************

ventas = leer_silver("ventas_enriquecidas")

dim_tienda   = leer_gold("DimTienda").select("tienda_id", "tienda_key")
dim_producto = leer_gold("DimProducto").select("sku", "producto_key", "costo_unitario")
dim_empleado = leer_gold("DimEmpleado").select("empleado_id", "empleado_key")
dim_cliente  = leer_gold("DimCliente").select("cliente_id", "cliente_key")

filas_silver = ventas.count()
print(f"Ventas en Silver: {filas_silver}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.2 Cruces con las dimensiones
# 
# **Truco para el cliente -1:** antes del cruce, las ventas sin cliente reciben `cliente_id = -1` (con `coalesce`). Como `DimCliente` tiene una fila con `cliente_id = -1`, el cruce les asigna automáticamente `cliente_key = -1`, sin lógica adicional.

# CELL ********************

fact = (
    ventas
    .withColumn("cliente_id_cruce", F.coalesce(F.col("cliente_id"), F.lit(-1)))
    .join(dim_tienda,   "tienda_id",   "left")
    .join(dim_producto, "sku",         "left")
    .join(dim_empleado, "empleado_id", "left")
    .join(dim_cliente.withColumnRenamed("cliente_id", "cliente_id_cruce"), "cliente_id_cruce", "left")
)

# Marca para revisión: alguna dimensión sin coincidencia
fact = fact.withColumn(
    "requiere_revision",
    F.col("tienda_key").isNull() | F.col("producto_key").isNull()
    | F.col("empleado_key").isNull() | F.col("cliente_key").isNull()
)

# Cliente informado pero inexistente: se asigna -1 (ya quedó marcado arriba)
fact = fact.withColumn("cliente_key", F.coalesce(F.col("cliente_key"), F.lit(-1)))

print(f"Filas después de los cruces: {fact.count()} (deben ser {filas_silver})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.3 Cálculos y selección de columnas finales
# 
# | Campo | Tipo | Origen |
# |---|---|---|
# | `transaccion_id` | STRING | Atributo degenerado: identifica la venta, no tiene dimensión propia |
# | `fecha_key` | INT | `fecha_venta` → `YYYYMMDD` |
# | `tienda_key`, `producto_key`, `empleado_key`, `cliente_key` | INT | Cruces con las dimensiones |
# | `cantidad` | INT | Silver |
# | `precio_unitario` | DECIMAL(10,2) | Silver |
# | `descuento_pct` | INT | Silver |
# | `venta_total` | DECIMAL(12,2) | Silver (calculado allí con la misma fórmula del mapping) |
# | `costo_total` | DECIMAL(12,2) | **Nuevo:** `cantidad × costo_unitario` |
# | `requiere_revision` | BOOLEAN | Decisión de diseño |
# 
# > **Sobre `hora_venta`:** el esquema de `FactVentas` definido en la consigna no incluye la hora, por lo que no se agrega. La hora queda disponible en `silver.ventas_enriquecidas` (`hora_venta` y `fecha_hora_venta`) si en el futuro se requiere un análisis por franja horaria.

# CELL ********************

fact_ventas = fact.select(
    "transaccion_id",
    fecha_a_key(F.col("fecha_venta")).alias("fecha_key"),
    "tienda_key",
    "producto_key",
    "empleado_key",
    "cliente_key",
    "cantidad",
    "precio_unitario",
    "descuento_pct",
    "venta_total",
    (F.col("cantidad") * F.col("costo_unitario")).cast(DecimalType(12, 2)).alias("costo_total"),
    "requiere_revision",
)

fact_ventas.printSchema()
display(fact_ventas.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.4 Controles de calidad
# 
# | Control | Qué verifica |
# |---|---|
# | **Reconciliación de filas** | Misma cantidad de filas que Silver: los cruces no perdieron ni duplicaron ventas |
# | **Reconciliación de importes** | La suma de `venta_total` coincide con la de Silver |
# | **Unicidad** | `transaccion_id` único |
# | **Fecha** | Ninguna venta sin `fecha_key` |
# | **Filas a revisar** | Cuántas quedaron marcadas (informativo) |

# CELL ********************

filas_fact = fact_ventas.count()
assert filas_fact == filas_silver, f"Los cruces cambiaron la cantidad de filas: {filas_silver} -> {filas_fact}"
print(f"✔ Reconciliación de filas: {filas_fact}")

suma_silver = ventas.agg(F.sum("venta_total")).first()[0]
suma_fact   = fact_ventas.agg(F.sum("venta_total")).first()[0]
assert suma_silver == suma_fact, f"Las sumas no coinciden: {suma_silver} vs {suma_fact}"
print(f"✔ Reconciliación de importes: {suma_fact:,}")

assert fact_ventas.select("transaccion_id").distinct().count() == filas_fact, "transaccion_id duplicado"
print("✔ transaccion_id único")

assert fact_ventas.filter(F.col("fecha_key").isNull()).count() == 0, "Hay ventas sin fecha_key"
print("✔ Todas las ventas tienen fecha_key")

a_revisar = fact_ventas.filter("requiere_revision").count()
sin_cliente = fact_ventas.filter(F.col("cliente_key") == -1).count()
print(f"Filas marcadas para revisión: {a_revisar}")
print(f"Ventas de 'Cliente No Identificado' (-1): {sin_cliente}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

escribir_gold(fact_ventas, "FactVentas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # B. `real_vs_presupuesto`
# 
# Compara lo vendido contra la meta, por tienda y mes.
# 
# ## Corrección de granularidad respecto de la consigna
# 
# El paso 9 de la consigna indica *"agregar FactVentas por `tienda_key` y `fecha_key`"* y luego cruzar con el presupuesto. Pero **el presupuesto es mensual** y `fecha_key` es **diaria**: si se cruzan así, cada día queda comparado contra la meta de todo el mes y la variación sale muy negativa en todas las filas, aunque la tienda haya superado su meta.
# 
# Ejemplo real con la tienda 101 en enero de 2025 (meta mensual 13.360,38; vendió en 26 días distintos):
# 
# | Enfoque | venta_total_real | meta_venta | variacion_pct | Conclusión |
# |---|---|---|---|---|
# | ❌ Por día (literal de la consigna) | ~1.239 (promedio de un día) | 13.360,38 | ~ -91% | "No cumplió" (falso) |
# | ✅ Por mes (aplicado) | 32.223,08 (todo el mes) | 13.360,38 | +141,18% | Superó la meta (correcto) |
# 
# Por eso las ventas se agregan **por tienda y mes** antes del cruce. Se respetan los campos finales que pide la consigna.
# 
# | Campo | Tipo | Origen |
# |---|---|---|
# | `tienda_id` | INT | Recuperado desde `DimTienda` |
# | `anio_mes` | STRING `YYYY/MM` | Recuperado desde `DimFecha`, formato pedido por la consigna |
# | `venta_total_real` | DECIMAL(14,2) | Suma de `venta_total` del mes |
# | `meta_venta` | DECIMAL(12,2) | `silver.presupuesto` |
# | `variacion_pct` | DECIMAL(10,2) | `(venta_total_real − meta_venta) / meta_venta × 100` |
# 
# **Cruce con el presupuesto:** en Silver, `anio_mes` es DATE (primer día del mes). Para cruzarlo con el texto `YYYY/MM` de `DimFecha` se formatea con `date_format(anio_mes, 'yyyy/MM')`. Se usa **Left Outer Join** desde las ventas, como indica la consigna.


# CELL ********************

fact_gold  = leer_gold("FactVentas")
dim_fecha  = leer_gold("DimFecha").select("fecha_key", "anio_mes")
dim_tienda_full = leer_gold("DimTienda").select("tienda_key", "tienda_id")

ventas_mes = (
    fact_gold
    .join(dim_fecha, "fecha_key")
    .join(dim_tienda_full, "tienda_key")
    .groupBy("tienda_id", "anio_mes")
    .agg(F.sum("venta_total").cast(DecimalType(14, 2)).alias("venta_total_real"))
)

presupuesto = leer_silver("presupuesto").select(
    "tienda_id",
    F.date_format("anio_mes", "yyyy/MM").alias("anio_mes"),
    "meta_venta",
)

real_vs_presupuesto = (
    ventas_mes
    .join(presupuesto, ["tienda_id", "anio_mes"], "left")
    .withColumn(
        "variacion_pct",
        F.round((F.col("venta_total_real") - F.col("meta_venta")) / F.col("meta_venta") * 100, 2)
         .cast(DecimalType(10, 2)),
    )
    .select("tienda_id", "anio_mes", "venta_total_real", "meta_venta", "variacion_pct")
    .orderBy("tienda_id", "anio_mes")
)

display(real_vs_presupuesto.limit(12))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## B.1 Controles y escritura
# 
# - **Reconciliación:** la suma de `venta_total_real` es igual a la suma de `FactVentas`
# - **Completitud:** 348 filas (29 tiendas × 12 meses) y ninguna sin meta
# - **Resumen de negocio:** cuántos meses cumplieron la meta y cuáles fueron el mejor y el peor desempeño

# CELL ********************

filas_rvp = real_vs_presupuesto.count()
suma_rvp  = real_vs_presupuesto.agg(F.sum("venta_total_real")).first()[0]
suma_fact_gold = fact_gold.agg(F.sum("venta_total")).first()[0]

assert suma_rvp == suma_fact_gold, f"La suma mensual no coincide con FactVentas: {suma_rvp} vs {suma_fact_gold}"
print(f"✔ Reconciliación: {suma_rvp:,}")

sin_meta = real_vs_presupuesto.filter(F.col("meta_venta").isNull()).count()
print(f"Filas: {filas_rvp} | sin meta asignada: {sin_meta}")

real_vs_presupuesto.agg(
    F.sum(F.when(F.col("variacion_pct") >= 0, 1).otherwise(0)).alias("meses_que_cumplen"),
    F.sum(F.when(F.col("variacion_pct") < 0, 1).otherwise(0)).alias("meses_que_no_cumplen"),
    F.min("variacion_pct").alias("peor_variacion"),
    F.max("variacion_pct").alias("mejor_variacion"),
).show()

escribir_gold(real_vs_presupuesto, "real_vs_presupuesto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # C. `ventas_diarias_por_tienda`
# 
# Tabla agregada pensada para el reporte de Power BI: totales por tienda y día, para graficar evolución diaria sin recorrer las 17.503 líneas de venta.
# 
# | Campo | Tipo | Descripción |
# |---|---|---|
# | `tienda_key` | INT | Clave hacia `DimTienda` |
# | `fecha_key` | INT | Clave hacia `DimFecha` |
# | `venta_total` | DECIMAL(14,2) | Suma del día |
# | `unidades` | INT | Suma de `cantidad` |
# | `cantidad_transacciones` | INT | Cantidad de ventas del día (agregado propio: permite calcular el ticket promedio) |

# CELL ********************

ventas_diarias = (
    fact_gold
    .groupBy("tienda_key", "fecha_key")
    .agg(
        F.sum("venta_total").cast(DecimalType(14, 2)).alias("venta_total"),
        F.sum("cantidad").cast("int").alias("unidades"),
        F.count("*").cast("int").alias("cantidad_transacciones"),
    )
)

assert ventas_diarias.agg(F.sum("venta_total")).first()[0] == suma_fact_gold, "La suma diaria no coincide con FactVentas"
print(f"✔ Reconciliación con FactVentas | filas: {ventas_diarias.count()}")

escribir_gold(ventas_diarias, "ventas_diarias_por_tienda")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # D. `FactInventario`
# 
# Tabla de hechos del stock mensual. **No está definida en la consigna**, pero es necesaria para que el Data Agent pueda responder sobre inventario, como pide el objetivo del proyecto.
# 
# | Campo | Tipo | Descripción |
# |---|---|---|
# | `tienda_key` | INT | Clave hacia `DimTienda` |
# | `fecha_key` | INT | Primer día del mes (`20250301`), clave hacia `DimFecha` |
# | `categoria` | STRING | Atributo propio: el inventario se informa por categoría, no por producto, así que no puede relacionarse con `DimProducto` |
# | `cantidad_disponible` | INT | Stock al cierre del mes |
# | `sin_stock` | BOOLEAN | `true` si hubo quiebre de stock |
# 


# CELL ********************

inventario = leer_silver("inventario")

fact_inventario = (
    inventario
    .join(dim_tienda, "tienda_id", "left")
    .select(
        "tienda_key",
        fecha_a_key(F.col("anio_mes")).alias("fecha_key"),
        "categoria",
        "cantidad_disponible",
        "sin_stock",
    )
)

assert fact_inventario.count() == inventario.count(), "El cruce cambió la cantidad de filas"
assert fact_inventario.filter(F.col("tienda_key").isNull()).count() == 0, "Hay inventario sin tienda_key"
print(f"✔ FactInventario: {fact_inventario.count()} filas, todas con tienda_key")

escribir_gold(fact_inventario, "FactInventario")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Verificación final
# 
# | Tabla | Filas esperadas | Control adicional |
# |---|---|---|
# | `FactVentas` | 17.503 | Suma `venta_total` = 10.743.932,79 · `costo_total` = 6.722.008,93 · 6.882 ventas con `cliente_key = -1` |
# | `real_vs_presupuesto` | 348 | 216 meses cumplen la meta y 132 no |
# | `ventas_diarias_por_tienda` | 8.553 | |
# | `FactInventario` | 1.728 | |

# CELL ********************

esperado = {"FactVentas": 17503, "real_vs_presupuesto": 348, "ventas_diarias_por_tienda": 8553, "FactInventario": 1728}

for tabla, filas_esperadas in esperado.items():
    filas = leer_gold(tabla).count()
    estado = "OK" if filas == filas_esperadas else "REVISAR"
    print(f"{tabla:26}: {filas:6} filas (esperadas {filas_esperadas}) -> {estado}")

leer_gold("FactVentas").agg(
    F.sum("venta_total").alias("venta_total"),
    F.sum("costo_total").alias("costo_total"),
    (F.sum("venta_total") - F.sum("costo_total")).alias("margen_bruto"),
    F.sum(F.when(F.col("cliente_key") == -1, 1).otherwise(0)).alias("ventas_cliente_no_identificado"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Resultado
# 
# El modelo estrella de Gold queda completo:
# 
# ```
#                        DimFecha
#                           │
#      DimTienda ──── FactVentas ──── DimProducto
#          │            │      │
#          │     DimEmpleado  DimCliente
#          │
#    FactInventario ── DimFecha
# 
#    + real_vs_presupuesto · ventas_diarias_por_tienda (agregadas)
# ```
# 

