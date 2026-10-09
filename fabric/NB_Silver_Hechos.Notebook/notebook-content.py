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
# META         },
# META         {
# META           "id": "10ef4bc0-d845-42c8-8f63-a6e556cd5073"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Objetivo
# 
# Transformar las tablas transaccionales y de metas de `LakehouseBronze.bronze` en tablas limpias y tipadas en `LakehouseSilver.silver`.
# 
# | Sección | Origen (Bronze) | Destino (Silver) | 
# |---|---|---|
# | A | `bronze.ventas_transacciones` | `silver.ventas_enriquecidas` + `silver.rechazos_ventas` |
# | B | `bronze.presupuesto_metas` | `silver.presupuesto` | 
# | C | `bronze.inventario_snapshot` | `silver.inventario` | 
# 
# **Lakehouses:** `LakehouseSilver` (predeterminado) · `LakehouseBronze` (adicional)
# 
# ## 1. Configuración y funciones auxiliares
# 
# | Función | Responsabilidad |
# |---|---|
# | `leer_bronze()` | Lee una tabla de Bronze sin las columnas de auditoría de ingesta |
# | `vacio_a_nulo()` | Convierte cadenas vacías o con solo espacios en `NULL`. Es necesario porque un campo vacío del CSV puede llegar a Bronze como `""` en lugar de `NULL`, y castear `""` a número da `NULL` o error según la configuración de Spark |
# | `validar()` | Unicidad de la clave y ausencia de nulos en columnas obligatorias. Detiene la ejecución si algo falla |
# | `escribir_silver()` | Agrega `_fecha_procesamiento` y sobrescribe la tabla en formato Delta |


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE = "LakehouseBronze.bronze"
SILVER = "LakehouseSilver.silver"

def leer_bronze(tabla): 
    """Lee una tabla de Bronze descartando las columnas de auditoría de ingesta."""
    return spark.table(f"{BRONZE}.{tabla}").drop("_archivo_origen", "_fecha_ingesta")

def vacio_a_nulo(col):
    """Devuelve NULL si el valor es NULL, vacío o solo espacios; si no, el valor sin espacios."""
    c = F.trim(F.col(col))
    return F.when(c.isNull() | (c == ""), F.lit(None)).otherwise(c)

def validar(df, nombre, pk, columnas_obligatorias):
    """Controles de calidad mínimos antes de escribir. Falla si algo está mal."""
    total = df.count()
    duplicados = total - df.select(pk).distinct().count()
    nulos = {c: df.filter(F.col(c).isNull()).count() for c in columnas_obligatorias}
    print(f"[{nombre}] filas={total} | duplicados en {pk}={duplicados} | nulos={nulos}")
    assert duplicados == 0, f"{nombre}: hay {duplicados} claves duplicadas en {pk}"
    assert all(v == 0 for v in nulos.values()), f"{nombre}: hay nulos en columnas obligatorias {nulos}"

def escribir_silver(df, tabla):
    """Agrega columna de auditoría y sobrescribe la tabla Silver."""
    (df.withColumn("_fecha_procesamiento", F.current_timestamp())
       .write.mode("overwrite")
       .option("overwriteSchema", "true")
       .format("delta")
       .saveAsTable(f"{SILVER}.{tabla}"))
    print(f"✔ {SILVER}.{tabla} escrita")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # A. Ventas → `ventas_enriquecidas` + `rechazos_ventas`
# 
# Es el archivo con más reglas de calidad. Se procesa con una **arquitectura de dos salidas**: las filas válidas van a `ventas_enriquecidas` y las filas sin `sku` se separan a `rechazos_ventas` para revisión, **sin descartarlas silenciosamente**.
# 
# ```
# bronze.ventas_transacciones (18.180)
#         │
#         ├─ 1. Eliminar duplicados exactos ──► −180 filas (reintentos de integración)
#         │
#         ▼  18.000 filas únicas
#         │
#         ├─ 2. ¿Tiene sku? ── No ──► silver.rechazos_ventas   (497)
#         │
#         └────────────────── Sí ──► 3. Limpieza y tipificación
#                                        ▼
#                                    silver.ventas_enriquecidas (17.503)
# ```
# 
# **Hallazgos del perfilado y reglas aplicadas:**
# 
# | Campo | Hallazgo en el origen | Regla |
# |---|---|---|
# | (fila completa) | 180 filas repetidas exactas por reintento de integración (~1%) | Eliminar duplicados exactos |
# | `sku` | 497 ventas sin sku (sobre filas únicas) | Separar a `rechazos_ventas` |
# | `fecha` | Texto `YYYY/MM/DD` | → `fecha_venta` DATE |
# | `hora` | Texto `HH:MM:SS` | → `hora_venta` validada + `fecha_hora_venta` TIMESTAMP |
# | `cliente_id` | ~39% vacío (venta sin programa de lealtad, **dato de negocio válido**) | Mantener `NULL` (en Gold se asigna la llave `-1`) |
# | `precio_unitario` | ~50% con coma decimal (`251,55`) | Reemplazar `,` por `.` → DECIMAL(10,2) |
# | `descuento_pct` | ~8% vacío | `NULL` → 0 |
# | `medio_pago` | 8 variantes (`TARJETA`, `tarjeta`, `billetera digital`…) | Trim + Formato Título → 3 valores |
# | — | No viene en el origen | Calcular `venta_total = cantidad × precio_unitario × (1 − descuento_pct/100)` |


# MARKDOWN ********************

# ## A.1 Lectura del dato crudo

# CELL ********************

df_ventas_raw = leer_bronze("ventas_transacciones")

filas_bronze = df_ventas_raw.count()
print(f"Filas en Bronze: {filas_bronze}")
df_ventas_raw.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.2 Eliminación de duplicados exactos
# 
# **¿Qué es un duplicado exacto?** Una fila que aparece dos veces **idéntica en todas sus columnas**: mismo `transaccion_id`, misma fecha, hora, tienda, producto, cantidad, precio, etc.
# 
# **¿Por qué existen?** Por *reintentos de integración*: el sistema del POS envía la venta, no recibe confirmación (por ejemplo, por un corte de red) y la vuelve a enviar. La venta ocurrió **una sola vez**, pero quedó registrada dos veces en el export.
# 
# **¿Por qué hay que eliminarlos?** Si se conservan, la venta se cuenta dos veces y **las métricas quedan infladas**: más ingresos, más unidades y más transacciones de las reales.
# 
# **Criterio:** se eliminan solo las filas idénticas en **todas** las columnas (`dropDuplicates()` sin argumentos). Después se verifica que `transaccion_id` quedó único: si apareciera un mismo `transaccion_id` con datos **distintos**, no sería un reintento sino un conflicto que requiere análisis, y el proceso se detiene.
# 
# Primero se muestran algunos ejemplos de duplicados para verlos con datos reales:


# CELL ********************

df_ejemplos_dup = (
    df_ventas_raw
    .groupBy(df_ventas_raw.columns)
    .count()
    .filter(F.col("count") > 1)
    .withColumnRenamed("count", "veces_repetida")
)

print(f"Filas que aparecen repetidas: {df_ejemplos_dup.count()}")
display(df_ejemplos_dup.orderBy("transaccion_id").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_unicas = df_ventas_raw.dropDuplicates()

filas_unicas = df_ventas_unicas.count()
duplicados_eliminados = filas_bronze - filas_unicas
print(f"Filas originales:        {filas_bronze}")
print(f"Duplicados eliminados:   {duplicados_eliminados}")
print(f"Filas únicas:            {filas_unicas}")

# Control: después de quitar duplicados exactos, transaccion_id debe ser único
ids_distintos = df_ventas_unicas.select("transaccion_id").distinct().count()
assert ids_distintos == filas_unicas, (
    f"Hay {filas_unicas - ids_distintos} transaccion_id con datos distintos: no son reintentos, revisar"
)
print("✔ transaccion_id es único después de eliminar duplicados")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.3 Separación de filas válidas y rechazos
# 
# Las ventas **sin `sku`** no pueden asociarse a ningún producto del catálogo, así que no se puede calcular su costo ni su margen. La consigna indica **no completarlas con supuestos**.
# 
# Se separan a `rechazos_ventas` conservando los **valores originales** (sin transformar) más un `motivo_rechazo`, para que el área responsable pueda revisarlas y corregirlas en el origen.
# 
# > **Orden importante:** los duplicados se eliminan **antes** de separar. Si no, una venta rechazada que estaba duplicada aparecería dos veces en `rechazos_ventas`. De hecho, 4 de los 180 duplicados son ventas sin sku.

# CELL ********************

sin_sku = vacio_a_nulo("sku").isNull()

df_rechazos_raw = df_ventas_unicas.filter(sin_sku)
df_validas_raw  = df_ventas_unicas.filter(~sin_sku)

filas_rechazadas = df_rechazos_raw.count()
filas_validas    = df_validas_raw.count()
print(f"Filas válidas:    {filas_validas}")
print(f"Filas rechazadas: {filas_rechazadas}  ({filas_rechazadas / filas_unicas:.1%})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.4 Limpieza y tipificación de las filas válidas
# 
# | Campo destino | Transformación |
# |---|---|
# | `fecha_venta` | `to_date(fecha, 'yyyy/MM/dd')` |
# | `hora_venta` | Texto `HH:mm:ss` validado (ver nota) |
# | `fecha_hora_venta` | `fecha + hora` → TIMESTAMP |
# | `tienda_id`, `empleado_id`, `cantidad` | Cast a INT |
# | `cliente_id` | Vacío → `NULL`; si no, INT |
# | `precio_unitario` | `,` → `.` y cast a DECIMAL(10,2) |
# | `descuento_pct` | Vacío → 0; cast a INT |
# | `medio_pago` | `initcap(trim(...))` |
# | `venta_total` | `cantidad × precio_unitario × (1 − descuento_pct/100)`, redondeado a 2 decimales |
# 
# > **Nota sobre `hora_venta`:** Spark (y por lo tanto Delta en el Lakehouse) no tiene un tipo `TIME` nativo. La hora se conserva como texto validado `HH:mm:ss` y además se crea `fecha_hora_venta` como TIMESTAMP, que permite análisis por hora del día. En el Warehouse Gold, `hora_venta` puede convertirse al tipo `TIME` de T-SQL.
# 
# > **Nota sobre `venta_total`:** se calcula con aritmética DECIMAL exacta, no con `double`, para que la suma de ventas no acumule errores de redondeo.


# MARKDOWN ********************


# CELL ********************

precio = F.regexp_replace(F.trim(F.col("precio_unitario")), ",", ".").cast(DecimalType(10, 2))
descuento = F.coalesce(vacio_a_nulo("descuento_pct").cast("int"), F.lit(0))

df_ventas = (
    df_validas_raw
    .select(
        F.trim("transaccion_id").alias("transaccion_id"),
        F.to_date(F.trim("fecha"), "yyyy/MM/dd").alias("fecha_venta"),
        F.trim("hora").alias("hora_venta"),
        F.to_timestamp(F.concat_ws(" ", F.trim("fecha"), F.trim("hora")), "yyyy/MM/dd HH:mm:ss").alias("fecha_hora_venta"),
        F.col("tienda_id").cast("int").alias("tienda_id"),
        F.col("empleado_id").cast("int").alias("empleado_id"),
        vacio_a_nulo("cliente_id").cast("int").alias("cliente_id"),
        F.trim("sku").alias("sku"),
        F.col("cantidad").cast("int").alias("cantidad"),
        precio.alias("precio_unitario"),
        descuento.alias("descuento_pct"),
        F.initcap(F.trim("medio_pago")).alias("medio_pago"),
    )
    .withColumn(
        "venta_total",
        F.round(
            F.col("cantidad")
            * F.col("precio_unitario")
            * (F.lit(1) - F.col("descuento_pct").cast(DecimalType(5, 2)) / F.lit(100)),
            2,
        ).cast(DecimalType(12, 2)),
    )
)

df_ventas.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Antes / después de las columnas más problemáticas**
# 
# Se compara el valor original con el limpio para confirmar visualmente las conversiones de precio, descuento y medio de pago:

# CELL ********************

display(
    df_validas_raw.select(
        "transaccion_id",
        F.col("precio_unitario").alias("precio_original"),
        F.col("descuento_pct").alias("descuento_original"),
        F.col("medio_pago").alias("medio_pago_original"),
    )
    .join(df_ventas.select("transaccion_id", "precio_unitario", "descuento_pct", "medio_pago", "venta_total"),
          "transaccion_id")
    .filter(F.col("precio_original").contains(",") | vacio_a_nulo("descuento_original").isNull())
    .limit(15)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.5 Preparación de `rechazos_ventas`
# 
# Se conservan las columnas **tal como venían del origen** (texto) para facilitar la revisión, más:
# - `motivo_rechazo`: por qué se separó la fila
# - `_fecha_procesamiento`: se agrega al escribir

# CELL ********************

df_rechazos = df_rechazos_raw.withColumn("motivo_rechazo", F.lit("SKU vacío: producto no identificable"))
display(df_rechazos.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.6 Controles de calidad antes de escribir
# 
# | Control | Qué verifica | Si falla |
# |---|---|---|
# | **Reconciliación** | `filas Bronze = duplicados + válidas + rechazadas`. Ninguna fila se pierde ni se inventa en el proceso | Detiene la ejecución |
# | **Unicidad y nulos** | `transaccion_id` único; sin nulos en columnas obligatorias. Detecta también **conversiones fallidas** (un cast inválido deja la columna en `NULL`) | Detiene la ejecución |
# | **Dominio de `medio_pago`** | Solo quedan `Tarjeta`, `Efectivo` y `Billetera Digital` | Detiene la ejecución |
# | **Formato de `hora_venta`** | Todas las horas cumplen `HH:mm:ss` válido | Detiene la ejecución |
# | **Integridad referencial** | `tienda_id`, `empleado_id`, `sku` y `cliente_id` existen en las dimensiones de Silver | **Solo informa**: según la consigna, las ventas sin coincidencia no se descartan; se marcan para revisión en Gold |

# CELL ********************

# 1. Reconciliación
filas_ventas = df_ventas.count()
assert filas_bronze == duplicados_eliminados + filas_ventas + filas_rechazadas, "La reconciliación de filas no cierra"
print(f"✔ Reconciliación: {filas_bronze} = {duplicados_eliminados} duplicados + {filas_ventas} válidas + {filas_rechazadas} rechazos")

# 2. Unicidad y nulos en obligatorias (cliente_id es opcional por regla de negocio)
validar(df_ventas, "ventas_enriquecidas", "transaccion_id",
        ["transaccion_id", "fecha_venta", "hora_venta", "fecha_hora_venta", "tienda_id", "empleado_id",
         "sku", "cantidad", "precio_unitario", "descuento_pct", "medio_pago", "venta_total"])

# 3. Dominio de medio_pago
medios_validos = {"Tarjeta", "Efectivo", "Billetera Digital"}
medios_encontrados = {r["medio_pago"] for r in df_ventas.select("medio_pago").distinct().collect()}
assert medios_encontrados <= medios_validos, f"Valores inesperados en medio_pago: {medios_encontrados - medios_validos}"
print(f"✔ medio_pago con valores válidos: {sorted(medios_encontrados)}")

# 4. Formato de hora
horas_invalidas = df_ventas.filter(~F.col("hora_venta").rlike(r"^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$")).count()
assert horas_invalidas == 0, f"Hay {horas_invalidas} horas con formato inválido"
print("✔ hora_venta con formato HH:mm:ss válido")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Integridad referencial (informativo)
referencias = [
    ("tienda_id",   "dim_tienda",   "tienda_id"),
    ("empleado_id", "dim_empleado", "empleado_id"),
    ("sku",         "dim_producto", "sku"),
    ("cliente_id",  "dim_cliente",  "cliente_id"),
]

for col_venta, dim, col_dim in referencias:
    claves_dim = spark.table(f"{SILVER}.{dim}").select(F.col(col_dim).alias(col_venta))
    huerfanas = (df_ventas.filter(F.col(col_venta).isNotNull())
                          .join(claves_dim, col_venta, "left_anti")
                          .count())
    estado = "OK" if huerfanas == 0 else "REVISAR (se marcarán en Gold)"
    print(f"{col_venta:12} sin coincidencia en {dim:13}: {huerfanas:5} -> {estado}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.7 Escritura en Silver

# CELL ********************

escribir_silver(df_ventas, "ventas_enriquecidas")
escribir_silver(df_rechazos, "rechazos_ventas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## A.8 Verificación final
# 
# Se relee desde el Lakehouse para confirmar que lo persistido coincide con lo esperado según el perfilado del origen:
# 
# | Métrica | Valor esperado |
# |---|---|
# | Filas en `ventas_enriquecidas` | 17.503 |
# | Filas en `rechazos_ventas` | 497 |
# | Medios de pago | Tarjeta 9.645 · Efectivo 5.162 · Billetera Digital 2.696 |
# | Suma de `venta_total` | 10.743.932,79 |

# CELL ********************

df_v = spark.table(f"{SILVER}.ventas_enriquecidas")
df_r = spark.table(f"{SILVER}.rechazos_ventas")

print(f"ventas_enriquecidas: {df_v.count()} filas (esperadas 17503)")
print(f"rechazos_ventas:     {df_r.count()} filas (esperadas 497)")

display(df_v.groupBy("medio_pago").count().orderBy(F.desc("count")))

df_v.agg(
    F.sum("venta_total").alias("venta_total"),
    F.sum("cantidad").alias("unidades"),
    F.min("fecha_venta").alias("primera_venta"),
    F.max("fecha_venta").alias("ultima_venta"),
    F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("ventas_sin_cliente"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # B. Presupuesto → `silver.presupuesto`

# MARKDOWN ********************

# ## B.1 Lectura del dato crudo

# CELL ********************

df_presupuesto_raw = leer_bronze("presupuesto_metas")

filas_presupuesto_bronze = df_presupuesto_raw.count()
print(f"Filas en Bronze: {filas_presupuesto_bronze}")
display(df_presupuesto_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## B.2 Validación de formato antes de convertir
# 
# Igual que con las fechas de las dimensiones, se verifica que los valores tengan la forma esperada **antes** de convertir, para no perder datos en silencio:
# 
# | Columna | Formato esperado | Ejemplo válido |
# |---|---|---|
# | `tienda_id` | Solo dígitos | `101` |
# | `anio_mes` | `YYYY/MM` con mes entre 01 y 12 | `2025/03` |
# | `meta_venta`, `meta_margen` | Número con punto decimal opcional | `13360.38` |

# CELL ********************

reglas_formato_presupuesto = {
    "tienda_id":   r"^[0-9]+$",
    "anio_mes":    r"^[0-9]{4}/(0[1-9]|1[0-2])$",
    "meta_venta":  r"^[0-9]+(\.[0-9]+)?$",
    "meta_margen": r"^[0-9]+(\.[0-9]+)?$",
}

for columna, patron in reglas_formato_presupuesto.items():
    n = df_presupuesto_raw.filter(~F.trim(F.col(columna)).rlike(patron)).count()
    print(f"{columna:12}: {n} filas con formato inválido")
    assert n == 0, f"Formato inválido en {columna}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## B.3 Transformación
# 
# Para convertir `YYYY/MM` en el primer día del mes se agrega `/01` al texto y se convierte con el patrón `yyyy/MM/dd`:

# CELL ********************

df_presupuesto = (
    df_presupuesto_raw
    .select(
        F.col("tienda_id").cast("int").alias("tienda_id"),
        F.to_date(F.concat(F.trim("anio_mes"), F.lit("/01")), "yyyy/MM/dd").alias("anio_mes"),
        F.col("meta_venta").cast(DecimalType(12, 2)).alias("meta_venta"),
        F.col("meta_margen").cast(DecimalType(12, 2)).alias("meta_margen"),
    )
)

df_presupuesto.printSchema()

# Antes / después de la conversión de anio_mes
display(
    df_presupuesto_raw.select(F.col("tienda_id"), F.col("anio_mes").alias("anio_mes_texto"))
    .withColumn("anio_mes_fecha", F.to_date(F.concat(F.trim("anio_mes_texto"), F.lit("/01")), "yyyy/MM/dd"))
    .limit(12)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## B.4 Controles de calidad
# 
# | Control | Qué verifica |
# |---|---|
# | **Unicidad y nulos** | La clave compuesta `(tienda_id, anio_mes)` es única y no hay nulos (también detecta conversiones fallidas) |
# | **Reglas de negocio** | Metas mayores a 0 y `meta_margen` menor que `meta_venta` (el margen es una parte de la venta) |
# | **Completitud** | Cada tienda tiene exactamente 12 meses de presupuesto |
# | **Integridad referencial** | Todas las tiendas del presupuesto existen en `silver.dim_tienda` |

# CELL ********************

# 1. Unicidad de la clave compuesta y nulos
validar(df_presupuesto, "presupuesto", ["tienda_id", "anio_mes"],
        ["tienda_id", "anio_mes", "meta_venta", "meta_margen"])

# 2. Reglas de negocio
fuera_de_regla = df_presupuesto.filter(
    (F.col("meta_venta") <= 0) | (F.col("meta_margen") <= 0) | (F.col("meta_margen") >= F.col("meta_venta"))
).count()
assert fuera_de_regla == 0, f"Hay {fuera_de_regla} metas que no cumplen las reglas de negocio"
print("✔ Metas positivas y meta_margen < meta_venta en todas las filas")

# 3. Completitud: 12 meses por tienda
meses_por_tienda = df_presupuesto.groupBy("tienda_id").agg(F.countDistinct("anio_mes").alias("meses"))
incompletas = meses_por_tienda.filter(F.col("meses") != 12).count()
assert incompletas == 0, f"Hay {incompletas} tiendas sin los 12 meses de presupuesto"
print(f"✔ {meses_por_tienda.count()} tiendas con 12 meses de presupuesto cada una")

# 4. Integridad referencial contra dim_tienda
tiendas_huerfanas = (df_presupuesto.select("tienda_id").distinct()
                     .join(spark.table(f"{SILVER}.dim_tienda").select("tienda_id"), "tienda_id", "left_anti")
                     .count())
assert tiendas_huerfanas == 0, f"Hay {tiendas_huerfanas} tiendas en el presupuesto que no existen en dim_tienda"
print("✔ Todas las tiendas del presupuesto existen en dim_tienda")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## B.5 Escritura y verificación
# 
# | Métrica | Valor esperado |
# |---|---|
# | Filas | 348 |
# | Rango de `anio_mes` | 2025-01-01 a 2025-12-01 |
# | Suma de `meta_venta` | 9.455.903,51 |
# | Suma de `meta_margen` | 2.172.876,81 |

# CELL ********************

escribir_silver(df_presupuesto, "presupuesto")

df_p = spark.table(f"{SILVER}.presupuesto")
df_p.agg(
    F.count("*").alias("filas"),
    F.countDistinct("tienda_id").alias("tiendas"),
    F.min("anio_mes").alias("primer_mes"),
    F.max("anio_mes").alias("ultimo_mes"),
    F.sum("meta_venta").alias("meta_venta_total"),
    F.sum("meta_margen").alias("meta_margen_total"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # C. Inventario → `silver.inventario`
# 
# Foto mensual del stock disponible por tienda y categoría, del área de Supply Chain.
# 
# ## Reglas aplicadas
# 
# | Campo | Tipo origen | Tipo destino | Regla |
# |---|---|---|---|
# | `sin_stock` | — | BOOLEAN | **Nuevo:** `true` si `cantidad_disponible = 0` (quiebre de stock) |
# 
# ## Hallazgos del perfilado
# 
# | Hallazgo | Decisión |
# |---|---|
# | 1.728 filas = **24 tiendas × 6 categorías × 12 meses**, sin huecos ni repetidos | Clave compuesta `(tienda_id, categoria, anio_mes)` |
# | **Las 5 tiendas online (125 a 129) no tienen inventario** | No es un error: las tiendas online no tienen estantería física (probablemente despachan desde un depósito central). Se documenta y se informa en el control de cobertura |
# | Las 6 categorías coinciden con las de `dim_producto` | Se valida en cada ejecución |
# | **6 registros con stock 0** (quiebres de stock) | Se conservan: son información de negocio valiosa. Se marcan con `sin_stock` |
# | Stock entre 0 y 400 unidades | Se valida que no haya cantidades negativas |


# MARKDOWN ********************

# ## C.1 Lectura y validación de formato


# CELL ********************

df_inventario_raw = leer_bronze("inventario_snapshot")

filas_inventario_bronze = df_inventario_raw.count()
print(f"Filas en Bronze: {filas_inventario_bronze}")
display(df_inventario_raw.limit(5))

reglas_formato_inventario = {
    "tienda_id":           r"^[0-9]+$",
    "anio_mes":            r"^[0-9]{4}/(0[1-9]|1[0-2])$",
    "cantidad_disponible": r"^[0-9]+$",
}

for columna, patron in reglas_formato_inventario.items():
    n = df_inventario_raw.filter(~F.trim(F.col(columna)).rlike(patron)).count()
    print(f"{columna:20}: {n} filas con formato inválido")
    assert n == 0, f"Formato inválido en {columna}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## C.2 Transformación

# CELL ********************

df_inventario = (
    df_inventario_raw
    .select(
        F.col("tienda_id").cast("int").alias("tienda_id"),
        F.initcap(F.trim("categoria")).alias("categoria"),
        F.to_date(F.concat(F.trim("anio_mes"), F.lit("/01")), "yyyy/MM/dd").alias("anio_mes"),
        F.col("cantidad_disponible").cast("int").alias("cantidad_disponible"),
    )
    .withColumn("sin_stock", F.col("cantidad_disponible") == 0)
)

df_inventario.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Quiebres de stock detectados**


# CELL ********************

display(df_inventario.filter("sin_stock").orderBy("anio_mes", "tienda_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## C.3 Controles de calidad
# 
# | Control | Qué verifica | Si falla |
# |---|---|---|
# | **Unicidad y nulos** | Clave `(tienda_id, categoria, anio_mes)` única, sin nulos | Detiene |
# | **Regla de negocio** | Sin cantidades negativas | Detiene |
# | **Completitud** | Cada combinación tienda-categoría tiene sus 12 meses | Detiene |
# | **Categorías conformadas** | Todas las categorías existen en `silver.dim_producto` (requisito para poder cruzar inventario con ventas por categoría) | Detiene |
# | **Integridad referencial** | Todas las tiendas existen en `silver.dim_tienda` | Detiene |
# | **Cobertura** | Qué tiendas no tienen inventario | Solo informa |

# CELL ********************

# 1. Unicidad y nulos
validar(df_inventario, "inventario", ["tienda_id", "categoria", "anio_mes"],
        ["tienda_id", "categoria", "anio_mes", "cantidad_disponible"])

# 2. Sin cantidades negativas
negativos = df_inventario.filter(F.col("cantidad_disponible") < 0).count()
assert negativos == 0, f"Hay {negativos} registros con stock negativo"
print("✔ Sin cantidades negativas")

# 3. Completitud: 12 meses por tienda y categoría
combinaciones = df_inventario.groupBy("tienda_id", "categoria").agg(F.countDistinct("anio_mes").alias("meses"))
incompletas = combinaciones.filter(F.col("meses") != 12).count()
assert incompletas == 0, f"Hay {incompletas} combinaciones tienda-categoría sin los 12 meses"
print(f"✔ {combinaciones.count()} combinaciones tienda-categoría con 12 meses cada una")

# 4. Categorías conformadas con dim_producto
categorias_producto = spark.table(f"{SILVER}.dim_producto").select("categoria").distinct()
categorias_huerfanas = df_inventario.select("categoria").distinct().join(categorias_producto, "categoria", "left_anti")
n_cat = categorias_huerfanas.count()
assert n_cat == 0, f"Categorías del inventario que no existen en dim_producto: {[r['categoria'] for r in categorias_huerfanas.collect()]}"
print("✔ Todas las categorías del inventario existen en dim_producto")

# 5. Integridad referencial con dim_tienda
dim_tienda = spark.table(f"{SILVER}.dim_tienda")
tiendas_huerfanas = df_inventario.select("tienda_id").distinct().join(dim_tienda.select("tienda_id"), "tienda_id", "left_anti").count()
assert tiendas_huerfanas == 0, f"Hay {tiendas_huerfanas} tiendas en el inventario que no existen en dim_tienda"
print("✔ Todas las tiendas del inventario existen en dim_tienda")

# 6. Cobertura (informativo): tiendas sin inventario
tiendas_sin_inventario = (dim_tienda.join(df_inventario.select("tienda_id").distinct(), "tienda_id", "left_anti")
                          .select("tienda_id", "nombre_tienda", "formato_tienda")
                          .orderBy("tienda_id"))
print(f"\nTiendas sin inventario en el origen: {tiendas_sin_inventario.count()}")
display(tiendas_sin_inventario)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## C.4 Escritura y verificación


# CELL ********************

escribir_silver(df_inventario, "inventario")

df_i = spark.table(f"{SILVER}.inventario")
df_i.agg(
    F.count("*").alias("filas"),
    F.countDistinct("tienda_id").alias("tiendas"),
    F.countDistinct("categoria").alias("categorias"),
    F.countDistinct("anio_mes").alias("meses"),
    F.sum(F.col("sin_stock").cast("int")).alias("registros_sin_stock"),
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ultimo_mes = df_i.agg(F.max("anio_mes")).first()[0]

stock_cierre = df_i.filter(F.col("anio_mes") == ultimo_mes).agg(F.sum("cantidad_disponible")).first()[0]
suma_incorrecta = df_i.agg(F.sum("cantidad_disponible")).first()[0]

print(f"✔ Stock al cierre de {ultimo_mes}:           {stock_cierre:>8,} unidades  <- valor correcto")
print(f"✘ Suma de los 12 meses (no usar):        {suma_incorrecta:>8,} unidades  <- cuenta la misma mercadería 12 veces")

display(df_i.filter(F.col("anio_mes") == ultimo_mes)
            .groupBy("categoria").agg(F.sum("cantidad_disponible").alias("stock_cierre"))
            .orderBy(F.desc("stock_cierre")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
