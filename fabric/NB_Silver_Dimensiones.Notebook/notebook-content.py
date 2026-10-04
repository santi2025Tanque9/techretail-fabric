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

# # NB_Silver_Dimensiones
# 
# **Proyecto:** TechRetail Corp — Proyecto Final Bootcamp Data Fabric
# **Capa:** Bronze → Silver
# **Autor:** Santiago Martinez
# **Tipo de carga:** Completa (overwrite) — las dimensiones son catálogos maestros
# **Ejecutado por:** pipeline orquestador, después de `PL_Ingesta_Raw_a_Bronze`
# 
# ## Objetivo
# 
# Transformar los catálogos maestros de `LakehouseBronze.bronze` en tablas de dimensión limpias y tipadas en `LakehouseSilver.silver`.
# 
# | Origen (Bronze) | Destino (Silver) | Sistema de origen |
# |---|---|---|
# | `bronze.tiendas` | `silver.dim_tienda` | Operaciones |
# | `bronze.productos` | `silver.dim_producto` | ERP |
# | `bronze.empleados` | `silver.dim_empleado` | RRHH |
# | `bronze.clientes` | `silver.dim_cliente` | CRM (programa de lealtad) |
# 
# Las reglas aplicadas surgen del análisis documentado en **`NB_Perfilado_Bronze`**. Este notebook contiene solo la lógica de producción: transformar, validar y escribir.
# 
# **Lakehouses:** `LakehouseSilver` (predeterminado) · `LakehouseBronze` (adicional)


# MARKDOWN ********************

# ## 1. Configuración y funciones auxiliares
# 
# Se definen funciones reutilizables para que cada dimensión solo describa **qué** transforma, no **cómo** leer, validar o escribir:
# 
# | Función | Responsabilidad |
# |---|---|
# | `leer_bronze()` | Lee la tabla de Bronze y descarta las columnas de auditoría de ingesta (`_archivo_origen`, `_fecha_ingesta`) |
# | `texto_a_fecha()` | Convierte texto `YYYY/MM/DD` a tipo `Date` |
# | `validar()` | Controles de calidad: unicidad de la clave y ausencia de nulos en columnas obligatorias. **Detiene la ejecución** si algo falla, para no persistir datos inválidos |
# | `escribir_silver()` | Agrega la columna de auditoría `_fecha_procesamiento` y sobrescribe la tabla en formato Delta |
# 
# **Decisiones técnicas:**
# - Se usan nombres completos `lakehouse.schema.tabla` para no depender del lakehouse predeterminado del notebook.
# - Los importes (`costo_unitario`, `salario`) se tipan como `DECIMAL(10,2)` y no como `double`, para evitar errores de redondeo de punto flotante.
# - `overwriteSchema = true` permite re-ejecutar el notebook aunque cambie el tipo de alguna columna.


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE = "LakehouseBronze.bronze"
SILVER = "LakehouseSilver.silver"

spark.sql("CREATE SCHEMA IF NOT EXISTS LakehouseSilver.silver")

def leer_bronze(tabla):
    """Lee una tabla de Bronze descartando las columnas de auditoría de ingesta."""
    df = spark.table(f"{BRONZE}.{tabla}")
    return df.drop("_archivo_origen", "_fecha_ingesta")

def texto_a_fecha(col):
    """Convierte 'YYYY/MM/DD' (texto) a tipo Date."""
    return F.to_date(F.trim(F.col(col)), "yyyy/MM/dd")

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

# ## 2. `silver.dim_tienda`
# 
# | Campo | Tipo destino | Regla |
# |---|---|---|
# | `tienda_id` | INT | Cast |
# | `nombre_tienda`, `ciudad`, `pais`, `region`, `formato_tienda` | STRING | Sin transformación |
# | `fecha_apertura` | DATE | Texto `YYYY/MM/DD` → Fecha |
# 
# > **Decisión (ver perfilado 1.5):** existen 6 nombres de tienda compartidos por dos sucursales distintas. No se deduplica: la clave es `tienda_id`.
# 
# `validar()` también protege contra conversiones fallidas: si un cast dejara una columna obligatoria en `NULL`, la ejecución se detiene antes de escribir.

# CELL ********************

df_tienda = (
    leer_bronze("tiendas")
    .select(
        F.col("tienda_id").cast("int").alias("tienda_id"),
        F.col("nombre_tienda"),
        F.col("ciudad"),
        F.col("pais"),
        F.col("region"),
        F.col("formato_tienda"),
        texto_a_fecha("fecha_apertura").alias("fecha_apertura"),
    )
)

validar(df_tienda, "dim_tienda", "tienda_id",
        ["tienda_id", "nombre_tienda", "ciudad", "pais", "region", "formato_tienda", "fecha_apertura"])
escribir_silver(df_tienda, "dim_tienda")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. `silver.dim_producto`
# 
# Catálogo de productos del ERP. Presenta dos problemas de captura manual.
# 
# | Campo | Tipo destino | Regla |
# |---|---|---|
# | `sku` | STRING | Sin transformación (clave natural) |
# | `nombre_producto` | STRING | **Trim** — espacios al inicio/final por captura manual |
# | `categoria` | STRING | **Trim + Formato Título** — `ELECTRONICA` / `electronica` → `Electronica` |
# | `subcategoria`, `marca` | STRING | Sin transformación |
# | `costo_unitario` | DECIMAL(10,2) | Cast a decimal exacto |
# 
# **Evidencia de calidad:** el `groupBy` sobre `categoria` debe mostrar una única variante por categoría (6 en total).

# CELL ********************

df_producto = (
    leer_bronze("productos")
    .select(
        F.col("sku"),
        F.trim("nombre_producto").alias("nombre_producto"),
        F.initcap(F.trim("categoria")).alias("categoria"),
        F.col("subcategoria"),
        F.col("marca"),
        F.col("costo_unitario").cast(DecimalType(10, 2)).alias("costo_unitario"),
    )
)

validar(df_producto, "dim_producto", "sku",
        ["sku", "nombre_producto", "categoria", "subcategoria", "costo_unitario"])

# Evidencia: la categoría queda con un único formato
df_producto.groupBy("categoria").count().orderBy("categoria").show()

escribir_silver(df_producto, "dim_producto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. `silver.dim_empleado`
# 
# Maestro de empleados de RRHH. Contiene **datos sensibles** (`salario`, `documento_identidad`) que se protegen más adelante en Gold con Dynamic Data Masking y OLS.
# 
# | Campo | Tipo destino | Regla |
# |---|---|---|
# | `empleado_id`, `tienda_id` | INT | Sin transformación (cast) |
# | `nombre_completo`, `puesto` | STRING | Sin transformación |
# | `fecha_ingreso` | DATE | Texto `YYYY/MM/DD` → Fecha |
# | `salario` | DECIMAL(10,2) | Cast a decimal exacto · **campo sensible** |
# | `documento_identidad` | STRING | **Trim** — espacios por error de digitación · **campo sensible** |
# 
# > `documento_identidad` se mantiene como texto (no numérico) porque es un identificador: no se opera aritméticamente y podría contener ceros a la izquierda o letras en otros países.

# CELL ********************

df_empleado = (
    leer_bronze("empleados")
    .select(
        F.col("empleado_id").cast("int").alias("empleado_id"),
        F.col("nombre_completo"),
        F.col("tienda_id").cast("int").alias("tienda_id"),
        F.col("puesto"),
        texto_a_fecha("fecha_ingreso").alias("fecha_ingreso"),
        F.col("salario").cast(DecimalType(10, 2)).alias("salario"),
        F.trim("documento_identidad").alias("documento_identidad"),
    )
)

validar(df_empleado, "dim_empleado", "empleado_id",
        ["empleado_id", "nombre_completo", "tienda_id", "puesto",
         "fecha_ingreso", "salario", "documento_identidad"])
escribir_silver(df_empleado, "dim_empleado")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. `silver.dim_cliente`
# 
# Clientes del programa de lealtad (CRM).
# 
# | Campo | Tipo destino | Regla |
# |---|---|---|
# | `cliente_id` | INT | Sin transformación (cast) |
# | `nombre_completo`, `ciudad` | STRING | Sin transformación |
# | `fecha_registro` | DATE | Texto `YYYY/MM/DD` → Fecha |
# | `tier_lealtad` | STRING | **Nulos → `"Sin Tier"`** (cliente registrado sin nivel asignado aún) |
# 
# **Decisión técnica:** un campo vacío del CSV puede llegar a Bronze como `NULL` o como cadena vacía `""` según la configuración del Copy activity. La regla cubre ambos casos para no depender de ese detalle de la ingesta.
# 
# > El cliente "No Identificado" (`-1`) para ventas sin cliente **no** se agrega aquí: se crea en Gold junto con la llave surrogada, según indica el documento de mapping.

# CELL ********************

tier = F.trim(F.col("tier_lealtad"))

df_cliente = (
    leer_bronze("clientes")
    .select(
        F.col("cliente_id").cast("int").alias("cliente_id"),
        F.col("nombre_completo"),
        texto_a_fecha("fecha_registro").alias("fecha_registro"),
        F.when(tier.isNull() | (tier == ""), F.lit("Sin Tier")).otherwise(tier).alias("tier_lealtad"),
        F.col("ciudad"),
    )
)

validar(df_cliente, "dim_cliente", "cliente_id",
        ["cliente_id", "nombre_completo", "fecha_registro", "tier_lealtad"])

# Evidencia: distribución de tiers, incluyendo "Sin Tier"
df_cliente.groupBy("tier_lealtad").count().orderBy("tier_lealtad").show()

escribir_silver(df_cliente, "dim_cliente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Verificación final
# 
# Se relee cada tabla **desde Silver** (no desde el DataFrame en memoria) para confirmar que quedó persistida con la cantidad de filas y el esquema esperados.
# 
# | Tabla | Filas esperadas |
# |---|---|
# | `silver.dim_tienda` | 29 |
# | `silver.dim_producto` | 185 |
# | `silver.dim_empleado` | 145 |
# | `silver.dim_cliente` | 900 |

# CELL ********************

esperado = {"dim_tienda": 29, "dim_producto": 185, "dim_empleado": 145, "dim_cliente": 900}

for tabla, filas_esperadas in esperado.items():
    df = spark.table(f"{SILVER}.{tabla}")
    filas = df.count()
    estado = "OK" if filas == filas_esperadas else "REVISAR"
    print(f"{tabla}: {filas} filas (esperadas {filas_esperadas}) -> {estado}")
    df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Resultado
# 
# Las cuatro dimensiones quedan disponibles en `LakehouseSilver.silver`, limpias y tipadas, listas para:
# 
# - Validar referencias en `NB_Silver_Hechos` (ventas con SKU inexistente, etc.)
# - Generar las llaves surrogadas en `NB_Gold_Dimensiones`
# 
# **Próximo notebook:** `NB_Silver_Hechos`
