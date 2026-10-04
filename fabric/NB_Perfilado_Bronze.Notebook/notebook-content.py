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

# # NB_Perfilado_Bronze
# 
# **Proyecto:** TechRetail Corp — Proyecto Final Bootcamp Data Fabric
# **Propósito:** análisis exploratorio (perfilado) de las tablas crudas de `LakehouseBronze.bronze`
# **Autor:** Santiago Martinez
# 
# ## ¿Para qué sirve este notebook?
# 
# Antes de definir las reglas de limpieza de Silver, se analiza cada tabla de Bronze para entender **qué problemas de calidad tiene realmente** el dato de origen: nulos, espacios sobrantes, formatos, valores inconsistentes y casos de negocio especiales.
# 
# Cada sección termina con **hallazgos y decisiones**, que son las que implementan los notebooks de transformación.
# 
# > **Este notebook NO forma parte del pipeline de carga.** Es documentación ejecutable: se corre manualmente cuando se quiere analizar una fuente. Solo lee datos, no escribe nada.
# 
# | Sección | Tabla Bronze | Notebook que aplica las decisiones |
# |---|---|---|
# | 1 | `bronze.tiendas` | `NB_Silver_Dimensiones` |
# | 2 | `bronze.productos` | `NB_Silver_Dimensiones` |
# | 3 | `bronze.empleados` | `NB_Silver_Dimensiones` |
# | 4 | `bronze.clientes` | `NB_Silver_Dimensiones` |
# 
# **Lakehouse requerido:** `LakehouseBronze` (solo lectura)


# MARKDOWN ********************

# ## 0. Configuración

# CELL ********************

from pyspark.sql import functions as F

BRONZE = "LakehouseBronze.bronze"

def texto_a_fecha(col):
    """Convierte 'YYYY/MM/DD' (texto) a tipo Date."""
    return F.to_date(F.trim(F.col(col)), "yyyy/MM/dd")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. `bronze.tiendas`
# 
# Catálogo de tiendas físicas y online del área de Operaciones.
# 
# **Pasos del análisis:**
# 1. Lectura del dato crudo
# 2. Perfilado: nulos, vacíos, espacios y valores distintos
# 3. Validación de formato
# 4. Prueba de conversión de tipos (antes / después)
# 5. Hallazgos y decisiones

# MARKDOWN ********************

# ### 1.1 Lectura del dato crudo desde Bronze
# 
# Se lee la tabla **tal como la dejó el pipeline de ingesta**, incluyendo las columnas de auditoría (`_archivo_origen`, `_fecha_ingesta`).
# 
# **Qué esperar:**
# - 29 filas
# - Todas las columnas de negocio con tipo `string`, porque Bronze es una copia fiel del CSV sin tipificar
# - Las dos columnas de auditoría agregadas por el Copy activity

# CELL ********************

df_tienda_raw = spark.table(f"{BRONZE}.tiendas")

print(f"Filas en Bronze: {df_tienda_raw.count()}")
print(f"Columnas: {len(df_tienda_raw.columns)}\n")
df_tienda_raw.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_tienda_raw.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1.2 Perfilado de la tabla
# 
# Antes de transformar, se mide la calidad del dato crudo. Se cuentan, por columna:
# 
# - **Nulos o vacíos:** valores `NULL`, cadena vacía `""` o solo espacios
# - **Con espacios sobrantes:** valores donde el texto original difiere del texto con `trim`
# - **Valores distintos:** cardinalidad de cada columna
# 
# Esto permite confirmar qué reglas de limpieza son realmente necesarias, en lugar de aplicarlas a ciegas.

# CELL ********************

columnas_negocio = [c for c in df_tienda_raw.columns if not c.startswith("_")]

perfil = []
for c in columnas_negocio:
    col = F.col(c)
    fila = df_tienda_raw.agg(
        F.sum(F.when(col.isNull() | (F.trim(col) == ""), 1).otherwise(0)).alias("nulos_o_vacios"),
        F.sum(F.when(col != F.trim(col), 1).otherwise(0)).alias("con_espacios"),
        F.countDistinct(col).alias("valores_distintos"),
    ).first()
    perfil.append((c, fila["nulos_o_vacios"], fila["con_espacios"], fila["valores_distintos"]))

df_perfil = spark.createDataFrame(perfil, ["columna", "nulos_o_vacios", "con_espacios", "valores_distintos"])
display(df_perfil)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Distribución de las columnas categóricas**
# 
# Para las columnas con pocos valores posibles (`pais`, `region`, `formato_tienda`) se revisa la lista completa de valores. Así se detectan variantes de escritura (por ejemplo `Mexico` vs `México`) o valores inesperados.
# 
# > `region` equivale a `pais` en este modelo, según el documento de mapping. Esta columna se usa más adelante para el **Row-Level Security** por gerente regional.

# CELL ********************

for c in ["pais", "region", "formato_tienda"]:
    print(f"--- {c} ---")
    df_tienda_raw.groupBy(c).count().orderBy(c).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Tiendas online**
# 
# Las tiendas con formato `Online` no tienen ubicación física, por eso el origen registra `ciudad = "N/A"`. Se conserva tal cual, porque es un valor de negocio válido y no un error de captura.

# CELL ********************

display(df_tienda_raw.filter(F.col("formato_tienda") == "Online")
                     .select("tienda_id", "nombre_tienda", "ciudad", "pais", "formato_tienda"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Nombres de tienda repetidos**
# 
# El perfilado muestra **29 `tienda_id` distintos pero solo 23 `nombre_tienda` distintos**: hay 6 nombres compartidos por dos tiendas.
# 
# No son duplicados. Cada par tiene un `tienda_id` y una `fecha_apertura` diferentes: son **sucursales distintas del mismo formato en la misma ciudad** (por ejemplo, dos locales "Express" en Ciudad de México). El nombre comercial describe ciudad y formato, no identifica la tienda.
# 
# **Decisiones:**
# - **No se deduplica por nombre.** Hacerlo eliminaría tiendas reales con sus ventas y su presupuesto.
# - **La clave de la dimensión es `tienda_id`**, que sí es único.
# - `nombre_tienda` se mantiene sin transformación, como indica el mapping.
# - **Impacto en reportes:** un segmentador por `nombre_tienda` en Power BI agruparía dos tiendas en una sola fila. Para evitarlo, en Gold se agrega un nombre de visualización único (por ejemplo `TechRetail Lima Express (112)`).

# CELL ********************

df_nombres_repetidos = (
    df_tienda_raw
    .groupBy("nombre_tienda")
    .agg(
        F.count("*").alias("cantidad_tiendas"),
        F.collect_list("tienda_id").alias("tienda_ids"),
        F.collect_list("fecha_apertura").alias("fechas_apertura"),
    )
    .filter(F.col("cantidad_tiendas") > 1)
    .orderBy("nombre_tienda")
)

print(f"Nombres compartidos por más de una tienda: {df_nombres_repetidos.count()}")
display(df_nombres_repetidos)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1.3 Validación de formato antes de convertir
# 
# Convertir tipos sin validar antes es riesgoso: dependiendo de la configuración de Spark, un valor mal formado puede **convertirse silenciosamente en `NULL`** (y perderse) o **detener el proceso con error**.
# 
# Por eso se verifica primero que los valores tengan el formato esperado:
# 
# | Columna | Formato esperado | Expresión regular |
# |---|---|---|
# | `tienda_id` | Solo dígitos | `^[0-9]+$` |
# | `fecha_apertura` | `YYYY/MM/DD` | `^[0-9]{4}/[0-9]{2}/[0-9]{2}$` |
# 
# **Resultado esperado:** 0 filas inválidas en ambas columnas.

# CELL ********************

reglas_formato = {
    "tienda_id":      r"^[0-9]+$",
    "fecha_apertura": r"^[0-9]{4}/[0-9]{2}/[0-9]{2}$",
}

for columna, patron in reglas_formato.items():
    invalidas = df_tienda_raw.filter(~F.trim(F.col(columna)).rlike(patron))
    n = invalidas.count()
    print(f"{columna}: {n} filas con formato inválido")
    if n > 0:
        invalidas.select("tienda_id", columna).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Rango de fechas de apertura**
# 
# Como control de razonabilidad, se revisa la fecha más antigua y la más reciente. Una fecha en el futuro o demasiado antigua indicaría un error en el origen.

# CELL ********************

df_tienda_raw.agg(
    F.min("fecha_apertura").alias("primera_apertura"),
    F.max("fecha_apertura").alias("ultima_apertura"),
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1.4 Prueba de conversión de tipos (antes / después)
# 
# Se simula la conversión que hará Silver, **sin escribir nada**, poniendo lado a lado el valor original (texto) y el convertido.
# 
# Se cuentan además las **conversiones fallidas**: filas donde el texto tenía valor pero el resultado quedó en `NULL`. Este control detecta valores con forma correcta pero imposibles (por ejemplo `2021/02/30`), que la expresión regular del paso anterior no puede detectar.

# CELL ********************

df_comparacion = df_tienda_raw.select(
    F.col("tienda_id").alias("tienda_id_texto"),
    F.col("tienda_id").cast("int").alias("tienda_id_int"),
    F.col("fecha_apertura").alias("fecha_texto"),
    texto_a_fecha("fecha_apertura").alias("fecha_date"),
)
display(df_comparacion.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fallidas = df_comparacion.filter(
    (F.col("tienda_id_texto").isNotNull() & F.col("tienda_id_int").isNull()) |
    (F.col("fecha_texto").isNotNull() & F.col("fecha_date").isNull())
).count()

print(f"Conversiones fallidas: {fallidas}")
assert fallidas == 0, "Hay valores que no se pudieron convertir; revisar el paso 1.3"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1.5 Hallazgos y decisiones — `tiendas`
# 
# | # | Hallazgo | Decisión |
# |---|---|---|
# | 1 | 29 filas, sin nulos ni espacios sobrantes en ninguna columna | No se requiere limpieza de texto |
# | 2 | `tienda_id` único (29 valores distintos) y numérico | Es la clave de la dimensión; se tipa como `INT` |
# | 3 | `fecha_apertura` en texto `YYYY/MM/DD`, formato válido en el 100% de las filas, sin conversiones fallidas | Convertir a `DATE` |
# | 4 | **6 nombres de tienda compartidos por 2 tiendas** (23 nombres distintos para 29 tiendas), con distinto `tienda_id` y fecha de apertura | **No son duplicados**: son sucursales distintas. No se deduplica. `nombre_tienda` se conserva sin cambios en Silver; en Gold se agrega un nombre de visualización único para Power BI |
# | 5 | Tiendas `Online` con `ciudad = "N/A"` | Valor de negocio válido, se conserva |
# | 6 | `region` equivale a `pais` | Se conserva; se usará para el RLS por gerente regional |

# MARKDOWN ********************

# ## 2. `bronze.productos`
# 
# *Pendiente.*

# CELL ********************

df_tienda_raw = spark.table(f"{BRONZE}.productos")

print(f"Filas en Bronze: {df_tienda_raw.count()}")
print(f"Columnas: {len(df_tienda_raw.columns)}\n")
df_tienda_raw.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. `bronze.empleados`
# 
# *Pendiente.*

# MARKDOWN ********************

# ## 4. `bronze.clientes`
# 
# *Pendiente.*
