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

# # NB_Gold_Dimensiones
# 
# **Proyecto:** TechRetail Corp — Proyecto Final Bootcamp Data Fabric
# **Capa:** Silver → Gold (Warehouse)
# **Autor:** Santiago Martinez
# **Tipo de carga:** Completa (overwrite)
# **Ejecutado por:** pipeline orquestador, después de `NB_Silver_Hechos`
# 
# ## Objetivo
# 
# Construir las **dimensiones del modelo estrella** en `WarehouseGold.gold`, a partir de las tablas limpias de `LakehouseSilver.silver`.
# 
# ```
#                  DimFecha
#                     │
#    DimTienda ── FactVentas ── DimProducto      ← FactVentas se construye en NB_Gold_Hechos
#                  │      │
#          DimEmpleado  DimCliente
# ```
# 
# | Dimensión | Origen | Qué se agrega en Gold |
# |---|---|---|
# | `DimFecha` | **Generada en PySpark** (no viene de ningún archivo) | Calendario completo de 2025 con año, mes, trimestre, nombre del mes y día de la semana |
# | `DimTienda` | `silver.dim_tienda` | `tienda_key` + `nombre_tienda_display` (nombre único con el ID) |
# | `DimProducto` | `silver.dim_producto` | `producto_key` |
# | `DimEmpleado` | `silver.dim_empleado` | `empleado_key` |
# | `DimCliente` | `silver.dim_cliente` | `cliente_key` + fila **-1 "Cliente No Identificado"** |
# 
# ## Conceptos clave
# 
# **Clave surrogada (`*_key`):** número entero asignado por el modelo, independiente del ID del sistema de origen (`tienda_id`, `sku`…). Las relaciones del modelo estrella usan esta clave, de modo que un cambio de codificación en el sistema de origen no rompe el modelo. El ID original se conserva en la dimensión como atributo.
# 
# **Fila -1 en `DimCliente`:** ~39% de las ventas no tiene cliente (compras fuera del programa de lealtad). En lugar de dejar la relación vacía, esas ventas se asocian a un cliente especial "No Identificado", que en Power BI aparece con nombre propio en vez de "(En blanco)".
# 
# ## Requisitos técnicos
# 
# - **Lakehouse predeterminado:** `LakehouseSilver`
# - **Warehouse:** `WarehouseGold` con el schema `gold` creado (`CREATE SCHEMA gold;`)
# - **Runtime 2.0:** la escritura en un Warehouse desde PySpark usa el *Spark connector for Fabric Data Warehouse* (`synapsesql`), que según la documentación oficial está disponible solo en **Runtime 2.0**. Este notebook se asocia a un Environment con Runtime 2.0; los notebooks de Silver siguen en el runtime por defecto.


# MARKDOWN ********************

# ## 1. Configuración
# 
# | Función | Responsabilidad |
# |---|---|
# | `leer_silver()` | Lee una tabla de Silver sin la columna de auditoría `_fecha_procesamiento` |
# | `agregar_clave_surrogada()` | Numera las filas 1, 2, 3… ordenadas por la clave natural. El orden fijo hace que la numeración sea **determinística**: con los mismos datos, cada fila recibe siempre la misma clave |
# | `validar_dimension()` | Controles: clave surrogada única, clave natural única y sin nulos |
# | `escribir_gold()` | Escribe la tabla en el Warehouse con `synapsesql` (modo overwrite) |

# CELL ********************

import com.microsoft.spark.fabric
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER    = "LakehouseSilver.silver"
WAREHOUSE = "WarehouseGold"
GOLD      = f"{WAREHOUSE}.gold"

def leer_silver(tabla):
    """Lee una tabla de Silver sin la columna de auditoría."""
    return spark.table(f"{SILVER}.{tabla}").drop("_fecha_procesamiento")

def agregar_clave_surrogada(df, nombre_clave, orden):
    """Agrega una clave surrogada entera (1, 2, 3...) ordenada por la clave natural."""
    ventana = Window.orderBy(orden)
    return df.withColumn(nombre_clave, F.row_number().over(ventana))

def validar_dimension(df, nombre, clave_surrogada, clave_natural):
    """Controles de calidad de una dimensión antes de escribir."""
    total = df.count()
    dup_sk = total - df.select(clave_surrogada).distinct().count()
    dup_nk = total - df.select(clave_natural).distinct().count()
    nulos_sk = df.filter(F.col(clave_surrogada).isNull()).count()
    print(f"[{nombre}] filas={total} | duplicados {clave_surrogada}={dup_sk} | "
          f"duplicados {clave_natural}={dup_nk} | nulos {clave_surrogada}={nulos_sk}")
    assert dup_sk == 0 and nulos_sk == 0, f"{nombre}: clave surrogada inválida"
    assert dup_nk == 0, f"{nombre}: clave natural duplicada"

def escribir_gold(df, tabla):
    """Escribe la dimensión en el Warehouse Gold (reemplaza el contenido si existe)."""
    df.write.mode("overwrite").synapsesql(f"{GOLD}.{tabla}")
    print(f"✔ {GOLD}.{tabla} escrita")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 2. `DimFecha`
# 
# Ningún archivo de origen trae un calendario: las ventas tienen la fecha `2025-03-15`, pero no dicen que es "Marzo", "trimestre 1" o "Sábado". Esta dimensión se **genera con código**, un registro por día.
# 
# **Rango:** se calcula a partir de las fechas reales de `silver.ventas_enriquecidas`, desde el 1 de enero del primer año hasta el 31 de diciembre del último año. 
# 
# | Campo | Tipo | Ejemplo | Para qué sirve |
# |---|---|---|---|
# | `fecha_key` | INT | `20250315` | Clave hacia `FactVentas` (formato YYYYMMDD) |
# | `fecha` | DATE | `2025-03-15` | Fecha calendario |
# | `anio` | INT | `2025` | |
# | `mes` | INT | `3` | También sirve para **ordenar** `nombre_mes` en Power BI |
# | `trimestre` | INT | `1` | |
# | `nombre_mes` | STRING | `Marzo` | |
# | `anio_mes` | STRING | `2025/03` | Etiqueta mensual, mismo formato que pide `real_vs_presupuesto` |
# | `dia_semana` | STRING | `Sábado` | |
# | `dia_semana_num` | INT | `6` | Para ordenar los días de lunes (1) a domingo (7) en Power BI |
# | `es_fin_de_semana` | BOOLEAN | `true` | Análisis de ventas de fin de semana |
# 
# > Los nombres de mes y día se generan en **español** con listas propias, en lugar de usar `date_format(..., 'MMMM')`, que los devuelve en inglés según la configuración regional del clúster.


# CELL ********************

rango = leer_silver("ventas_enriquecidas").agg(
    F.year(F.min("fecha_venta")).alias("anio_inicio"),
    F.year(F.max("fecha_venta")).alias("anio_fin"),
).first()
fecha_inicio = f"{rango['anio_inicio']}-01-01"
fecha_fin    = f"{rango['anio_fin']}-12-31"
print(f"Rango de DimFecha: {fecha_inicio} a {fecha_fin}")

meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
dias  = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

nombres_mes = F.array(*[F.lit(m) for m in meses])
nombres_dia = F.array(*[F.lit(d) for d in dias])

# dayofweek() de Spark devuelve 1=Domingo ... 7=Sábado; se convierte a 1=Lunes ... 7=Domingo
dia_semana_num = ((F.dayofweek("fecha") + 5) % 7) + 1

dim_fecha = (
    spark.sql(f"SELECT explode(sequence(to_date('{fecha_inicio}'), to_date('{fecha_fin}'), interval 1 day)) AS fecha")
    .select(
        F.date_format("fecha", "yyyyMMdd").cast("int").alias("fecha_key"),
        F.col("fecha"),
        F.year("fecha").alias("anio"),
        F.month("fecha").alias("mes"),
        F.quarter("fecha").alias("trimestre"),
        nombres_mes[F.month("fecha") - 1].alias("nombre_mes"),
        F.date_format("fecha", "yyyy/MM").alias("anio_mes"),
        nombres_dia[dia_semana_num - 1].alias("dia_semana"),
        dia_semana_num.alias("dia_semana_num"),
        (dia_semana_num >= 6).alias("es_fin_de_semana"),
    )
)

validar_dimension(dim_fecha, "DimFecha", "fecha_key", "fecha")
display(dim_fecha.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Controles específicos del calendario:**
# - No faltan días: la cantidad de filas coincide con los días del rango
# - Todas las fechas usadas en ventas, presupuesto e inventario existen en el calendario (si faltara alguna, esas filas quedarían sin fecha en Gold)
# - Control visual: el 1 de enero de 2025 fue **miércoles**

# CELL ********************

from datetime import date
dias_esperados = (date.fromisoformat(fecha_fin) - date.fromisoformat(fecha_inicio)).days + 1
assert dim_fecha.count() == dias_esperados, "Faltan días en el calendario"
print(f"✔ {dias_esperados} días sin huecos")

fechas_calendario = dim_fecha.select("fecha")
for tabla, columna in [("ventas_enriquecidas", "fecha_venta"), ("presupuesto", "anio_mes"), ("inventario", "anio_mes")]:
    faltantes = (leer_silver(tabla).select(F.col(columna).alias("fecha")).distinct()
                 .join(fechas_calendario, "fecha", "left_anti").count())
    assert faltantes == 0, f"{tabla}: {faltantes} fechas no existen en DimFecha"
    print(f"✔ Todas las fechas de {tabla} existen en DimFecha")

dim_fecha.filter(F.col("fecha") == F.lit(fecha_inicio).cast("date")).select("fecha", "dia_semana").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

escribir_gold(dim_fecha, "DimFecha")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 3. `DimTienda`
# 
# | Campo agregado | Por qué |
# |---|---|
# | `tienda_key` | Clave surrogada |
# | `nombre_tienda_display` | En el perfilado se detectaron **6 nombres compartidos por dos sucursales distintas** (por ejemplo, dos "TechRetail Lima Express"). En un gráfico de Power BI se verían como una sola barra que suma dos tiendas. Agregando el ID, cada tienda tiene un nombre único: `TechRetail Lima Express (112)` |
# 
# `region` se conserva porque es la columna que usa el **Row-Level Security** por gerente regional.

# CELL ********************

dim_tienda = agregar_clave_surrogada(leer_silver("dim_tienda"), "tienda_key", "tienda_id")
dim_tienda = (
    dim_tienda
    .withColumn("nombre_tienda_display",
                F.concat(F.col("nombre_tienda"), F.lit(" ("), F.col("tienda_id").cast("string"), F.lit(")")))
    .select("tienda_key", "tienda_id", "nombre_tienda", "nombre_tienda_display",
            "ciudad", "pais", "region", "formato_tienda", "fecha_apertura")
)

validar_dimension(dim_tienda, "DimTienda", "tienda_key", "tienda_id")
assert dim_tienda.select("nombre_tienda_display").distinct().count() == dim_tienda.count(), \
    "nombre_tienda_display debe ser único"
print("✔ nombre_tienda_display es único")

display(dim_tienda.filter(F.col("nombre_tienda").contains("Lima")))
escribir_gold(dim_tienda, "DimTienda")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 4. `DimProducto`
# 
# Se agrega `producto_key`. Se conserva `costo_unitario` porque `NB_Gold_Hechos` lo necesita para calcular `costo_total` (y el margen) en `FactVentas`.

# CELL ********************

dim_producto = (
    agregar_clave_surrogada(leer_silver("dim_producto"), "producto_key", "sku")
    .select("producto_key", "sku", "nombre_producto", "categoria", "subcategoria", "marca", "costo_unitario")
)

validar_dimension(dim_producto, "DimProducto", "producto_key", "sku")
escribir_gold(dim_producto, "DimProducto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 5. `DimEmpleado`
# 
# Se agrega `empleado_key`. Contiene **campos sensibles** que se protegen en pasos posteriores:
# 
# | Campo | Protección | Dónde se configura |
# |---|---|---|
# | `salario` | **Object-Level Security**: solo el rol RRHH ve la columna | Modelo semántico de Power BI |
# | `documento_identidad` | **Dynamic Data Masking** con `partial()` | Warehouse (T-SQL) |
# 
# > **A tener en cuenta:** como esta tabla se reescribe en cada carga (overwrite), el enmascaramiento se aplica con T-SQL **después** de la carga, como un paso del pipeline orquestador. Así se garantiza que siga activo aunque la tabla se regenere.

# CELL ********************

dim_empleado = (
    agregar_clave_surrogada(leer_silver("dim_empleado"), "empleado_key", "empleado_id")
    .select("empleado_key", "empleado_id", "nombre_completo", "tienda_id", "puesto",
            "fecha_ingreso", "salario", "documento_identidad")
)

validar_dimension(dim_empleado, "DimEmpleado", "empleado_key", "empleado_id")
escribir_gold(dim_empleado, "DimEmpleado")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 6. `DimCliente`
# 
# Se agrega `cliente_key` y una fila especial:
# 
# | cliente_key | cliente_id | nombre_completo | fecha_registro | tier_lealtad | ciudad |
# |---|---|---|---|---|---|
# | **-1** | **-1** | **Cliente No Identificado** | *(nulo)* | **Sin Tier** | **N/A** |
# 
# `NB_Gold_Hechos` asigna `cliente_key = -1` a todas las ventas sin `cliente_id`. Así el reporte muestra, por ejemplo, *"Cliente No Identificado: 6.882 ventas"*: casi 4 de cada 10 compras son de clientes fuera del programa de lealtad, un dato de negocio útil en lugar de un hueco.

# CELL ********************

clientes = agregar_clave_surrogada(leer_silver("dim_cliente"), "cliente_key", "cliente_id")

cliente_no_identificado = spark.createDataFrame(
    [(-1, -1, "Cliente No Identificado", None, "Sin Tier", "N/A")],
    "cliente_key INT, cliente_id INT, nombre_completo STRING, fecha_registro DATE, tier_lealtad STRING, ciudad STRING",
)

dim_cliente = (
    cliente_no_identificado
    .unionByName(clientes.select("cliente_key", "cliente_id", "nombre_completo",
                                 "fecha_registro", "tier_lealtad", "ciudad"))
)

validar_dimension(dim_cliente, "DimCliente", "cliente_key", "cliente_id")
assert dim_cliente.filter(F.col("cliente_key") == -1).count() == 1, "Falta la fila -1"
print("✔ Fila -1 'Cliente No Identificado' presente")

display(dim_cliente.orderBy("cliente_key").limit(5))
escribir_gold(dim_cliente, "DimCliente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 7. Verificación final
# 
# Se relee cada dimensión **desde el Warehouse** para confirmar que quedó persistida.
# 
# | Dimensión | Filas esperadas |
# |---|---|
# | `DimFecha` | 365 |
# | `DimTienda` | 29 |
# | `DimProducto` | 185 |
# | `DimEmpleado` | 145 |
# | `DimCliente` | 901 (900 + fila -1) |
# 
# > También se puede verificar desde el editor SQL del Warehouse:
# > ```sql
# > SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
# > FROM INFORMATION_SCHEMA.COLUMNS
# > WHERE TABLE_SCHEMA = 'gold'
# > ORDER BY TABLE_NAME, ORDINAL_POSITION;
# > ```

# CELL ********************

esperado = {"DimFecha": 365, "DimTienda": 29, "DimProducto": 185, "DimEmpleado": 145, "DimCliente": 901}

for tabla, filas_esperadas in esperado.items():
    filas = spark.read.synapsesql(f"{GOLD}.{tabla}").count()
    estado = "OK" if filas == filas_esperadas else "REVISAR"
    print(f"{tabla:12}: {filas:4} filas (esperadas {filas_esperadas}) -> {estado}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Resultado
# 
# Las 5 dimensiones del modelo estrella quedan en `WarehouseGold.gold`.
# 
# **Próximo notebook:** `NB_Gold_Hechos` — construye `FactVentas` reemplazando cada ID natural por su clave surrogada, y las tablas agregadas `real_vs_presupuesto` y `ventas_diarias_por_tienda`.
