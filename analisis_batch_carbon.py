# -*- coding: utf-8 -*-
"""
Tarea 3 - Procesamiento de Datos con Apache Spark (UNAD - Big Data 202016911)
Procesamiento en batch: producción y regalías de carbón en Colombia.

Fuente: Agencia Nacional de Minería (ANM) - "ANM Volúmen de Explotación de
Minerales Asociados a Pagos de Regalías", Portal de Datos Abiertos de Colombia
(datos.gov.co, identificador r85m-vv6c).

Problema: ¿cómo ha evolucionado la producción de carbón que paga regalías en
Colombia, qué departamentos y municipios la concentran, cuánto se paga de
regalía por tonelada y qué papel tiene el departamento de Córdoba?

Uso:
    spark-submit analisis_batch_carbon.py --input regalias_minerales.csv --output resultados
    (también acepta rutas HDFS, p. ej. hdfs://localhost:9000/Tarea3/regalias_minerales.csv)
"""

import argparse
import os
import re
import unicodedata

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ACENTOS = "ÁÉÍÓÚÜÑáéíóúüñ"
SIN_ACENTOS = "AEIOUUNaeiouun"


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def normalizar_nombre_columna(nombre):
    """'Año Liquidado' -> 'ano_liquidado'; 'a_o_liquidado' se deja igual."""
    nombre = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    nombre = re.sub(r"[^0-9a-zA-Z]+", "_", nombre.strip().lower())
    return nombre.strip("_")


def buscar_columna(columnas, *prefijos):
    """Devuelve la primera columna cuyo nombre empiece por alguno de los prefijos."""
    for p in prefijos:
        for c in columnas:
            if c.startswith(p):
                return c
    raise ValueError(f"No se encontró una columna que empiece por {prefijos}. Columnas: {columnas}")


def texto_limpio(col):
    """Mayúsculas, sin espacios sobrantes y sin tildes."""
    return F.upper(F.trim(F.translate(col, ACENTOS, SIN_ACENTOS)))


def numero_limpio(col):
    """'1,234,567.00' -> 1234567.0 ; '- 0' -> 0.0 ; '' o '-' -> null."""
    s = F.regexp_replace(F.trim(col), r"[,\s$]", "")
    s = F.when((s == "") | (s == "-"), None).otherwise(s)
    n = s.cast("double")
    return F.when(n == 0, F.lit(0.0)).otherwise(n)


def titulo(texto):
    print("\n" + "=" * 78)
    print(texto)
    print("=" * 78)


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Análisis batch de regalías de carbón (ANM)")
    parser.add_argument("--input", default="regalias_minerales.csv", help="CSV descargado de datos.gov.co")
    parser.add_argument("--output", default="resultados", help="Carpeta local para tablas y gráficas")
    parser.add_argument("--departamento", default="CORDOBA", help="Departamento a analizar en detalle")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("Tarea3_Batch_Regalias_Carbon")
             .config("spark.sql.shuffle.partitions", "8")   # VM pequeña: menos particiones = más rápido
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    os.makedirs(os.path.join(args.output, "tablas"), exist_ok=True)
    os.makedirs(os.path.join(args.output, "graficas"), exist_ok=True)

    # ------------------------------------------------------------------ 1. Carga
    titulo("1. CARGA DEL CONJUNTO DE DATOS")
    crudo = (spark.read
             .option("header", True)
             .option("multiLine", True)
             .option("escape", '"')
             .csv(args.input))
    for c in crudo.columns:
        crudo = crudo.withColumnRenamed(c, normalizar_nombre_columna(c))
    cols = crudo.columns

    c_dane = buscar_columna(cols, "codigo_dane")
    c_mun = buscar_columna(cols, "nombre_municipio", "municipio")
    c_dep = buscar_columna(cols, "nombre_departamento", "departamento")
    c_rec = buscar_columna(cols, "recurso_natural", "recurso")
    c_trim = buscar_columna(cols, "trimestre")
    c_anio = buscar_columna(cols, "a_o_liquidado", "ano_liquidado", "anio")
    c_uni = buscar_columna(cols, "unidad_medida", "unidad")
    c_contra = buscar_columna(cols, "contraprestacion")
    c_reg = buscar_columna(cols, "regalias_pagadas", "regalias")
    c_vol = buscar_columna(cols, "volumenes_de_explotacion", "volumen")
    c_grupo = buscar_columna(cols, "min_agrupado", "mineral_agrupado")

    total_crudo = crudo.count()
    print(f"Registros leídos: {total_crudo:,}")
    print(f"Columnas ({len(cols)}): {cols}")
    crudo.printSchema()
    crudo.show(5, truncate=40)

    # ------------------------------------------------- 2. Calidad de los datos
    titulo("2. CALIDAD DE LOS DATOS (valores nulos o vacíos por columna)")
    nulos = crudo.select([
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
        for c in cols
    ])
    nulos.show(truncate=False)

    # ---------------------------------------------- 3. Limpieza y transformación
    titulo("3. LIMPIEZA Y TRANSFORMACIÓN")
    df = crudo.select(
        F.lpad(F.trim(F.col(c_dane)), 5, "0").alias("codigo_dane"),
        texto_limpio(F.col(c_mun)).alias("municipio"),
        texto_limpio(F.col(c_dep)).alias("departamento"),
        texto_limpio(F.col(c_rec)).alias("recurso"),
        texto_limpio(F.col(c_grupo)).alias("grupo_mineral"),
        texto_limpio(F.col(c_uni)).alias("unidad"),
        texto_limpio(F.col(c_contra)).alias("contraprestacion"),
        F.col(c_anio).cast("int").alias("anio"),
        F.regexp_extract(F.col(c_trim), r"(\d)", 1).cast("int").alias("trimestre"),
        numero_limpio(F.col(c_vol)).alias("volumen"),
        numero_limpio(F.col(c_reg)).alias("regalias_cop"),
    )

    df = (df.dropna(subset=["anio", "trimestre", "departamento"])
            .fillna({"volumen": 0.0, "regalias_cop": 0.0})
            .dropDuplicates())
    df = df.withColumn("periodo", F.concat_ws("-T", F.col("anio").cast("string"), F.col("trimestre").cast("string")))
    df.cache()

    total_limpio = df.count()
    print(f"Registros después de limpieza: {total_limpio:,} "
          f"(se descartaron {total_crudo - total_limpio:,} nulos/duplicados)")
    print(f"Años cubiertos: {df.agg(F.min('anio')).first()[0]} - {df.agg(F.max('anio')).first()[0]}")

    titulo("3.1 Registros con valores atípicos")
    df.select(
        F.sum(F.when(F.col("volumen") < 0, 1).otherwise(0)).alias("volumen_negativo"),
        F.sum(F.when(F.col("regalias_cop") < 0, 1).otherwise(0)).alias("regalias_negativas"),
        F.sum(F.when((F.col("volumen") == 0) & (F.col("regalias_cop") > 0), 1).otherwise(0))
         .alias("regalias_sin_volumen"),
    ).show()

    # ---------------------------------------- 4. EDA general (todos los minerales)
    titulo("4. EDA GENERAL - Regalías por grupo de mineral (todos los minerales)")
    por_grupo = (df.groupBy("grupo_mineral")
                   .agg(F.count("*").alias("registros"),
                        F.round(F.sum("regalias_cop") / 1e9, 2).alias("regalias_miles_millones"))
                   .orderBy(F.desc("regalias_miles_millones")))
    total_reg = df.agg(F.sum("regalias_cop")).first()[0] or 1.0
    por_grupo = por_grupo.withColumn(
        "participacion_pct", F.round(F.col("regalias_miles_millones") * 1e9 / total_reg * 100, 2))
    por_grupo.show(15, truncate=False)

    # ----------------------------------------------------- 5. Filtro: carbón
    titulo("5. SUBCONJUNTO DE CARBÓN")
    carbon = df.filter(
        (F.col("recurso").contains("CARBON") | F.col("grupo_mineral").contains("CARBON"))
        & F.col("unidad").startswith("TONELADA")
        & (F.col("volumen") >= 0)
    ).withColumn("tipo_carbon",
                 F.when(F.col("recurso").contains("METALURG") | F.col("recurso").contains("COQUI"),
                        "METALURGICO")
                  .when(F.col("recurso").contains("TERMIC"), "TERMICO")
                  .otherwise("SIN CLASIFICAR"))
    carbon.cache()
    print(f"Registros de carbón: {carbon.count():,}")
    carbon.groupBy("recurso", "tipo_carbon").count().orderBy(F.desc("count")).show(truncate=False)
    carbon.select("volumen", "regalias_cop").describe().show()

    # --------------------------------------------- 5.1 Evolución anual
    titulo("5.1 Producción y regalías de carbón por año")
    w = Window.orderBy("anio")
    anual = (carbon.groupBy("anio")
             .agg(F.sum("volumen").alias("toneladas"), F.sum("regalias_cop").alias("regalias_cop"))
             .withColumn("millones_t", F.round(F.col("toneladas") / 1e6, 2))
             .withColumn("regalias_billones", F.round(F.col("regalias_cop") / 1e12, 3))
             .withColumn("cop_por_tonelada",
                         F.round(F.col("regalias_cop") / F.when(F.col("toneladas") > 0, F.col("toneladas")), 0))
             .withColumn("variacion_pct",
                         F.round((F.col("toneladas") / F.lag("toneladas").over(w) - 1) * 100, 1))
             .orderBy("anio"))
    anual.select("anio", "millones_t", "variacion_pct", "regalias_billones", "cop_por_tonelada").show(50)

    # --------------------------------------------- 5.2 Departamentos
    titulo("5.2 Producción acumulada de carbón por departamento")
    total_t = carbon.agg(F.sum("volumen")).first()[0] or 1.0
    deptos = (carbon.groupBy("departamento")
              .agg(F.round(F.sum("volumen") / 1e6, 2).alias("millones_t"),
                   F.round(F.sum("regalias_cop") / 1e9, 1).alias("regalias_miles_millones"),
                   F.countDistinct("municipio").alias("municipios"))
              .withColumn("participacion_pct", F.round(F.col("millones_t") * 1e6 / total_t * 100, 2))
              .orderBy(F.desc("millones_t")))
    deptos.show(30, truncate=False)

    # --------------------------------------------- 5.3 Municipios
    titulo("5.3 Top 15 municipios productores de carbón")
    municipios = (carbon.groupBy("departamento", "municipio")
                  .agg(F.round(F.sum("volumen") / 1e6, 3).alias("millones_t"),
                       F.round(F.sum("regalias_cop") / 1e9, 1).alias("regalias_miles_millones"))
                  .orderBy(F.desc("millones_t")))
    municipios.show(15, truncate=False)

    # --------------------------------------------- 5.4 Estacionalidad
    titulo("5.4 Producción promedio por trimestre (estacionalidad)")
    trimestral = (carbon.groupBy("anio", "trimestre").agg(F.sum("volumen").alias("t"))
                  .groupBy("trimestre")
                  .agg(F.round(F.avg("t") / 1e6, 2).alias("promedio_millones_t"))
                  .orderBy("trimestre"))
    trimestral.show()

    # --------------------------------------------- 5.5 Tipo de carbón
    titulo("5.5 Producción por tipo de carbón")
    tipos = (carbon.groupBy("tipo_carbon")
             .agg(F.round(F.sum("volumen") / 1e6, 2).alias("millones_t"),
                  F.round(F.sum("regalias_cop") / F.sum("volumen"), 0).alias("cop_por_tonelada"))
             .orderBy(F.desc("millones_t")))
    tipos.show(truncate=False)

    # --------------------------------------------- 5.6 Departamento en detalle
    depto = texto_limpio(F.lit(args.departamento))
    nombre_depto = spark.range(1).select(depto).first()[0]
    titulo(f"5.6 Detalle del departamento: {nombre_depto}")
    detalle = carbon.filter(F.col("departamento") == nombre_depto)
    detalle_anual = (detalle.groupBy("anio")
                     .agg(F.round(F.sum("volumen") / 1e3, 1).alias("miles_t"),
                          F.round(F.sum("regalias_cop") / 1e9, 2).alias("regalias_miles_millones"))
                     .orderBy("anio"))
    detalle_anual.show(50)
    (detalle.groupBy("municipio")
            .agg(F.round(F.sum("volumen") / 1e3, 1).alias("miles_t"))
            .orderBy(F.desc("miles_t"))).show(truncate=False)

    # --------------------------------------------- 6. Operaciones con RDD
    titulo("6. OPERACIONES CON RDD (map, filter, reduceByKey, take, count)")
    rdd = carbon.select("departamento", "volumen").rdd
    rdd_pares = (rdd.filter(lambda fila: fila["volumen"] > 0)              # transformación
                    .map(lambda fila: (fila["departamento"], fila["volumen"]))  # transformación
                    .reduceByKey(lambda a, b: a + b))                       # transformación
    print(f"Departamentos con producción (count): {rdd_pares.count()}")       # acción
    top5 = rdd_pares.takeOrdered(5, key=lambda kv: -kv[1])                   # acción
    for dep, t in top5:
        print(f"  {dep:<25} {t / 1e6:>12,.2f} millones de t")
    total_rdd = rdd_pares.map(lambda kv: kv[1]).reduce(lambda a, b: a + b)    # acción
    print(f"Total con RDD: {total_rdd / 1e6:,.2f} Mt | Total con DataFrame: {total_t / 1e6:,.2f} Mt")

    # --------------------------------------------- 7. Spark SQL
    titulo("7. CONSULTA CON SPARK SQL - Concentración: participación de los 2 mayores departamentos por año")
    carbon.createOrReplaceTempView("carbon")
    concentracion = spark.sql("""
        WITH dep_anio AS (
            SELECT anio, departamento, SUM(volumen) AS t
            FROM carbon GROUP BY anio, departamento
        ), ranking AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY anio ORDER BY t DESC) AS pos,
                      SUM(t) OVER (PARTITION BY anio) AS total_anio
            FROM dep_anio
        )
        SELECT anio,
               ROUND(SUM(CASE WHEN pos <= 2 THEN t END) / MAX(total_anio) * 100, 1) AS pct_top2,
               CONCAT_WS(' + ', COLLECT_LIST(CASE WHEN pos <= 2 THEN departamento END)) AS lideres
        FROM ranking
        WHERE total_anio > 0
        GROUP BY anio ORDER BY anio
    """)
    concentracion.show(50, truncate=False)

    # --------------------------------------------- 8. Guardar resultados
    titulo("8. GUARDANDO RESULTADOS")
    tablas = {
        "regalias_por_grupo_mineral": por_grupo,
        "carbon_anual": anual,
        "carbon_departamentos": deptos,
        "carbon_top_municipios": municipios.limit(30),
        "carbon_trimestral": trimestral,
        "carbon_tipos": tipos,
        f"carbon_{nombre_depto.lower()}_anual": detalle_anual,
        "concentracion_top2": concentracion,
    }
    pdfs = {}
    for nombre, tabla in tablas.items():
        pdf = tabla.toPandas()          # tablas agregadas: pocas filas
        pdf.to_csv(os.path.join(args.output, "tablas", f"{nombre}.csv"), index=False, encoding="utf-8")
        pdfs[nombre] = pdf
        print(f"  tablas/{nombre}.csv ({len(pdf)} filas)")

    # --------------------------------------------- 9. Visualización
    titulo("9. VISUALIZACIÓN")
    graficar(pdfs, nombre_depto, os.path.join(args.output, "graficas"))

    spark.stop()
    print("\nAnálisis finalizado. Revise la carpeta:", os.path.abspath(args.output))


def graficar(pdfs, nombre_depto, carpeta):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    azul, naranja, gris = "#1f5fa8", "#e8862a", "#8a8f98"
    plt.rcParams.update({"figure.dpi": 120, "axes.spines.top": False, "axes.spines.right": False,
                         "axes.titleweight": "bold", "font.size": 10})

    def guardar(fig, nombre):
        ruta = os.path.join(carpeta, nombre)
        fig.tight_layout()
        fig.savefig(ruta)
        plt.close(fig)
        print("  graficas/" + nombre)

    # 1. Evolución anual
    a = pdfs["carbon_anual"].dropna(subset=["anio"])
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(a["anio"].astype(str), a["millones_t"], color=azul, label="Producción (Mt)")
    ax1.set_ylabel("Millones de toneladas")
    ax2 = ax1.twinx()
    ax2.plot(a["anio"].astype(str), a["regalias_billones"], color=naranja, marker="o", lw=2,
             label="Regalías (billones COP)")
    ax2.set_ylabel("Billones de COP")
    ax2.spines["top"].set_visible(False)
    ax1.set_title("Carbón en Colombia: producción con regalías y regalías pagadas por año")
    ax1.tick_params(axis="x", rotation=45)
    l1, n1 = ax1.get_legend_handles_labels()
    l2, n2 = ax2.get_legend_handles_labels()
    ax1.legend(l1 + l2, n1 + n2, loc="upper left", frameon=False)
    guardar(fig, "01_produccion_regalias_anual.png")

    # 2. Departamentos
    d = pdfs["carbon_departamentos"].head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 5))
    colores = [naranja if x == nombre_depto else azul for x in d["departamento"]]
    ax.barh(d["departamento"], d["millones_t"], color=colores)
    for y, (v, p) in enumerate(zip(d["millones_t"], d["participacion_pct"])):
        ax.text(v, y, f"  {v:,.0f} Mt ({p:.1f}%)", va="center", fontsize=8)
    ax.set_xlabel("Millones de toneladas (acumulado)")
    ax.set_title("Top 10 departamentos productores de carbón")
    guardar(fig, "02_top_departamentos.png")

    # 3. Municipios
    m = pdfs["carbon_top_municipios"].head(15).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(m["municipio"] + " (" + m["departamento"].str[:10] + ")", m["millones_t"], color=azul)
    ax.set_xlabel("Millones de toneladas (acumulado)")
    ax.set_title("Top 15 municipios productores de carbón")
    guardar(fig, "03_top_municipios.png")

    # 4. Regalía por tonelada
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(a["anio"].astype(str), a["cop_por_tonelada"], color=azul, marker="o", lw=2)
    ax.set_ylabel("COP por tonelada")
    ax.set_title("Regalía pagada por tonelada de carbón")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y", alpha=0.3)
    guardar(fig, "04_regalia_por_tonelada.png")

    # 5. Estacionalidad
    t = pdfs["carbon_trimestral"]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar("T" + t["trimestre"].astype(str), t["promedio_millones_t"], color=azul)
    ax.set_ylabel("Millones de t (promedio)")
    ax.set_title("Producción promedio por trimestre")
    guardar(fig, "05_estacionalidad_trimestral.png")

    # 6. Departamento en detalle
    clave = f"carbon_{nombre_depto.lower()}_anual"
    c = pdfs.get(clave)
    if c is not None and len(c):
        fig, ax = plt.subplots(figsize=(10, 4.5))
        ax.bar(c["anio"].astype(str), c["miles_t"], color=naranja)
        ax.set_ylabel("Miles de toneladas")
        ax.set_title(f"Producción de carbón con regalías en {nombre_depto.title()}")
        ax.tick_params(axis="x", rotation=45)
        guardar(fig, f"06_{nombre_depto.lower()}_anual.png")

    # 7. Participación por grupo mineral
    g = pdfs["regalias_por_grupo_mineral"].head(8)
    otros = 100 - g["participacion_pct"].sum()
    etiquetas = list(g["grupo_mineral"].str.title()) + (["Otros"] if otros > 0.5 else [])
    valores = list(g["participacion_pct"]) + ([otros] if otros > 0.5 else [])
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(etiquetas[::-1], valores[::-1], color=[gris] * (len(valores) - 1) + [azul])
    ax.set_xlabel("% del total de regalías")
    ax.set_title("Participación en regalías por grupo de mineral")
    guardar(fig, "07_regalias_por_mineral.png")


if __name__ == "__main__":
    main()
