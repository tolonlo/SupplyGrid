#!/usr/bin/env python3
"""
E2-05 — Medicion de Q1-Q5 con percentiles.

Ejecuta cada consulta 200 veces en dos escenarios:
- frio: proveedores de la cola larga
- caliente: proveedores del top 1 %

Genera:
- datos/salida/metricas_q1_q5.csv
- datos/salida/metricas_q1_q5_crudas.csv

Q4 tiene una limitacion del dataset: no existen eventos de entrega ni
cantidades efectivamente entregadas. Por ello no es posible derivar un
OTIF/fill rate logistico real. La consulta mide la agregacion mensual
disponible sobre ordenes y lineas, dejando esta limitacion explicitamente
documentada.
"""

import csv
import os
import random
import sys
import time
from pathlib import Path

import psycopg2

REPETICIONES = 200
SEED = 42

SALIDA = Path("datos/salida")
ARCHIVO_RESUMEN = SALIDA / "metricas_q1_q5.csv"
ARCHIVO_CRUDO = SALIDA / "metricas_q1_q5_crudas.csv"


def conectar():
    try:
        return psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", "postgres"),
            dbname=os.environ.get("PGDATABASE", "supplygrid_db"),
        )
    except Exception as exc:
        print(f"No se pudo conectar a PostgreSQL: {exc}")
        sys.exit(1)


def percentil(valores, p):
    ordenados = sorted(valores)
    if not ordenados:
        return 0.0

    posicion = (len(ordenados) - 1) * p
    inferior = int(posicion)
    superior = min(inferior + 1, len(ordenados) - 1)
    fraccion = posicion - inferior

    return (
        ordenados[inferior]
        + (ordenados[superior] - ordenados[inferior]) * fraccion
    )


def obtener_volumen(cur):
    tablas = [
        ("proveedores.proveedores",),
        ("proveedores.contratos",),
        ("catalogo.catalogo_sku",),
        ("ordenes.ordenes",),
        ("ordenes.lineas_orden",),
        ("logistica.franjas_descargue",),
        ("auditoria.eventos",),
    ]

    volumen = {}
    for (tabla,) in tablas:
        cur.execute(f"SELECT COUNT(*) FROM {tabla}")
        volumen[tabla] = cur.fetchone()[0]

    return volumen


def obtener_proveedores(cur):
    """
    Selecciona grupos a partir del numero real de ordenes.

    caliente = top 1 % de los proveedores con ordenes
    frio = 1 % inferior de los proveedores con ordenes
    """
    cur.execute("""
        SELECT proveedor_id, COUNT(*) AS cantidad
        FROM ordenes.ordenes
        GROUP BY proveedor_id
        ORDER BY cantidad DESC, proveedor_id;
    """)
    filas = cur.fetchall()

    cantidad_grupo = max(1, len(filas) // 100)

    calientes = [fila[0] for fila in filas[:cantidad_grupo]]
    frios = [fila[0] for fila in filas[-cantidad_grupo:]]

    return frios, calientes


def contrato_proveedor(cur, proveedor_id):
    cur.execute("""
        SELECT id
        FROM proveedores.contratos
        WHERE proveedor_id = %s
        ORDER BY id
        LIMIT 1;
    """, (proveedor_id,))
    fila = cur.fetchone()
    return fila[0] if fila else None


def skus_proveedor(cur, proveedor_id):
    cur.execute("""
        SELECT sku_codigo
        FROM catalogo.catalogo_sku
        WHERE proveedor_id = %s
        ORDER BY sku_codigo
        LIMIT 20;
    """, (proveedor_id,))
    return [fila[0] for fila in cur.fetchall()]


def medir(cur, nombre, escenario, parametros, sql):
    tiempos = []
    crudos = []

    for numero, params in enumerate(parametros, start=1):
        inicio = time.perf_counter_ns()
        cur.execute(sql, params)
        cur.fetchall()
        fin = time.perf_counter_ns()

        ms = (fin - inicio) / 1_000_000
        tiempos.append(ms)
        crudos.append((nombre, escenario, numero, ms, repr(params)))

    return tiempos, crudos


def construir_parametros(cur, proveedores, rng):
    datos = []

    for _ in range(REPETICIONES):
        proveedor = rng.choice(proveedores)
        contrato = contrato_proveedor(cur, proveedor)
        skus = skus_proveedor(cur, proveedor)

        datos.append({
            "proveedor": proveedor,
            "contrato": contrato,
            "skus": skus,
        })

    return datos


def main():
    SALIDA.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)

    conn = conectar()
    conn.autocommit = True
    cur = conn.cursor()

    volumen = obtener_volumen(cur)
    frios, calientes = obtener_proveedores(cur)

    print("== E2-05: medicion Q1-Q5 ==")
    print(f"Repeticiones por consulta/escenario: {REPETICIONES}")
    print(f"Proveedores frios disponibles: {len(frios)}")
    print(f"Proveedores calientes disponibles: {len(calientes)}")
    print(f"Ejemplo frio: {frios[0]}")
    print(f"Ejemplo caliente: {calientes[0]}")

    resultados = []
    crudos_totales = []

    for escenario, proveedores in (
        ("frio", frios),
        ("caliente", calientes),
    ):
        base = construir_parametros(cur, proveedores, rng)

        # Q1 — contrato vigente por proveedor y fecha.
        parametros = [
            (d["proveedor"], "2026-09-18 10:00:00")
            for d in base
        ]
        sql = """
            SELECT id, proveedor_id, fecha_inicio, fecha_fin, estado
            FROM proveedores.contratos
            WHERE proveedor_id = %s
              AND fecha_inicio <= %s
              AND fecha_fin >= %s
              AND estado = 'VIGENTE';
        """
        parametros_q1 = [
            (p, fecha, fecha)
            for p, fecha in parametros
        ]
        tiempos, crudos = medir(
            cur, "Q1", escenario, parametros_q1, sql
        )
        resultados.append(("Q1", escenario, tiempos))
        crudos_totales.extend(crudos)

        # Q2 — precio y vigencia de 20 SKU negociados.
        parametros_q2 = [
            (
                d["proveedor"],
                d["skus"],
                "2026-09-18",
                "2026-09-18",
            )
            for d in base
        ]
        sql = """
            SELECT sku_codigo, precio, fecha_inicio, fecha_fin
            FROM catalogo.catalogo_sku
            WHERE proveedor_id = %s
              AND sku_codigo = ANY(%s)
              AND fecha_inicio <= %s
              AND fecha_fin >= %s;
        """
        tiempos, crudos = medir(
            cur, "Q2", escenario, parametros_q2, sql
        )
        resultados.append(("Q2", escenario, tiempos))
        crudos_totales.extend(crudos)

        # Q3 — franjas disponibles por CEDI y fecha.
        parametros_q3 = [
            (
                rng.randint(1, 5),
                f"2026-{rng.choice(['09', '10', '11'])}-{rng.randint(18, 28):02d}",
            )
            for _ in range(REPETICIONES)
        ]
        sql = """
            SELECT id, hora_inicio, hora_fin
            FROM logistica.franjas_descargue
            WHERE cedi_id = %s
              AND fecha = %s
              AND disponible = TRUE
            ORDER BY hora_inicio;
        """
        tiempos, crudos = medir(
            cur, "Q3", escenario, parametros_q3, sql
        )
        resultados.append(("Q3", escenario, tiempos))
        crudos_totales.extend(crudos)

        # Q4 — agregacion mensual disponible en el dataset.
        #
        # El dataset no contiene entrega real, fecha prometida ni cantidad
        # entregada. Por tanto no permite calcular OTIF/fill rate logistico
        # real. Esta consulta mide el recorrido analitico de 24 meses sobre
        # ordenes + lineas del mismo modulo.
        parametros_q4 = [
            (d["proveedor"],)
            for d in base
        ]
        sql = """
            SELECT
                date_trunc('month', o.fecha_orden) AS mes,
                COUNT(DISTINCT o.id) AS ordenes,
                COUNT(l.id) AS lineas,
                COALESCE(SUM(l.cantidad), 0) AS unidades_solicitadas
            FROM ordenes.ordenes o
            LEFT JOIN ordenes.lineas_orden l
                ON l.orden_id = o.id
            WHERE o.proveedor_id = %s
            GROUP BY date_trunc('month', o.fecha_orden)
            ORDER BY mes;
        """
        tiempos, crudos = medir(
            cur, "Q4", escenario, parametros_q4, sql
        )
        resultados.append(("Q4", escenario, tiempos))
        crudos_totales.extend(crudos)

        # Q5 — ultimas 50 ordenes, paginacion por cursor.
        parametros_q5 = [
            (d["proveedor"],)
            for d in base
        ]
        sql = """
            SELECT id, contrato_id, fecha_orden, estado
            FROM ordenes.ordenes
            WHERE proveedor_id = %s
            ORDER BY fecha_orden DESC
            LIMIT 50;
        """
        tiempos, crudos = medir(
            cur, "Q5", escenario, parametros_q5, sql
        )
        resultados.append(("Q5", escenario, tiempos))
        crudos_totales.extend(crudos)

    with ARCHIVO_CRUDO.open("w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow([
            "consulta",
            "escenario",
            "ejecucion",
            "tiempo_ms",
            "parametros",
        ])
        for consulta, escenario, ejecucion, tiempo_ms, parametros in crudos_totales:
            writer.writerow([
                consulta,
                escenario,
                ejecucion,
                f"{tiempo_ms:.6f}",
                parametros,
            ])

    total_filas = sum(volumen.values())

    with ARCHIVO_RESUMEN.open("w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow([
            "consulta",
            "escenario",
            "ejecuciones",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "volumen_total_filas",
            "seed",
        ])

        for consulta, escenario, tiempos in resultados:
            p50 = percentil(tiempos, 0.50)
            p95 = percentil(tiempos, 0.95)
            p99 = percentil(tiempos, 0.99)

            writer.writerow([
                consulta,
                escenario,
                len(tiempos),
                f"{p50:.6f}",
                f"{p95:.6f}",
                f"{p99:.6f}",
                total_filas,
                SEED,
            ])

            print(
                f"{consulta} {escenario:<9} "
                f"p50={p50:.3f} ms  "
                f"p95={p95:.3f} ms  "
                f"p99={p99:.3f} ms"
            )

    q1 = [
        tiempos
        for consulta, escenario, tiempos in resultados
        if consulta == "Q1"
        for tiempos in tiempos
    ]

    print()
    print(f"Volumen total: {total_filas} filas")
    print(f"Q1 p99 global: {percentil(q1, 0.99):.3f} ms")
    print(
        "Q1 < 10 ms: "
        + ("PASA" if percentil(q1, 0.99) < 10 else "FALLA")
    )
    print(f"Resumen: {ARCHIVO_RESUMEN}")
    print(f"Crudo:   {ARCHIVO_CRUDO}")
    print()
    print(
        "NOTA Q4: el dataset actual no contiene entrega real, fecha "
        "prometida ni cantidad entregada; no permite derivar OTIF/fill "
        "rate logistico real."
    )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
