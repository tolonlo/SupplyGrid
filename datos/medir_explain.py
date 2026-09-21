#!/usr/bin/env python3
"""
datos/medir_explain.py

Ejecuta EXPLAIN (ANALYZE, BUFFERS, VERBOSE) para las consultas Q4 y Q5
antes y después de sus respectivos índices, tanto en escenario frío como
caliente, y evalúa alternativas técnicas de indexación.

Todas las pruebas de modificación de índices se ejecutan dentro de
transacciones con ROLLBACK, de modo que el estado físico de la base
de datos no se altera.
"""
import os
import sys
import psycopg2


def conectar():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
        dbname=os.environ.get("PGDATABASE", "supplygrid_db"),
    )


SQL_Q5 = """
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT id, proveedor_id, contrato_id, fecha_orden, estado, idempotency_key
FROM ordenes.ordenes
WHERE proveedor_id = %s
ORDER BY fecha_orden DESC
LIMIT 50;
"""

SQL_Q4 = """
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    date_trunc('month', o.fecha_orden) AS mes,
    count(DISTINCT o.id) AS total_ordenes,
    count(l.id) AS total_lineas,
    sum(l.cantidad) AS total_unidades,
    round(avg(l.cantidad), 2) AS promedio_unidades_linea
FROM ordenes.ordenes o
JOIN ordenes.lineas_orden l ON l.orden_id = o.id
WHERE o.proveedor_id = %s
  AND o.fecha_orden >= '2024-09-01' AND o.fecha_orden <= '2026-09-18'
GROUP BY date_trunc('month', o.fecha_orden)
ORDER BY mes;
"""


def ejecutar_plan(cur, sql, params):
    cur.execute(sql, params)
    return "\n".join(r[0] for r in cur.fetchall())


def medir_q5(conn, out_dir):
    print("--> Midiendo Q5 (Últimas 50 órdenes paginadas)...")
    cur = conn.cursor()

    # 1. DESPUÉS: Con el índice compuesto oficial (proveedor_id, fecha_orden DESC)
    # Caliente (proveedor 3)
    plan_despues_caliente = ejecutar_plan(cur, SQL_Q5, (3,))
    # Frío (proveedor 14000)
    plan_despues_frio = ejecutar_plan(cur, SQL_Q5, (14000,))

    with open(os.path.join(out_dir, "explain_q5_despues.txt"), "w", encoding="utf-8") as f:
        f.write("=== EXPLAIN (ANALYZE, BUFFERS) - Q5 DESPUÉS (Con idx_ordenes_proveedor_fecha) ===\n\n")
        f.write("--- ESCENARIO CALIENTE (proveedor_id = 3, top 1% hot partition) ---\n")
        f.write(plan_despues_caliente + "\n\n")
        f.write("--- ESCENARIO FRÍO (proveedor_id = 14000, cola larga) ---\n")
        f.write(plan_despues_frio + "\n")

    # 2. ANTES: Sin índice compuesto idx_ordenes_proveedor_fecha (Seq Scan baseline)
    cur.execute("BEGIN;")
    cur.execute("DROP INDEX IF EXISTS ordenes.idx_ordenes_proveedor_fecha;")
    cur.execute("DROP INDEX IF EXISTS ordenes.idx_ordenes_fecha;")
    plan_antes_caliente = ejecutar_plan(cur, SQL_Q5, (3,))
    plan_antes_frio = ejecutar_plan(cur, SQL_Q5, (14000,))
    cur.execute("ROLLBACK;")

    with open(os.path.join(out_dir, "explain_q5_antes.txt"), "w", encoding="utf-8") as f:
        f.write("=== EXPLAIN (ANALYZE, BUFFERS) - Q5 ANTES (Sin índices en ordenes.ordenes) ===\n\n")
        f.write("--- ESCENARIO CALIENTE (proveedor_id = 3, top 1% hot partition) ---\n")
        f.write(plan_antes_caliente + "\n\n")
        f.write("--- ESCENARIO FRÍO (proveedor_id = 14000, cola larga) ---\n")
        f.write(plan_antes_frio + "\n")

    # 3. ALTERNATIVAS para Q5
    # Alternativa A: Solo índice por fecha (idx_ordenes_fecha)
    cur.execute("BEGIN;")
    cur.execute("DROP INDEX IF EXISTS ordenes.idx_ordenes_proveedor_fecha;")
    plan_alt_fecha_caliente = ejecutar_plan(cur, SQL_Q5, (3,))
    plan_alt_fecha_frio = ejecutar_plan(cur, SQL_Q5, (14000,))
    cur.execute("ROLLBACK;")

    # Alternativa B: Solo índice por proveedor (sin fecha)
    cur.execute("BEGIN;")
    cur.execute("DROP INDEX IF EXISTS ordenes.idx_ordenes_proveedor_fecha;")
    cur.execute("DROP INDEX IF EXISTS ordenes.idx_ordenes_fecha;")
    cur.execute("CREATE INDEX idx_alt_solo_proveedor ON ordenes.ordenes (proveedor_id);")
    plan_alt_proveedor_caliente = ejecutar_plan(cur, SQL_Q5, (3,))
    plan_alt_proveedor_frio = ejecutar_plan(cur, SQL_Q5, (14000,))
    cur.execute("ROLLBACK;")

    with open(os.path.join(out_dir, "explain_q5_alternativas.txt"), "w", encoding="utf-8") as f:
        f.write("=== EXPLAIN (ANALYZE, BUFFERS) - Q5 ALTERNATIVAS DE ÍNDICE ===\n\n")
        f.write("--- ALTERNATIVA A: Índice solo en (fecha_orden) ---\n")
        f.write("[Caliente - proveedor 3]:\n" + plan_alt_fecha_caliente + "\n\n")
        f.write("[Frío - proveedor 14000]:\n" + plan_alt_fecha_frio + "\n\n")
        f.write("--- ALTERNATIVA B: Índice solo en (proveedor_id) ---\n")
        f.write("[Caliente - proveedor 3]:\n" + plan_alt_proveedor_caliente + "\n\n")
        f.write("[Frío - proveedor 14000]:\n" + plan_alt_proveedor_frio + "\n")

    cur.close()
    print("    -> Guardados explain_q5_antes.txt, explain_q5_despues.txt, explain_q5_alternativas.txt")


def medir_q4(conn, out_dir):
    print("--> Midiendo Q4 (OTIF y fill rate sobre 24 meses)...")
    cur = conn.cursor()

    # 1. DESPUÉS: Con el índice oficial idx_lineas_orden_id ON ordenes.lineas_orden (orden_id)
    plan_despues_caliente = ejecutar_plan(cur, SQL_Q4, (3,))
    plan_despues_frio = ejecutar_plan(cur, SQL_Q4, (14000,))

    with open(os.path.join(out_dir, "explain_q4_despues.txt"), "w", encoding="utf-8") as f:
        f.write("=== EXPLAIN (ANALYZE, BUFFERS) - Q4 DESPUÉS (Con idx_lineas_orden_id) ===\n\n")
        f.write("--- ESCENARIO CALIENTE (proveedor_id = 3, 5.000 órdenes) ---\n")
        f.write(plan_despues_caliente + "\n\n")
        f.write("--- ESCENARIO FRÍO (proveedor_id = 14000, 21 órdenes) ---\n")
        f.write(plan_despues_frio + "\n")

    # 2. ANTES: Sin índice idx_lineas_orden_id (Seq Scan de 3M filas + Hash Join)
    cur.execute("BEGIN;")
    cur.execute("DROP INDEX IF EXISTS ordenes.idx_lineas_orden_id;")
    plan_antes_caliente = ejecutar_plan(cur, SQL_Q4, (3,))
    plan_antes_frio = ejecutar_plan(cur, SQL_Q4, (14000,))
    cur.execute("ROLLBACK;")

    with open(os.path.join(out_dir, "explain_q4_antes.txt"), "w", encoding="utf-8") as f:
        f.write("=== EXPLAIN (ANALYZE, BUFFERS) - Q4 ANTES (Sin idx_lineas_orden_id) ===\n\n")
        f.write("--- ESCENARIO CALIENTE (proveedor_id = 3, 5.000 órdenes) ---\n")
        f.write(plan_antes_caliente + "\n\n")
        f.write("--- ESCENARIO FRÍO (proveedor_id = 14000, 21 órdenes) ---\n")
        f.write(plan_antes_frio + "\n")

    # 3. ALTERNATIVAS para Q4
    # Alternativa: Índice compuesto cubriente en (orden_id, cantidad)
    cur.execute("BEGIN;")
    cur.execute("DROP INDEX IF EXISTS ordenes.idx_lineas_orden_id;")
    cur.execute("CREATE INDEX idx_alt_lineas_cubriente ON ordenes.lineas_orden (orden_id, cantidad);")
    plan_alt_cubriente_caliente = ejecutar_plan(cur, SQL_Q4, (3,))
    plan_alt_cubriente_frio = ejecutar_plan(cur, SQL_Q4, (14000,))
    cur.execute("ROLLBACK;")

    with open(os.path.join(out_dir, "explain_q4_alternativas.txt"), "w", encoding="utf-8") as f:
        f.write("=== EXPLAIN (ANALYZE, BUFFERS) - Q4 ALTERNATIVAS DE ÍNDICE ===\n\n")
        f.write("--- ALTERNATIVA: Índice compuesto cubriente (orden_id, cantidad) ---\n")
        f.write("[Caliente - proveedor 3]:\n" + plan_alt_cubriente_caliente + "\n\n")
        f.write("[Frío - proveedor 14000]:\n" + plan_alt_cubriente_frio + "\n")

    cur.close()
    print("    -> Guardados explain_q4_antes.txt, explain_q4_despues.txt, explain_q4_alternativas.txt")


def main():
    out_dir = os.path.join(os.path.dirname(__file__), "salida")
    os.makedirs(out_dir, exist_ok=True)

    print("Conectando a PostgreSQL para capturar planes de ejecución...")
    conn = conectar()

    try:
        medir_q5(conn, out_dir)
        medir_q4(conn, out_dir)
        print("\nTodas las mediciones de EXPLAIN se completaron exitosamente.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
