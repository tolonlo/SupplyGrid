#!/usr/bin/env python3
"""
datos/verificar.py — E2-07 (Tom)

Revisa lo que pide la Fase 5 del enunciado y para cada cosa imprime
PASA o FALLA. Se conecta a la misma base que usa cargar.sh, con las
mismas variables de entorno (PGHOST, PGPORT, PGUSER, PGPASSWORD,
PGDATABASE) — psycopg2 las lee solo, no hay que pasarlas a mano.

Uso:
    python3 datos/verificar.py
"""
import hashlib
import os
import sys

import psycopg2

# Volumen MINIMO exigido por el enunciado (tabla 2.1). Cuando Ema
# entregue el generador final (E2-02b) con el volumen objetivo real,
# este script no cambia: solo debe empezar a decir PASA en vez de FALLA.
MINIMOS = {
    ("proveedores", "proveedores"): 100_000,
    ("proveedores", "contratos"): 10_000,
    ("catalogo", "catalogo_sku"): 200_000,
    ("ordenes", "ordenes"): 300_000,
    ("ordenes", "lineas_orden"): 1_500_000,
    ("logistica", "franjas_descargue"): 90_000,
    ("auditoria", "eventos"): 500_000,
}


def conectar():
    try:
        return psycopg2.connect(
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", "postgres"),
            dbname=os.environ.get("PGDATABASE", "supplygrid_db"),
        )
    except Exception as e:
        print(f"No se pudo conectar a la base de datos: {e}")
        sys.exit(1)


def verificar_volumen(cur):
    print("\n== Volumen por tabla (minimo exigido) ==")
    todo_paso = True
    for (esquema, tabla), minimo in MINIMOS.items():
        cur.execute(f"SELECT count(*) FROM {esquema}.{tabla};")
        conteo = cur.fetchone()[0]
        veredicto = "PASA" if conteo >= minimo else "FALLA"
        if veredicto == "FALLA":
            todo_paso = False
        print(f"  {esquema}.{tabla:<20} {conteo:>10} filas  (minimo {minimo:>10})  -> {veredicto}")
    return todo_paso


def verificar_distribucion(cur):
    print("\n== Distribucion acumulada (sesgo Zipf esperado) ==")
    cur.execute("SELECT count(*) FROM ordenes.ordenes;")
    total_ordenes = cur.fetchone()[0]
    if total_ordenes == 0:
        print("  No hay ordenes cargadas todavia.")
        return

    cur.execute("""
        SELECT proveedor_id, count(*) AS n
        FROM ordenes.ordenes
        GROUP BY proveedor_id
        ORDER BY n DESC;
    """)
    conteos = [r[1] for r in cur.fetchall()]
    n_proveedores = len(conteos)

    for pct in (1, 5, 20):
        top_n = max(1, int(n_proveedores * pct / 100))
        ordenes_top = sum(conteos[:top_n])
        porcentaje = 100 * ordenes_top / total_ordenes
        print(f"  Top {pct:>2}% de proveedores ({top_n} de {n_proveedores}) concentra {porcentaje:.1f}% de las ordenes")


def ordenes_por_dia(cur):
    print("\n== Ordenes por dia, ultimos 60 dias ==")
    cur.execute("""
        SELECT fecha_orden::date AS dia, count(*) AS n
        FROM ordenes.ordenes
        WHERE fecha_orden >= now() - interval '60 days'
        GROUP BY dia
        ORDER BY dia;
    """)
    filas = cur.fetchall()
    if not filas:
        print("  Sin ordenes en los ultimos 60 dias (revisar rango de fechas del generador).")
        return
    for dia, n in filas:
        print(f"  {dia}  {n:>6} ordenes")


def verificar_casos_borde():
    print("\n== Casos borde (8 exigidos) ==")
    resultado = True
    try:
        with open("datos/salida/casos-borde.txt", encoding="utf-8") as f:
            lineas = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        if len(lineas) == 8:
            print("  8 casos borde documentados -> PASA")
        else:
            resultado = False
            print(f"  {len(lineas)} de 8 casos borde documentados -> FALLA")
        for l in lineas:
            print(f"    {l}")
    except FileNotFoundError:
        resultado = False
        print("  casos-borde.txt no existe todavia (lo agrega Ema en E2-02b) -> PENDIENTE")
    return resultado


def verificar_casos_en_datos(cur):
    print("\n== Casos borde sembrados en datos ==")
    casos = {
        "orden de 300 lineas": "SELECT count(*) = 300 FROM ordenes.lineas_orden WHERE orden_id = 1",
        "contrato vencido ayer": "SELECT COALESCE((SELECT fecha_fin::date = (SELECT fecha_fin::date FROM proveedores.contratos WHERE id = 2) - 1 FROM proveedores.contratos WHERE id = 1), FALSE)",
        "contrato vence hoy": "SELECT COALESCE((SELECT fecha_fin::time = TIME '12:00:00' FROM proveedores.contratos WHERE id = 2), FALSE)",
        "ultima franja CEDI": "SELECT COALESCE((SELECT disponible AND cedi_id = 1 AND hora_inicio = TIME '17:00:00' FROM logistica.franjas_descargue WHERE id = 1), FALSE)",
        "proveedor sin contrato": "SELECT NOT EXISTS (SELECT 1 FROM proveedores.contratos WHERE proveedor_id = 100000)",
        "SKU fuera del catalogo": "SELECT EXISTS (SELECT 1 FROM ordenes.lineas_orden WHERE sku_codigo = 'SKU-EDGE-FUERA-CATALOGO')",
        "proveedor hot": "SELECT count(*) >= 5000 FROM ordenes.ordenes WHERE proveedor_id = 3",
        "mes sin ordenes": "SELECT NOT EXISTS (SELECT 1 FROM ordenes.ordenes WHERE proveedor_id = 15000 AND fecha_orden >= TIMESTAMP '2026-08-01' AND fecha_orden < TIMESTAMP '2026-09-01')",
    }
    resultado = True
    for nombre, consulta in casos.items():
        cur.execute(consulta)
        pasa = bool(cur.fetchone()[0])
        resultado = resultado and pasa
        print(f"  {nombre:<28} -> {'PASA' if pasa else 'FALLA'}")
    return resultado


def checksum_determinismo(cur):
    print("\n== Checksum de determinismo ==")
    cur.execute("""
        SELECT idempotency_key FROM ordenes.ordenes ORDER BY id;
    """)
    claves = "".join(r[0] for r in cur.fetchall())
    h = hashlib.md5(claves.encode("utf-8")).hexdigest()
    ruta = "datos/salida/checksum_ordenes.txt"
    try:
        with open(ruta, encoding="utf-8") as archivo:
            esperado = archivo.read().strip()
        pasa = h == esperado
        print(f"  md5(idempotency_key ordenadas por id) = {h} -> {'PASA' if pasa else 'FALLA'}")
        return pasa
    except FileNotFoundError:
        print(f"  md5(idempotency_key ordenadas por id) = {h} -> PENDIENTE (falta {ruta})")
        return False


def main():
    conn = conectar()
    cur = conn.cursor()

    volumen_ok = verificar_volumen(cur)
    verificar_distribucion(cur)
    ordenes_por_dia(cur)
    casos_documentados_ok = verificar_casos_borde()
    casos_datos_ok = verificar_casos_en_datos(cur)
    checksum_ok = checksum_determinismo(cur)

    print("\n== Resumen ==")
    todo_ok = volumen_ok and casos_documentados_ok and casos_datos_ok and checksum_ok
    print("Volumen minimo: " + ("PASA" if volumen_ok else "FALLA"))
    print("Verificacion completa: " + ("PASA" if todo_ok else "FALLA"))

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()