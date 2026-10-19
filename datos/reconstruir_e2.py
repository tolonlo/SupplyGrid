#!/usr/bin/env python3
"""
datos/reconstruir_e2.py — Reconstrucción y Verificación Reproducible del E2 (Momento 1)
SupplyGrid · Entregable 3 · Aplicaciones y Sistemas Escalables · EAFIT

Provee:
  1. Reconstrucción determinista completa del dataset canónico del Entregable 2 (seed 42).
  2. Ingesta masiva y creación de índices secundarios en PostgreSQL.
  3. Verificación no destructiva de conteos, 8 casos borde y checksum determinista.
  4. Generación de manifest estructurado 'manifest_e2_referencia.json'.

Códigos de salida:
  0: Éxito total (todos los conteos, casos borde y checksums coinciden exactamente).
  2: Discrepancia detectada (conteos o checksum no coinciden con la referencia).
  1: Error de ejecución, conexión o argumentos inválidos.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import psycopg2

# Metadatos canónicos del Entregable 2
CANONICAL_SEED = 42
CANONICAL_COUNTS = {
    "proveedores.proveedores": 100_000,
    "proveedores.contratos": 15_000,
    "catalogo.catalogo_sku": 400_000,
    "logistica.franjas_descargue": 150_000,
    "ordenes.ordenes": 500_000,
    "ordenes.lineas_orden": 3_000_000,
    "auditoria.eventos": 800_000,
}
CANONICAL_TOTAL_ROWS = sum(CANONICAL_COUNTS.values())  # 4.965.000
HISTORICAL_E2_IDEMPOTENCY_MD5 = "1c1969f67843450e37fc74df05d87db1"

SALIDA_DIR = Path("datos/salida")
MANIFEST_PATH = SALIDA_DIR / "manifest_e2_referencia.json"

TABLE_COLUMNS = {
    "proveedores.proveedores": "id,nombre,nit,ciudad,fecha_registro,activo",
    "proveedores.contratos": "id,proveedor_id,fecha_inicio,fecha_fin,estado",
    "catalogo.catalogo_sku": "id,proveedor_id,contrato_id,sku_codigo,descripcion,precio,fecha_inicio,fecha_fin",
    "logistica.franjas_descargue": "id,cedi_id,fecha,hora_inicio,hora_fin,disponible,orden_id",
    "ordenes.ordenes": "id,proveedor_id,contrato_id,fecha_orden,estado,idempotency_key",
    "ordenes.lineas_orden": "id,orden_id,sku_codigo,cantidad,precio_unitario",
    "auditoria.eventos": "id,entidad,entidad_id,tipo_evento,fecha_evento,detalle",
}

CSV_FILES = {
    "proveedores.proveedores": "proveedores.csv",
    "proveedores.contratos": "contratos.csv",
    "catalogo.catalogo_sku": "catalogo_sku.csv",
    "logistica.franjas_descargue": "franjas_descargue.csv",
    "ordenes.ordenes": "ordenes.csv",
    "ordenes.lineas_orden": "lineas_orden.csv",
    "auditoria.eventos": "eventos.csv",
}

SECONDARY_INDEXES = [
    ("idx_contratos_proveedor_vigencia", "CREATE INDEX IF NOT EXISTS idx_contratos_proveedor_vigencia ON proveedores.contratos (proveedor_id, fecha_inicio, fecha_fin)"),
    ("idx_catalogo_proveedor_sku", "CREATE INDEX IF NOT EXISTS idx_catalogo_proveedor_sku ON catalogo.catalogo_sku (proveedor_id, sku_codigo)"),
    ("idx_ordenes_proveedor_fecha", "CREATE INDEX IF NOT EXISTS idx_ordenes_proveedor_fecha ON ordenes.ordenes (proveedor_id, fecha_orden DESC)"),
    ("idx_ordenes_fecha", "CREATE INDEX IF NOT EXISTS idx_ordenes_fecha ON ordenes.ordenes (fecha_orden)"),
    ("idx_lineas_orden_id", "CREATE INDEX IF NOT EXISTS idx_lineas_orden_id ON ordenes.lineas_orden (orden_id)"),
    ("idx_franjas_cedi_fecha_disponible", "CREATE INDEX IF NOT EXISTS idx_franjas_cedi_fecha_disponible ON logistica.franjas_descargue (cedi_id, fecha) WHERE disponible = TRUE"),
    ("idx_eventos_fecha", "CREATE INDEX IF NOT EXISTS idx_eventos_fecha ON auditoria.eventos (fecha_evento)"),
]


def conectar():
    """Conecta a PostgreSQL usando variables de entorno con defaults estándar."""
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
        dbname=os.environ.get("PGDATABASE", "supplygrid_db"),
    )


def obtener_conteos(cur) -> Dict[str, int]:
    """Obtiene el número exacto de filas por tabla."""
    conteos = {}
    for tabla in CANONICAL_COUNTS:
        cur.execute(f"SELECT COUNT(*) FROM {tabla};")
        conteos[tabla] = cur.fetchone()[0]
    return conteos


def drop_indices(cur):
    """Elimina índices secundarios para acelerar la carga masiva."""
    cur.execute("""
        DROP INDEX IF EXISTS proveedores.idx_contratos_proveedor_vigencia;
        DROP INDEX IF EXISTS catalogo.idx_catalogo_proveedor_sku;
        DROP INDEX IF EXISTS ordenes.idx_ordenes_proveedor_fecha;
        DROP INDEX IF EXISTS ordenes.idx_ordenes_fecha;
        DROP INDEX IF EXISTS ordenes.idx_lineas_orden_id;
        DROP INDEX IF EXISTS logistica.idx_franjas_cedi_fecha_disponible;
        DROP INDEX IF EXISTS auditoria.idx_eventos_fecha;
    """)


def create_indices(cur):
    """Recrea los índices secundarios optimizados tras la carga."""
    for nombre, ddl in SECONDARY_INDEXES:
        cur.execute(ddl)


def calcular_checksums(cur) -> Tuple[str, str, Dict[str, str]]:
    """
    Calcula:
      1. md5_idempotencia: Checksum histórico exacto de E2 sobre ordenes.idempotency_key ordenadas por id.
      2. sha256_global: Hash determinista de todas las 7 tablas ordenadas por ID.
      3. sha256_por_tabla: Subhashes por tabla para trazabilidad.
    """
    # 1. MD5 histórico de idempotency_keys
    cur.execute("SELECT idempotency_key FROM ordenes.ordenes ORDER BY id;")
    md5_hasher = hashlib.md5()
    while True:
        filas = cur.fetchmany(10_000)
        if not filas:
            break
        for r in filas:
            md5_hasher.update(r[0].encode("utf-8"))
    md5_idempotencia = md5_hasher.hexdigest()

    # 2. SHA-256 integral por tabla
    sha256_global = hashlib.sha256()
    sha256_por_tabla = {}

    for tabla, cols in TABLE_COLUMNS.items():
        table_hasher = hashlib.sha256()
        cur.execute(f"SELECT {cols} FROM {tabla} ORDER BY id;")
        while True:
            filas = cur.fetchmany(10_000)
            if not filas:
                break
            for r in filas:
                linea = "|".join("" if v is None else str(v) for v in r) + "\n"
                encoded = linea.encode("utf-8")
                table_hasher.update(encoded)
                sha256_global.update(encoded)

        h_tabla = table_hasher.hexdigest()
        sha256_por_tabla[tabla] = h_tabla

    return md5_idempotencia, sha256_global.hexdigest(), sha256_por_tabla


def verificar_casos_borde(cur) -> Dict[str, bool]:
    """Verifica los 8 casos borde sembrados en el dataset."""
    casos = {
        "CASO-01 orden_1_300_lineas": "SELECT count(*) = 300 FROM ordenes.lineas_orden WHERE orden_id = 1",
        "CASO-02 contrato_1_vencido_ayer": "SELECT COALESCE((SELECT fecha_fin::date = (SELECT fecha_fin::date FROM proveedores.contratos WHERE id = 2) - 1 FROM proveedores.contratos WHERE id = 1), FALSE)",
        "CASO-03 contrato_2_vence_hoy_mediodia": "SELECT COALESCE((SELECT fecha_fin::time = TIME '12:00:00' FROM proveedores.contratos WHERE id = 2), FALSE)",
        "CASO-04 franja_1_ultima_17_00": "SELECT COALESCE((SELECT disponible AND cedi_id = 1 AND hora_inicio = TIME '17:00:00' FROM logistica.franjas_descargue WHERE id = 1), FALSE)",
        "CASO-05 proveedor_sin_contrato": "SELECT NOT EXISTS (SELECT 1 FROM proveedores.contratos WHERE proveedor_id = 100000)",
        "CASO-06 sku_fuera_catalogo": "SELECT EXISTS (SELECT 1 FROM ordenes.lineas_orden WHERE sku_codigo = 'SKU-EDGE-FUERA-CATALOGO')",
        "CASO-07 proveedor_3_hot_5000_ordenes": "SELECT count(*) >= 5000 FROM ordenes.ordenes WHERE proveedor_id = 3",
        "CASO-08 proveedor_15000_mes_sin_ordenes": "SELECT NOT EXISTS (SELECT 1 FROM ordenes.ordenes WHERE proveedor_id = 15000 AND fecha_orden >= TIMESTAMP '2026-08-01' AND fecha_orden < TIMESTAMP '2026-09-01')",
    }
    resultados = {}
    for nombre, sql in casos.items():
        cur.execute(sql)
        resultados[nombre] = bool(cur.fetchone()[0])
    return resultados


def ejecutar_reconstruccion(seed: int, clean: bool, skip_generate: bool) -> int:
    print("=" * 75)
    print(f" RECONSTRUCCIÓN REPRODUCIBLE DE ESTADO E2 (SEED={seed})")
    print("=" * 75)

    if seed != CANONICAL_SEED:
        print(f"[AVISO DISCREPANCIA] La seed solicitada ({seed}) difiere de la canónica ({CANONICAL_SEED}).")
        print("                      Se generará con la seed indicada pero se advertirá en el manifest.")

    SALIDA_DIR.mkdir(parents=True, exist_ok=True)

    # Paso 1: Generar CSVs si no se salta
    if not skip_generate:
        print("\n[1/4] Generando CSVs deterministas por streaming...")
        t_gen_start = time.perf_counter()
        cmd = [
            sys.executable,
            "datos/generar.py",
            "--seed", str(seed),
            "--out", str(SALIDA_DIR),
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            print(f"[ERROR] Falló generar.py:\n{res.stderr}")
            return 1
        print(f"      Generación completada en {time.perf_counter() - t_gen_start:.2f} s.")
    else:
        print("\n[1/4] Omitiendo generación de CSVs (--skip-generate activo).")

    # Paso 2: Conectar y limpiar si corresponde
    print("\n[2/4] Preparando base de datos PostgreSQL...")
    conn = conectar()
    conn.autocommit = True
    cur = conn.cursor()

    if clean:
        print("      Ejecutando TRUNCATE limpio en todos los esquemas...")
        cur.execute("""
            TRUNCATE proveedores.contratos, proveedores.proveedores, catalogo.catalogo_sku,
                     ordenes.lineas_orden, ordenes.ordenes, logistica.franjas_descargue,
                     auditoria.eventos RESTART IDENTITY CASCADE;
        """)

    # Eliminar índices secundarios antes de cargar
    print("      Deshabilitando índices secundarios para carga óptima...")
    drop_indices(cur)

    # Paso 3: Carga masiva con COPY
    print("\n[3/4] Cargando tablas con COPY masivo...")
    t_carga_start = time.perf_counter()
    for tabla, archivo_nombre in CSV_FILES.items():
        ruta_csv = SALIDA_DIR / archivo_nombre
        if not ruta_csv.exists():
            print(f"[ERROR] No existe el archivo {ruta_csv}")
            return 1

        cols = TABLE_COLUMNS[tabla]
        t0 = time.perf_counter()
        with open(ruta_csv, "r", encoding="utf-8") as f:
            cur.copy_expert(f"COPY {tabla} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)", f)
        dt = time.perf_counter() - t0
        print(f"      {tabla:<32} cargada en {dt:6.2f} s")

    # Recrear índices y ajustar secuencias
    print("      Recreando los 7 índices secundarios optimizados...")
    t0_idx = time.perf_counter()
    create_indices(cur)
    print(f"      Índices listos en {time.perf_counter() - t0_idx:.2f} s")

    print("      Ajustando secuencias serial y ejecutando ANALYZE...")
    for t in CANONICAL_COUNTS:
        cur.execute(f"SELECT setval(pg_get_serial_sequence('{t}','id'), COALESCE((SELECT max(id) FROM {t}),0)+1, false);")
    cur.execute("ANALYZE;")
    print(f"      Carga masiva e indexación completa en {time.perf_counter() - t_carga_start:.2f} s.")

    # Paso 4: Verificación, conteos y checksums
    print("\n[4/4] Verificando conteos, casos borde y calculando checksums deterministas...")
    conteos = obtener_conteos(cur)
    casos = verificar_casos_borde(cur)
    md5_idemp, sha256_global, sha256_tablas = calcular_checksums(cur)

    cur.close()
    conn.close()

    # Comprobar conteos
    conteos_ok = all(conteos[t] == CANONICAL_COUNTS[t] for t in CANONICAL_COUNTS)
    casos_ok = all(casos.values())
    md5_ok = (md5_idemp == HISTORICAL_E2_IDEMPOTENCY_MD5)

    print("\n" + "-" * 75)
    print(" RESUMEN DE CONTEOS POR ENTIDAD:")
    for tabla, count in conteos.items():
        esperado = CANONICAL_COUNTS[tabla]
        match_str = "MATCH" if count == esperado else f"MISMATCH (esperado {esperado})"
        print(f"  {tabla:<32} : {count:>10} filas  [{match_str}]")
    print(f"  {'TOTAL':<32} : {sum(conteos.values()):>10} filas  [{'MATCH' if sum(conteos.values()) == CANONICAL_TOTAL_ROWS else 'MISMATCH'}]")

    print("\n RESUMEN DE CASOS BORDE:")
    for c_nombre, c_pasa in casos.items():
        print(f"  {c_nombre:<38} : {'PASA' if c_pasa else 'FALLA'}")

    print("\n CHECKSUMS DETERMINISTAS:")
    print(f"  MD5 Claves Idempotencia  : {md5_idemp} [{'PASA E2' if md5_ok else 'FALLA'}]")
    print(f"  SHA-256 Estado Integral  : {sha256_global}")
    print("-" * 75)

    # Generar manifest estructurado
    manifest = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "seed": seed,
        "is_canonical_seed": (seed == CANONICAL_SEED),
        "total_rows": sum(conteos.values()),
        "canonical_total_rows": CANONICAL_TOTAL_ROWS,
        "counts_by_table": conteos,
        "counts_match_canonical": conteos_ok,
        "edge_cases": casos,
        "edge_cases_all_pass": casos_ok,
        "checksums": {
            "historical_e2_idempotency_md5": md5_idemp,
            "idempotency_md5_matches_reference": md5_ok,
            "state_sha256_global": sha256_global,
            "sha256_by_table": sha256_tablas,
        },
        "environment": {
            "pghost": os.environ.get("PGHOST", "localhost"),
            "pgport": os.environ.get("PGPORT", "5432"),
            "pgdatabase": os.environ.get("PGDATABASE", "supplygrid_db"),
        },
    }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"\nManifest estructurado guardado en: {MANIFEST_PATH.resolve()}")

    if conteos_ok and casos_ok and md5_ok:
        print("\n[RESULTADO] RECONSTRUCCIÓN DEL E2 EXITOSA Y 100% REPRODUCIBLE.")
        return 0
    else:
        print("\n[RESULTADO] RECONSTRUCCIÓN CON DISCREPANCIAS FRENTE AL E2 CANÓNICO.")
        return 2


def ejecutar_verificacion() -> int:
    print("=" * 75)
    print(" VERIFICACIÓN NO DESTRUCTIVA DEL ESTADO ACTUAL DE SUPPLYGRID")
    print("=" * 75)

    try:
        conn = conectar()
        cur = conn.cursor()
    except Exception as exc:
        print(f"[ERROR] No se pudo conectar a PostgreSQL: {exc}")
        return 1

    conteos = obtener_conteos(cur)
    casos = verificar_casos_borde(cur)
    md5_idemp, sha256_global, sha256_tablas = calcular_checksums(cur)
    cur.close()
    conn.close()

    conteos_ok = all(conteos[t] == CANONICAL_COUNTS[t] for t in CANONICAL_COUNTS)
    casos_ok = all(casos.values())
    md5_ok = (md5_idemp == HISTORICAL_E2_IDEMPOTENCY_MD5)

    # Comparar con manifest de referencia si existe
    manifest_match = None
    if MANIFEST_PATH.exists():
        try:
            with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                ref_manifest = json.load(f)
            ref_sha256 = ref_manifest.get("checksums", {}).get("state_sha256_global")
            manifest_match = (sha256_global == ref_sha256)
        except Exception:
            manifest_match = False

    print("\n CONTEOS POR ENTIDAD:")
    for tabla, count in conteos.items():
        esperado = CANONICAL_COUNTS[tabla]
        match_str = "MATCH" if count == esperado else f"MISMATCH (esperado {esperado})"
        print(f"  {tabla:<32} : {count:>10} / {esperado:>10}  [{match_str}]")

    print("\n CASOS BORDE:")
    for c_nombre, c_pasa in casos.items():
        print(f"  {c_nombre:<38} : {'PASA' if c_pasa else 'FALLA'}")

    print("\n CHECKSUMS:")
    print(f"  MD5 Claves Idempotencia  : {md5_idemp} [{'PASA E2' if md5_ok else 'FALLA'}]")
    print(f"  SHA-256 Estado Actual    : {sha256_global}")
    if manifest_match is not None:
        print(f"  Coincide con Manifest    : {'SÍ (IDÉNTICO)' if manifest_match else 'NO (DISCREPANCIA)'}")
    print("-" * 75)

    todo_ok = conteos_ok and casos_ok and md5_ok and (manifest_match is not False)
    if todo_ok:
        print("[VEREDICTO] ESTADO DE DATOS VÁLIDO, CANÓNICO Y REPRODUCIBLE (EXIT CODE 0).")
        return 0
    else:
        print("[VEREDICTO] SE DETECTARON DISCREPANCIAS EN EL ESTADO (EXIT CODE 2).")
        return 2


def main():
    parser = argparse.ArgumentParser(
        description="Reconstrucción y Verificación Reproducible del E2 (Momento 1 · E3)"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--rebuild", action="store_true", help="Reconstruye el dataset completo con seed y carga limpia")
    group.add_argument("--verify", action="store_true", help="Verificación no destructiva del estado actual contra la referencia")

    parser.add_argument("--seed", type=int, default=CANONICAL_SEED, help=f"Semilla de generación (default: {CANONICAL_SEED})")
    parser.add_argument("--clean", action="store_true", default=True, help="Ejecuta TRUNCATE limpio antes de cargar (default: True)")
    parser.add_argument("--skip-generate", action="store_true", help="Reutiliza los CSVs existentes en datos/salida sin regenerar")

    args = parser.parse_args()

    if args.rebuild:
        code = ejecutar_reconstruccion(seed=args.seed, clean=args.clean, skip_generate=args.skip_generate)
        sys.exit(code)
    elif args.verify:
        code = ejecutar_verificacion()
        sys.exit(code)


if __name__ == "__main__":
    main()
