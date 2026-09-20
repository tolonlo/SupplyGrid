#!/usr/bin/env bash
# datos/cargar.sh — E2-04
# Uso:  bash datos/cargar.sh [bench|carga|todo]      (default: todo)
#   bench -> Fase 3: 4 estrategias sobre el mismo subconjunto de N_SUB ordenes
#   carga -> Fase 2: COPY de las 7 tablas, indices despues, ANALYZE
# Requisito para bench: la app corriendo en APP_URL (estrategia 4).
# Para probar con pocos datos:  N_SUB=8000 bash datos/cargar.sh todo
set -euo pipefail
cd "$(dirname "$0")/.."

DIR=datos/salida
SUBCONJUNTO=$DIR/subconjunto_ordenes.csv
N_SUB=${N_SUB:-50000}
APP_URL=${APP_URL:-http://localhost:8080}
T_TABLAS=$DIR/tiempos_carga_por_tabla.csv
T_ESTRAT=$DIR/comparacion_estrategias.csv

export PGHOST=${PGHOST:-localhost} PGPORT=${PGPORT:-5432}
export PGUSER=${PGUSER:-postgres} PGPASSWORD=${PGPASSWORD:-postgres}
export PGDATABASE=${PGDATABASE:-supplygrid_db}
# Si no tienes psql instalado, usa el del contenedor:
#   export PSQL="docker exec -i supplygrid_postgres psql -U postgres -d supplygrid_db -X -q -v ON_ERROR_STOP=1"
PSQL=${PSQL:-psql -X -q -v ON_ERROR_STOP=1}

ahora() { date +%s.%N; }
dur()   { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", b-a}'; }
tasa()  { awk -v n="$1" -v s="$2" 'BEGIN{printf "%.0f", (s>0)? n/s : 0}'; }

COLS_ORD="id,proveedor_id,contrato_id,fecha_orden,estado,idempotency_key"

# ---------------------------------------------------------------- indices
# Los 7 indices secundarios de V1 (PK y UNIQUE se quedan).
drop_indices() {
  $PSQL -c "
    DROP INDEX IF EXISTS proveedores.idx_contratos_proveedor_vigencia;
    DROP INDEX IF EXISTS catalogo.idx_catalogo_proveedor_sku;
    DROP INDEX IF EXISTS ordenes.idx_ordenes_proveedor_fecha;
    DROP INDEX IF EXISTS ordenes.idx_ordenes_fecha;
    DROP INDEX IF EXISTS ordenes.idx_lineas_orden_id;
    DROP INDEX IF EXISTS logistica.idx_franjas_cedi_fecha_disponible;
    DROP INDEX IF EXISTS auditoria.idx_eventos_fecha;"
}
create_indices() {
  $PSQL -c "
    CREATE INDEX IF NOT EXISTS idx_contratos_proveedor_vigencia
      ON proveedores.contratos (proveedor_id, fecha_inicio, fecha_fin);
    CREATE INDEX IF NOT EXISTS idx_catalogo_proveedor_sku
      ON catalogo.catalogo_sku (proveedor_id, sku_codigo);
    CREATE INDEX IF NOT EXISTS idx_ordenes_proveedor_fecha
      ON ordenes.ordenes (proveedor_id, fecha_orden DESC);
    CREATE INDEX IF NOT EXISTS idx_ordenes_fecha
      ON ordenes.ordenes (fecha_orden);
    CREATE INDEX IF NOT EXISTS idx_lineas_orden_id
      ON ordenes.lineas_orden (orden_id);
    CREATE INDEX IF NOT EXISTS idx_franjas_cedi_fecha_disponible
      ON logistica.franjas_descargue (cedi_id, fecha) WHERE disponible = TRUE;
    CREATE INDEX IF NOT EXISTS idx_eventos_fecha
      ON auditoria.eventos (fecha_evento);"
}

# ------------------------------------------------- FASE 3: 4 estrategias
reset_ordenes() {
  $PSQL -c "TRUNCATE ordenes.lineas_orden, ordenes.ordenes RESTART IDENTITY;"
}

gen_sql() {  # $1 = fila | lote ; imprime SQL a stdout
  python3 - "$1" "$SUBCONJUNTO" <<'PY'
import csv, sys
modo, ruta = sys.argv[1], sys.argv[2]
with open(ruta, newline="", encoding="utf-8") as f:
    filas = list(csv.reader(f))[1:]
cols = "id,proveedor_id,contrato_id,fecha_orden,estado,idempotency_key"
v = lambda r: "(%s,%s,%s,'%s','%s','%s')" % tuple(r)
if modo == "fila":
    for r in filas:
        print(f"INSERT INTO ordenes.ordenes ({cols}) VALUES {v(r)};")
else:
    print("BEGIN;")
    for i in range(0, len(filas), 1000):
        print(f"INSERT INTO ordenes.ordenes ({cols}) VALUES "
              + ",".join(v(r) for r in filas[i:i+1000]) + ";")
    print("COMMIT;")
PY
}

est_copy()  { $PSQL -c "COPY ordenes.ordenes ($COLS_ORD) FROM STDIN WITH (FORMAT csv, HEADER true)" < "$SUBCONJUNTO"; }
est_lotes() { $PSQL -f "$DIR/.lotes.sql"; }      # 1 transaccion, lotes de 1000 filas
est_fila()  { $PSQL -f "$DIR/.filafila.sql"; }   # autocommit: 1 commit (fsync) por fila
est_app()   { python3 datos/bench_app.py "$SUBCONJUNTO" "$APP_URL"; }

medir() {  # $1 = nombre, resto = comando
  local nombre=$1; shift
  reset_ordenes
  local t0 t1 s
  t0=$(ahora); "$@"; t1=$(ahora)
  s=$(dur "$t0" "$t1")
  echo "$nombre,$N_SUB,$s,$(tasa "$N_SUB" "$s")" >> "$T_ESTRAT"
  printf '  %-34s %8s s   %8s filas/s\n' "$nombre" "$s" "$(tasa "$N_SUB" "$s")"
}

fase3_bench() {
  echo "== FASE 3: comparacion de 4 estrategias (mismo subconjunto de $N_SUB ordenes) =="
  local total; total=$(( $(wc -l < "$DIR/ordenes.csv") - 1 ))
  [ "$total" -ge "$N_SUB" ] || { echo "ordenes.csv tiene $total filas y se necesitan >= $N_SUB (E2-02b). Prueba con N_SUB=$total"; exit 1; }
    code=$(curl -s -o /dev/null -w '%{http_code}' "$APP_URL/actuator/health" || true)
  [ "$code" != "000" ] && [ -n "$code" ] \
    || { echo "La app no responde en $APP_URL (levantala: ./mvnw spring-boot:run)"; exit 1; }
  head -n $((N_SUB + 1)) "$DIR/ordenes.csv" > "$SUBCONJUNTO"
  gen_sql fila > "$DIR/.filafila.sql"     # se generan ANTES de cronometrar
  gen_sql lote > "$DIR/.lotes.sql"

  echo "estrategia,filas,segundos,filas_por_segundo" > "$T_ESTRAT"
  # Misma tabla, mismos indices, mismo estado inicial (vacia) para las 4.
  medir "1_COPY_masivo"                   est_copy
  medir "2_INSERT_lotes_1000_una_tx"      est_lotes
  medir "3_INSERT_fila_autocommit"        est_fila
  medir "4_ruta_transaccional_app"        est_app
  reset_ordenes
  rm -f "$DIR/.filafila.sql" "$DIR/.lotes.sql"
  echo "-> $T_ESTRAT"
}

# ------------------------------------------------ FASE 2: carga completa
cargar_tabla() {  # tabla | columnas | archivo
  local tabla=$1 cols=$2 archivo=$3 t0 t1 s n
  n=$(( $(wc -l < "$DIR/$archivo") - 1 ))
  t0=$(ahora)
  $PSQL -c "COPY $tabla ($cols) FROM STDIN WITH (FORMAT csv, HEADER true)" < "$DIR/$archivo"
  t1=$(ahora); s=$(dur "$t0" "$t1")
  echo "$tabla,$n,$s,$(tasa "$n" "$s")" >> "$T_TABLAS"
  printf '  %-32s %9s filas %8s s %9s filas/s\n' "$tabla" "$n" "$s" "$(tasa "$n" "$s")"
}

fase2_carga() {
  echo "== FASE 2: carga masiva con COPY =="
  echo "tabla,filas,segundos,filas_por_segundo" > "$T_TABLAS"
  local T0 T1
  T0=$(ahora)
  $PSQL -c "TRUNCATE proveedores.contratos, proveedores.proveedores, catalogo.catalogo_sku,
            ordenes.lineas_orden, ordenes.ordenes, logistica.franjas_descargue,
            auditoria.eventos RESTART IDENTITY CASCADE;"
  drop_indices

  # Orden que respeta las FK internas de cada esquema.
  cargar_tabla proveedores.proveedores     "id,nombre,nit,ciudad,fecha_registro,activo"                                        proveedores.csv
  cargar_tabla proveedores.contratos       "id,proveedor_id,fecha_inicio,fecha_fin,estado"                                     contratos.csv
  cargar_tabla catalogo.catalogo_sku       "id,proveedor_id,contrato_id,sku_codigo,descripcion,precio,fecha_inicio,fecha_fin"  catalogo_sku.csv
  cargar_tabla logistica.franjas_descargue "id,cedi_id,fecha,hora_inicio,hora_fin,disponible,orden_id"                         franjas_descargue.csv
  cargar_tabla ordenes.ordenes             "$COLS_ORD"                                                                         ordenes.csv
  cargar_tabla ordenes.lineas_orden        "id,orden_id,sku_codigo,cantidad,precio_unitario"                                   lineas_orden.csv
  cargar_tabla auditoria.eventos           "id,entidad,entidad_id,tipo_evento,fecha_evento,detalle"                            eventos.csv

  # Indices DESPUES de la carga (cronometrado).
  local i0 i1; i0=$(ahora); create_indices; i1=$(ahora)
  echo "creacion_de_7_indices,,$(dur "$i0" "$i1")," >> "$T_TABLAS"
  printf '  %-32s %28s s\n' "creacion de indices" "$(dur "$i0" "$i1")"

  # COPY con IDs explicitos NO mueve las secuencias: sin esto la Fase 4 choca.
  for t in proveedores.proveedores proveedores.contratos catalogo.catalogo_sku \
           logistica.franjas_descargue ordenes.ordenes ordenes.lineas_orden auditoria.eventos; do
    $PSQL -c "SELECT setval(pg_get_serial_sequence('$t','id'),
                            COALESCE((SELECT max(id) FROM $t),0)+1, false);" >/dev/null
  done

  $PSQL -c "ANALYZE;"   # sin esto el planeador miente
  T1=$(ahora)
  echo "TOTAL,,$(dur "$T0" "$T1")," >> "$T_TABLAS"
  echo "  TOTAL (incluye indices + ANALYZE): $(dur "$T0" "$T1") s"
  echo "-> $T_TABLAS"
}

case "${1:-todo}" in
  bench) fase3_bench ;;
  carga) fase2_carga ;;
  todo)  fase3_bench; fase2_carga ;;   # bench primero; la carga completa hace TRUNCATE y deja todo limpio
  *) echo "uso: $0 [bench|carga|todo]"; exit 1 ;;
esac