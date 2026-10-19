.PHONY: datos generar bench cargar verificar db-reset explain reconstruir-e2 verificar-e2 linea-base chaos-status chaos-kill chaos-reset test-chaos

PSQL ?= psql
PYTHON ?= python3

datos: db-reset generar cargar verificar

generar:
	$(PYTHON) datos/generar.py --seed 42 --out datos/salida

bench:
	PSQL="$(PSQL)" bash datos/cargar.sh bench

cargar:
	PSQL="$(PSQL)" bash datos/cargar.sh carga

verificar:
	$(PYTHON) datos/verificar.py

explain:
	$(PYTHON) datos/medir_explain.py

# Deja los cuatro modulos y el esquema transversal vacios sin borrar la base completa.
db-reset:
	$(PSQL) -c "TRUNCATE proveedores.contratos, proveedores.proveedores, catalogo.catalogo_sku, \
	          ordenes.lineas_orden, ordenes.ordenes, logistica.franjas_descargue, \
	          auditoria.eventos RESTART IDENTITY CASCADE;"

# --- Metas Entregable 3 (Fallas configurables + Momento 1 Reproducible) ---
reconstruir-e2:
	$(PYTHON) datos/reconstruir_e2.py --rebuild --seed 42 --clean

verificar-e2:
	$(PYTHON) datos/reconstruir_e2.py --verify

linea-base:
	$(PYTHON) datos/generar_linea_base.py

chaos-status:
	$(PYTHON) -m chaos.cli status

chaos-kill:
	$(PYTHON) -m chaos.cli kill

chaos-reset:
	$(PYTHON) -m chaos.cli reset

test-chaos:
	$(PYTHON) -m unittest tests/test_chaos_acceptance.py -v