.PHONY: datos generar bench cargar verificar db-reset

PSQL ?= psql

datos: db-reset generar cargar verificar

generar:
	python3 datos/generar.py --seed 42 --out datos/salida

bench:
	PSQL="$(PSQL)" bash datos/cargar.sh bench

cargar:
	PSQL="$(PSQL)" bash datos/cargar.sh carga

verificar:
	python3 datos/verificar.py

# Deja los cuatro modulos y el esquema transversal vacios sin borrar la base completa.
db-reset:
	$(PSQL) -c "TRUNCATE proveedores.contratos, proveedores.proveedores, catalogo.catalogo_sku, \
	          ordenes.lineas_orden, ordenes.ordenes, logistica.franjas_descargue, \
	          auditoria.eventos RESTART IDENTITY CASCADE;"