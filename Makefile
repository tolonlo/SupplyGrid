.PHONY: datos generar bench cargar verificar db-reset

datos: generar cargar verificar

generar:
	python3 datos/generar.py --seed 42 --out datos/salida

bench:
	bash datos/cargar.sh bench

cargar:
	bash datos/cargar.sh carga

verificar:
	python3 datos/verificar.py

# Deja los 4 esquemas vacios sin borrar la base completa. Necesita la
# app APAGADA (docker compose stop app) para no chocar con Flyway.
db-reset:
	psql -c "TRUNCATE proveedores.contratos, proveedores.proveedores, catalogo.catalogo_sku, \
	          ordenes.lineas_orden, ordenes.ordenes, logistica.franjas_descargue, \
	          auditoria.eventos RESTART IDENTITY CASCADE;"