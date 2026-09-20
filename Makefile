.PHONY: datos generar bench cargar verificar

datos: generar cargar verificar

generar:
	python3 datos/generar.py --seed 42 --out datos/salida

bench:
	bash datos/cargar.sh bench

cargar:
	bash datos/cargar.sh carga

verificar:
	python3 datos/verificar.py