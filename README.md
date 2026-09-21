# SupplyGrid

Repositorio de datos para analizar escalabilidad en proveedores, catalogo,
ordenes y logistica. El alcance de este trabajo es el modelo fisico, la
generacion reproducible, la carga masiva y la validacion automatica.

La aplicacion Java bajo `src/` queda fuera de alcance. No es necesaria para
generar, cargar ni verificar el dataset.

## Requisitos

- Docker Desktop y Docker Compose.
- Python 3 con `psycopg2` instalado.
- Bash y `make` (WSL o Git Bash en Windows).

La configuracion por defecto es PostgreSQL en `localhost:5432`, base
`supplygrid_db`, usuario `postgres` y contrasena `postgres`. Se pueden cambiar
con `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` y `PSQL`.

## Artefactos principales

La inyeccion esta separada en tres programas pequenos:

```text
datos/
    generar.py       # genera los CSV por streaming
    cargar.sh        # ejecuta COPY y las tres mediciones PostgreSQL
    verificar.py     # comprueba volumen, sesgo y casos borde
datos/salida/       # CSV y metricas generados
modelo-fisico.sql   # esquemas, tablas, FK internas e indices
Makefile            # orquesta el flujo de datos
```

`datos/salida/` contiene los CSV, metricas y resultados generados. Los CSV no
se consultan desde la base durante la generacion: las relaciones se resuelven
con IDs y aritmetica sobre rangos conocidos.

## Arranque limpio

Iniciar PostgreSQL:

```bash
docker compose up -d postgres-db
```

Resetear la base antes de cada carga:

```bash
make db-reset
```

`make db-reset` trunca las tablas de los cuatro modulos y auditoria, reinicia
las identidades y conserva la base de datos. No borra el volumen de Docker.

## Flujo completo

La orden principal es:

```bash
make datos
```

Ejecuta, en este orden:

1. `db-reset` para comenzar vacio.
2. `generar` para crear los CSV.
3. `cargar` para ejecutar la carga masiva con `COPY`.
4. `verificar` para imprimir el resultado de la Fase 5.

Para ejecutar cada parte por separado:

```bash
make generar
make cargar
make bench
make verificar
```

La generacion completa usa `seed=42` y escribe en `datos/salida`. Para una
prueba pequena se puede cambiar la salida y conservar el mismo codigo:

```bash
python3 datos/generar.py --seed 42 --out datos/salida
```

## Fases cubiertas

### Fase 1: generacion streaming

`generar.py` escribe cada fila directamente en su CSV, con encabezado, UTF-8 y
fechas ISO-8601. Respeta este orden: proveedores, contratos, SKU, franjas,
ordenes, lineas y eventos. Genera tambien:

- `metricas_generacion.csv`: filas, segundos y bytes por archivo.
- `checksum_ordenes.txt`: checksum determinista de las llaves de idempotencia.
- `casos-borde.txt`: los ocho casos exigidos.

Con la configuracion actual se esperan 100.000 proveedores, 15.000 contratos,
400.000 SKU, 150.000 franjas, 500.000 ordenes, 3.000.000 lineas y 800.000
eventos.

### Fase 2: carga masiva

`cargar.sh carga` elimina los indices secundarios, carga las siete tablas con
`COPY ... CSV HEADER`, crea los indices despues de cargar y ejecuta `ANALYZE`.
El archivo `datos/salida/tiempos_carga_por_tabla.csv` registra el tiempo por
tabla, la creacion de indices y el total.

### Fase 3: estrategias comparables de base de datos

`cargar.sh bench` toma siempre las primeras 50.000 ordenes y mide tres
estrategias directamente contra PostgreSQL:

| Estrategia | Que mide |
| --- | --- |
| `1_COPY_masivo` | Piso teorico de carga |
| `2_INSERT_lotes_1000_una_tx` | Protocolo y commit de lotes |
| `3_INSERT_fila_autocommit` | Anti-patron de commit por fila |
Los resultados quedan en `datos/salida/comparacion_estrategias.csv`. La ruta
transaccional de la aplicacion y la inyeccion incremental por endpoint no forman
parte del alcance de este repositorio de datos.

### Fase 5: verificacion

```bash
make verificar
```

`verificar.py` imprime el conteo contra minimos con `PASA` o `FALLA`, la
distribucion acumulada del top 1 %, 5 % y 20 % de proveedores, las ordenes por
dia de los ultimos 60 dias, los ocho casos borde y el checksum. El entregable
debe conservar esta salida completa.

## Verificacion tecnica rapida

Antes de una carga completa se pueden validar los scripts sin tocar PostgreSQL:

```bash
python3 -m py_compile datos/generar.py datos/verificar.py
bash -n datos/cargar.sh
```

Para comprobar la base de datos:

```bash
docker compose up -d postgres-db
```

En Windows, ejecutar los comandos Bash desde WSL o Git Bash. Si `psql` no esta
instalado en el host, se puede usar el cliente del contenedor:

```bash
export PSQL='docker exec -i supplygrid_postgres psql -U postgres -d supplygrid_db -X -q -v ON_ERROR_STOP=1'
make datos
```

## Modelo fisico

`modelo-fisico.sql` es la referencia del modelo que se carga en PostgreSQL:

- esquemas `proveedores`, `catalogo`, `ordenes`, `logistica` y `auditoria`;
- claves foraneas solo dentro del mismo esquema;
- IDs simples para relaciones entre esquemas;
- indices secundarios creados despues de `COPY` por `cargar.sh`;
- indice parcial para franjas disponibles e indices de consulta por proveedor,
  fecha y auditoria.

Las migraciones de `src/` pueden crear la misma estructura al iniciar la
aplicacion, pero no son necesarias para el flujo documentado aqui.
