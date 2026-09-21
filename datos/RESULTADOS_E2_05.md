# E2-05 — Medición de Q1–Q5 con percentiles

## Protocolo

Cada consulta se ejecuta 200 veces y se reportan los percentiles p50, p95 y p99.

Para las consultas parametrizadas por proveedor se usan dos grupos obtenidos directamente del dataset:

- Frío: 1 % inferior de proveedores según cantidad de órdenes (150 proveedores).
- Caliente: top 1 % según cantidad de órdenes (150 proveedores).

La selección dentro de cada grupo varía durante las 200 ejecuciones y utiliza una semilla fija (`42`) para permitir reproducibilidad.

El proveedor 3 es el caso más caliente del dataset, con 5.000 órdenes. En la cola larga se observan proveedores con aproximadamente 20–21 órdenes.

Q3 no recibe `proveedor_id`; sus parámetros son `cedi_id` y `fecha`. Por tanto, la clasificación frío/caliente por proveedor no aplica directamente. Se conservan ambos escenarios para mantener el mismo protocolo de 200 ejecuciones por escenario, utilizando muestras independientes de CEDI/fecha.

## Volumen medido

| Tabla | Filas |
|---|---:|
| proveedores.proveedores | 100.000 |
| proveedores.contratos | 15.000 |
| catalogo.catalogo_sku | 400.000 |
| ordenes.ordenes | 500.000 |
| ordenes.lineas_orden | 3.000.000 |
| logistica.franjas_descargue | 150.000 |
| auditoria.eventos | 800.000 |
| **Total** | **4.965.000** |

La historia de órdenes cargada comprende desde 2024-09-18 hasta 2026-09-18.

## Resultados

Los valores definitivos se encuentran en:

- `datos/salida/metricas_q1_q5.csv`: percentiles resumidos.
- `datos/salida/metricas_q1_q5_crudas.csv`: las 2.000 ejecuciones individuales.

En la corrida registrada:

| Consulta | Escenario | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---:|---:|---:|
| Q1 | frío | 0.122 | 0.164 | 0.215 |
| Q1 | caliente | 0.126 | 0.149 | 0.169 |
| Q2 | frío | 0.165 | 0.188 | 0.255 |
| Q2 | caliente | 0.160 | 0.177 | 0.196 |
| Q3 | frío | 0.456 | 0.544 | 0.725 |
| Q3 | caliente | 0.444 | 0.524 | 0.599 |
| Q4 | frío | 0.496 | 0.843 | 0.956 |
| Q4 | caliente | 2.126 | 7.149 | 10.186 |
| Q5 | frío | 0.150 | 0.169 | 0.184 |
| Q5 | caliente | 0.306 | 0.598 | 0.712 |

Q1 cumple el objetivo de latencia menor a 10 ms con amplio margen: incluso su p99 global fue de aproximadamente 0,195 ms.

## Diferencia entre parámetros fríos y calientes

En Q1 y Q2 la diferencia es pequeña porque las consultas son selectivas y los índices reducen el conjunto de datos rápidamente.

Q4 presenta la diferencia más marcada. Los proveedores calientes poseen muchas más órdenes y líneas asociadas, por lo que PostgreSQL debe procesar más filas para realizar la agregación mensual. Esto eleva especialmente p95 y p99.

Q5 también muestra mayor latencia para proveedores calientes. Aunque solo devuelve 50 órdenes, existe una partición lógica mucho más grande bajo el mismo `proveedor_id`.

En Q3 los tiempos de ambos escenarios son similares porque la consulta no utiliza proveedor y ambos escenarios muestrean combinaciones de CEDI y fecha.

En este entregable, "frío" y "caliente" describen el sesgo de los parámetros/datos consultados; no representan vaciado o calentamiento manual de la caché de PostgreSQL.

## Limitación de Q4

El requisito funcional define Q4 como OTIF y fill rate por proveedor y mes durante 24 meses.

Sin embargo, el modelo y el dataset actuales no contienen fecha prometida de entrega, fecha real de entrega ni cantidad efectivamente entregada. `ordenes.lineas_orden` solo contiene la cantidad solicitada.

Por esa razón no es posible derivar un OTIF o fill rate logístico real sin inventar información que no existe en el dataset.

La Q4 medida en este issue representa la carga analítica disponible: agregación mensual de órdenes, líneas y unidades solicitadas por proveedor durante la historia cargada. Sus tiempos son válidos como benchmark de esa carga, pero sus resultados no deben interpretarse como valores funcionales de OTIF/fill rate.

Esta limitación queda explícita para evitar introducir una definición artificial de las métricas o modificar retrospectivamente el modelo físico desde una issue dedicada a medición.
