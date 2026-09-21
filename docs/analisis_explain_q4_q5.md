# Análisis de Planes de Ejecución: EXPLAIN (ANALYZE, BUFFERS) en Q4 y Q5

**Proyecto:** SupplyGrid · Entregable 2 (Escalabilidad de los datos)  
**Curso:** Aplicaciones y Sistemas Escalables · Universidad EAFIT  
**Fecha:** Septiembre 2026  
**Entorno de Ejecución:** PostgreSQL 15-alpine en Docker sobre dataset real (100.000 proveedores, 15.000 contratos, 400.000 SKUs, 500.000 órdenes, 3.000.000 líneas de orden, 150.000 franjas y 800.000 eventos).

---

## 1. Resumen Ejecutivo

Este documento sustenta el comportamiento del optimizador de consultas de PostgreSQL para las consultas analíticas y transaccionales críticas **Q4** y **Q5**, analizando la variación estructural de los planes de ejecución (`EXPLAIN (ANALYZE, BUFFERS, VERBOSE)`), el costo computacional, el método de acceso y unión, los buffers de memoria leídos y los tiempos de ejecución antes y después de la creación de índices, contrastando además con alternativas técnicas de indexación.

| Consulta | Operación ANTES del Índice | Operación DESPUÉS del Índice | Buffers ANTES | Buffers DESPUÉS | Reducción Buffers | Tiempo ANTES | Tiempo DESPUÉS | Speedup |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Q5 (Caliente)** | Parallel Seq Scan (500k) + **Sort (top-N)** | **Index Scan (Zero Sort)** | 5.754 | **56** | **-99.03 %** | 14,02 ms | **0,29 ms** | **48,3x** |
| **Q5 (Frío)** | Parallel Seq Scan (500k) + Quicksort | Bitmap Index Scan + Heap Scan | 5.754 | **27** | **-99.53 %** | 10,46 ms | **0,19 ms** | **55,0x** |
| **Q4 (Caliente)** | Parallel Seq Scan (3M) + **Parallel Hash Join** | **Nested Loop** + Index Scan | 31.510 | **23.637** | **-24,98 %** | 125,16 ms | **24,30 ms** | **5,1x** |
| **Q4 (Frío)** | Parallel Seq Scan (3M) + **Hash Join** | **Nested Loop** + Index Scan | 28.206 | **110** | **-99,61 %** | 100,60 ms | **0,34 ms** | **295,8x** |

---

## 2. Consulta Q5: Últimas 50 órdenes de un proveedor del top 1 %, paginadas

### 2.1 Definición y Consulta SQL
La consulta simula la carga del dashboard del portal B2B para proveedores altamente activos (hot partition, caso borde 7 con $\ge 5.000$ órdenes), donde el usuario consulta las órdenes más recientes en orden cronológico inverso:

```sql
SELECT id, proveedor_id, contrato_id, fecha_orden, estado, idempotency_key
FROM ordenes.ordenes
WHERE proveedor_id = :proveedor_id
ORDER BY fecha_orden DESC
LIMIT 50;
```

**Índice seleccionado:**
```sql
CREATE INDEX idx_ordenes_proveedor_fecha
    ON ordenes.ordenes (proveedor_id, fecha_orden DESC);
```

### 2.2 Lectura del Plan: ANTES vs. DESPUÉS

#### A. Escenario Caliente (`proveedor_id = 3`, 5.000 órdenes)

* **Plan ANTES (Sin índices en `ordenes.ordenes`):**
  ```text
  Limit  (cost=9357.94..9363.78 rows=50 width=62) (actual time=11.597..13.980 rows=50 loops=1)
    Buffers: shared hit=5754
    ->  Gather Merge  (cost=9357.94..9861.98 rows=4320 width=62) (actual time=11.595..13.972 rows=50 loops=1)
          Workers Planned: 2, Workers Launched: 2
          ->  Sort  (cost=8357.92..8363.32 rows=2160 width=62) (actual time=9.234..9.237 rows=41 loops=3)
                Sort Key: ordenes.fecha_orden DESC
                Sort Method: top-N heapsort  Memory: 37kB
                ->  Parallel Seq Scan on ordenes.ordenes (cost=0.00..8286.17 rows=2160 width=62)
                      Filter: (ordenes.proveedor_id = 3)
                      Rows Removed by Filter: 165000 (loops=3)
                      Buffers: shared hit=5682
  Execution Time: 14.025 ms
  ```
  * **Diagnóstico del Plan ANTES:**
    1. Al no existir ruta de acceso indexada por `proveedor_id`, el motor recurre a un **`Parallel Seq Scan`** barriendo las 500.000 tuplas de la tabla repartidas entre el proceso líder y dos workers (`loops=3`).
    2. Cada worker descarta en promedio 165.000 filas que no pertenecen al proveedor 3 (`Rows Removed by Filter: 165000`).
    3. Para resolver el `ORDER BY fecha_orden DESC LIMIT 50`, cada worker debe acumular sus coincidencias y ejecutar un algoritmo de ordenamiento en memoria **`top-N heapsort`** (37 kB de RAM).
    4. Posteriormente, un nodo **`Gather Merge`** entrelaza los flujos ordenados de los workers para entregar las 50 filas al nodo `Limit`.
    5. **Consumo de I/O:** Se acceden **5.754 bloques de buffer** (~45 MB de páginas de datos).

* **Plan DESPUÉS (Con `idx_ordenes_proveedor_fecha`):**
  ```text
  Limit  (cost=0.42..139.44 rows=50 width=62) (actual time=0.030..0.257 rows=50 loops=1)
    Buffers: shared hit=56
    ->  Index Scan using idx_ordenes_proveedor_fecha on ordenes.ordenes (cost=0.42..14410.89 rows=5183 width=62) (actual time=0.029..0.253 rows=50 loops=1)
          Index Cond: (ordenes.proveedor_id = 3)
          Buffers: shared hit=56
  Execution Time: 0.296 ms
  ```
  * **Diagnóstico del Plan DESPUÉS:**
    1. **Eliminación del nodo Sort (Zero-Sort):** Como el árbol B-tree almacena las entradas ordenadas físicamente por `(proveedor_id ASC, fecha_orden DESC)`, las filas correspondientes a `proveedor_id = 3` ya se encuentran en orden cronológico inverso exacto.
    2. **Parada Temprana (`Early Exit`):** El nodo `Limit 50` solo requiere que el cursor del índice avance 50 entradas desde el inicio del subárbol de `proveedor_id = 3`. No se lee ninguna de las restantes 4.950 órdenes de ese proveedor ni se tocan las órdenes de otros proveedores.
    3. **Consumo de I/O:** Pasa de 5.754 buffers a solo **56 buffers** (reducción del **99,03 %**).
    4. **Tiempo de Ejecución:** Se reduce de 14,02 ms a **0,29 ms** (aceleración de **48,3x**).

#### B. Escenario Frío (`proveedor_id = 14000`, 21 órdenes en total)

* **Plan ANTES:** Obligado a hacer `Parallel Seq Scan` sobre 500.000 filas, leyendo 5.754 buffers y tardando **10,46 ms** simplemente para encontrar 21 órdenes.
* **Plan DESPUÉS:** Ejecuta `Bitmap Index Scan` sobre `idx_ordenes_proveedor_fecha` recuperando las 21 tuplas exactas con solo **27 buffers** leídos en **0,19 ms** (**55x más rápido**).

---

### 2.3 Justificación Técnica frente a Alternativas

¿Por qué el índice compuesto `(proveedor_id, fecha_orden DESC)` y no las alternativas evaluadas?

```text
+-----------------------------------------------------------------------------------------------+
| Comparación de Alternativas para Q5 (Proveedor Caliente = 3)                                 |
+------------------------------------+--------------------------+---------+----------+----------+
| Alternativa de Índice              | Estrategia Planificador  | Buffers | Sort?    | Tiempo   |
+------------------------------------+--------------------------+---------+----------+----------+
| Ninguno (Baseline)                 | Parallel Seq Scan        | 5.754   | Heapsort | 14,02 ms |
| Alternativa A: (fecha_orden DESC)  | Index Scan Backward      | 4.685   | No       |  2,81 ms |
| Alternativa B: (proveedor_id)      | Bitmap Heap Scan         | 3.373   | Heapsort |  3,49 ms |
| **ELEGIDO: (proveedor_id, fecha)** | **Index Scan (Directo)** | **56**  | **No**   | **0,29 ms|
+------------------------------------+--------------------------+---------+----------+----------+
```

1. **Descarte de Alternativa A: Índice mono-columna en `(fecha_orden DESC)`**
   * *Comportamiento medido:* El planificador intenta usar `Index Scan Backward` sobre `idx_ordenes_fecha`. Al recorrer la historia global hacia atrás, tiene que filtrar en el heap `Filter: (ordenes.proveedor_id = 3)`.
   * *Falla:* Para obtener 50 órdenes del proveedor 3, tuvo que inspeccionar y descartar **4.607 tuplas de otros proveedores** (`Rows Removed by Filter: 4607`), consumiendo **4.685 buffers** (casi igual que un Seq Scan).
   * *Patología en frío:* Para un proveedor frío (e.g., 14.000), el optimizador reconoce que tendría que recorrer casi los 500.000 registros del índice antes de encontrar 50 filas, por lo que **abandona el índice** y se degrada a Seq Scan con 5.754 buffers y 10,7 ms.

2. **Descarte de Alternativa B: Índice mono-columna en `(proveedor_id)`**
   * *Comportamiento medido:* Ubica eficientemente las 5.000 órdenes del proveedor mediante `Bitmap Index Scan` + `Bitmap Heap Scan`.
   * *Falla:* Al no estar indexada la fecha junto al proveedor, el motor se ve forzado a leer **3.366 bloques del heap** para traer las 5.000 tuplas a memoria de trabajo y ejecutar un nodo explícito **`Sort Key: fecha_orden DESC` (`top-N heapsort`)** para descartar 4.950 y conservar 50.
   * *Consumo:* 3.373 buffers y 3,49 ms (12 veces más lento que el índice compuesto).

3. **Descarte de Alternativa C: Índice Hash en `(proveedor_id)`**
   * Los índices Hash en PostgreSQL no preservan orden lexicográfico ni soportan escaneo por rangos. Forzaría de forma invariable la recuperación de todas las filas y un Sort explícito, sin permitir paradas tempranas en `LIMIT`.

4. **Conclusión de Q5:** El índice compuesto B-tree `(proveedor_id, fecha_orden DESC)` implementa el principio de diseño de acceso **Equality-First, Range/Sort-Second**: el primer campo reduce la búsqueda al subárbol unívoco del proveedor, y el segundo campo garantiza que la lectura física coincida con el orden de salida, transformando un problema de complejidad $O(N \log N)$ en una lectura acotada $O(\log M + K)$ donde $K = 50$, haciendo la consulta insensible al tamaño de la partición caliente.

---

## 3. Consulta Q4: OTIF y Fill Rate por proveedor y mes, sobre 24 meses

### 3.1 Definición y Consulta SQL
La consulta agrega el desempeño logístico mensual de un proveedor cruzando cabeceras de órdenes (`ordenes.ordenes`) con sus respectivas líneas de detalle (`ordenes.lineas_orden`):

```sql
SELECT
    date_trunc('month', o.fecha_orden) AS mes,
    count(DISTINCT o.id) AS total_ordenes,
    count(l.id) AS total_lineas,
    sum(l.cantidad) AS total_unidades,
    round(avg(l.cantidad), 2) AS promedio_unidades_linea
FROM ordenes.ordenes o
JOIN ordenes.lineas_orden l ON l.orden_id = o.id
WHERE o.proveedor_id = :proveedor_id
  AND o.fecha_orden >= '2024-09-01' AND o.fecha_orden <= '2026-09-18'
GROUP BY date_trunc('month', o.fecha_orden)
ORDER BY mes;
```

**Índice seleccionado:**
```sql
CREATE INDEX idx_lineas_orden_id
    ON ordenes.lineas_orden (orden_id);
```

### 3.2 Lectura del Plan: ANTES vs. DESPUÉS

#### A. Escenario Caliente (`proveedor_id = 3`, 5.000 órdenes, ~30.000 líneas)

* **Plan ANTES (Sin índice `idx_lineas_orden_id` en `lineas_orden`):**
  ```text
  GroupAggregate  (cost=51710.43..55807.77 rows=5178 width=64) (actual time=115.891..124.992 rows=25 loops=1)
    Buffers: shared hit=8519 read=22991
    ->  Gather Merge  (actual time=115.691..120.992 rows=29956 loops=1)
          ->  Sort  (actual time=112.924..113.439 rows=9985 loops=3)
                Sort Method: quicksort  Memory: 1079kB
                ->  Parallel Hash Join  (cost=5996.62..49826.25 rows=12945 width=28) (actual time=1.686..111.062 rows=9985 loops=3)
                      Hash Cond: (l.orden_id = o.id)
                      Buffers: shared hit=8505 read=22991
                      ->  Parallel Seq Scan on lineas_orden l (cost=0.00..40516.00 rows=1250000 width=20) (actual time=0.010..52.452 rows=1000000 loops=3)
                            Buffers: shared hit=5025 read=22991
                      ->  Parallel Hash (cost=5969.64..5969.64 rows=2158 width=16) (actual time=1.359..1.360 rows=1664 loops=3)
                            Buckets: 8192  Memory Usage: 352kB
                            ->  Parallel Bitmap Heap Scan on ordenes.ordenes o
                                  Recheck Cond: ((proveedor_id = 3) AND (fecha_orden BETWEEN ...))
  Execution Time: 125.162 ms
  ```
  * **Diagnóstico del Plan ANTES:**
    1. **Estructura del JOIN:** Como la tabla interna `lineas_orden` (3.000.000 de filas) carece de índice en su clave foránea `orden_id`, el optimizador no tiene forma de buscar selectivamente las líneas que pertenecen a las 5.000 órdenes del proveedor.
    2. **Algoritmo Parallel Hash Join:**
       * Crea una tabla hash en memoria con los IDs de las órdenes filtradas (`Parallel Hash` con 352 kB de RAM).
       * Para verificar qué líneas coinciden, realiza un **`Parallel Seq Scan on lineas_orden` barriendo secuencialmente los 3.000.000 de registros** de la tabla (1.000.000 de tuplas por worker).
    3. **I/O Masivo:** El escaneo secuencial obliga a leer **22.991 bloques físicos de disco** (`read=22991`) y 8.519 en caché, para un total de **31.510 buffers** (~252 MB de memoria transitada).
    4. **Tiempo de Ejecución:** **125,16 ms**.

* **Plan DESPUÉS (Con `idx_lineas_orden_id`):**
  ```text
  GroupAggregate  (cost=22371.63..26468.97 rows=5178 width=64) (actual time=14.489..24.131 rows=25 loops=1)
    Buffers: shared hit=23637
    ->  Gather Merge  (actual time=14.202..19.770 rows=29956 loops=1)
          ->  Sort  (actual time=11.434..11.964 rows=9985 loops=3)
                Sort Method: quicksort  Memory: 1426kB
                ->  Nested Loop  (cost=146.87..20487.45 rows=12945 width=28) (actual time=0.410..9.985 rows=9985 loops=3)
                      Buffers: shared hit=23623
                      ->  Parallel Bitmap Heap Scan on ordenes.ordenes o (cost=146.44..5969.64 rows=2158 width=16)
                            Index Cond: ((proveedor_id = 3) AND (fecha_orden BETWEEN ...))
                      ->  Index Scan using idx_lineas_orden_id on lineas_orden l (cost=0.43..6.65 rows=6 width=20) (loops=4993)
                            Index Cond: (l.orden_id = o.id)
                            Buffers: shared hit=20239
  Execution Time: 24.302 ms
  ```
  * **Diagnóstico del Plan DESPUÉS:**
    1. **Cambio de Método de JOIN (Hash Join $\rightarrow$ Nested Loop):** Al disponer del índice B-tree en `lineas_orden.orden_id`, el optimizador reemplaza el costoso escaneo completo por un bucle anidado indexado (**`Nested Loop`**).
    2. **Index Scan en Tabla Hija:** Para cada una de las 4.993 órdenes recuperadas de la cabecera, el motor desciende directamente por el B-tree de `idx_lineas_orden_id` (`loops=4993`), recuperando en promedio 6 líneas por orden en apenas 0,003 ms por ciclo.
    3. **Eliminación de Lecturas en Disco:** La totalidad de los buffers requeridos se atienden desde caché compartida (`shared hit=23637, read=0`).
    4. **Tiempo de Ejecución:** Pasa de 125,16 ms a **24,30 ms** (aceleración de **5,1x**).

---

#### B. Escenario Frío (`proveedor_id = 14000`, 21 órdenes en 24 meses)

En el escenario frío es donde se revela la verdadera patología del modelo sin índice:

* **Plan ANTES (Sin índice):**
  * Para procesar únicamente **21 órdenes**, el motor no tiene más alternativa que ejecutar el `Hash Join` con `Parallel Seq Scan` sobre **los 3.000.000 de líneas de orden**.
  * **Buffers leídos:** **28.206 buffers** (~225 MB transferidos).
  * **Tiempo de ejecución:** **100,60 ms**.

* **Plan DESPUÉS (Con `idx_lineas_orden_id`):**
  ```text
  GroupAggregate  (cost=434.13..437.78 rows=34 width=64) (actual time=0.178..0.286 rows=13 loops=1)
    Buffers: shared hit=110
    ->  Sort  (cost=434.13..434.64 rows=204 width=28) (actual time=0.162..0.167 rows=126 loops=1)
          ->  Nested Loop  (cost=5.29..426.30 rows=204 width=28) (actual time=0.031..0.142 rows=126 loops=1)
                Buffers: shared hit=110
                ->  Bitmap Heap Scan on ordenes.ordenes o (actual time=0.016..0.038 rows=21 loops=1)
                      Buffers: shared hit=24
                ->  Index Scan using idx_lineas_orden_id on lineas_orden l (loops=21)
                      Index Cond: (l.orden_id = o.id)
                      Buffers: shared hit=86
  Execution Time: 0.340 ms
  ```
  * **Buffers leídos:** Pasa de 28.206 buffers a **110 buffers** (reducción del **99,61 %**).
  * **Tiempo de ejecución:** Pasa de 100,60 ms a **0,34 ms** (**aceleración de 295,8x**).
  * *Explicación del hallazgo:* En el modelo sin índice, una consulta para un proveedor marginal con 21 órdenes tarda casi lo mismo que para un proveedor gigante (100 ms vs 125 ms), porque ambas están dominadas por el costo fijo $O(T)$ de escanear 3 millones de líneas. Con el índice, el costo se vuelve proporcional al volumen de datos del proveedor $O(K \log T)$, bajando a fracciones de milisegundo.

---

### 3.3 Justificación Técnica frente a Alternativas

```text
+-----------------------------------------------------------------------------------------------+
| Comparación de Alternativas para Q4                                                           |
+------------------------------------+--------------------------+---------+----------+----------+
| Alternativa de Índice              | Estrategia Planificador  | Buffers | Tipo     | Tiempo   |
+------------------------------------+--------------------------+---------+----------+----------+
| Ninguno en lineas_orden (Baseline) | Parallel Hash Join       | 31.510  | Seq Scan | 125,16 ms|
| Alternativa: Cubriente (orden, cant| Nested Loop + Index Scan | 24.067  | Index    |  30,58 ms|
| **ELEGIDO: idx_lineas_orden_id**   | **Nested Loop + Index**  |**23.637**|**Index**| **24,30 ms|
+------------------------------------+--------------------------+---------+----------+----------+
```

1. **Evaluación de Alternativa: Índice compuesto cubriente `(orden_id, cantidad)`**
   * *Motivación teórica:* En teoría, agregar `cantidad` al índice permitiría un `Index-Only Scan` sobre `lineas_orden`, evitando visitar las páginas del heap para sumar unidades en la agregación.
   * *Resultado medido:*
     * El planificador no pudo realizar un `Index-Only Scan` completo porque la consulta analítica proyecta también `l.id` para el conteo de líneas (`count(l.id)`).
     * El índice compuesto consumió **24.067 buffers** (19.962 hit + 4.105 read) y tardó **30,58 ms** (más lento que el índice simple de 24,30 ms).
   * *Impacto en la fase de carga y concurrencia (Fase 2 y 4):*
     * `lineas_orden` es la tabla con mayor volumen de escritura (3.000.000 de filas).
     * Un índice compuesto con columnas numéricas adicionales incrementa el ancho de cada tupla en el B-tree en un 40 %, inflando el tamaño del archivo en disco de ~65 MB a ~95 MB.
     * Durante la carga masiva con `COPY` y las inserciones de la API transaccional, cada escritura debe actualizar páginas B-tree más densas y pesadas, causando **amplificación de escritura y mayor contención de I/O**.

2. **Evaluación de Alternativa: Índice Hash en `lineas_orden(orden_id)`**
   * Los índices Hash no admiten ordenamiento ni algoritmos de `Merge Join`. En caso de que el optimizador deba resolver joins sobre lotes grandes o tablas temporales ordenadas, el índice Hash queda descartado. Adicionalmente, los índices B-tree ofrecen mejor concurrencia en inserciones mediante bloqueos a nivel de página (*page-level locking* con algoritmos Lehman-Yao).

3. **Conclusión de Q4:** El índice B-tree mono-columna en la clave foránea `idx_lineas_orden_id (orden_id)` es la opción arquitectónicamente óptima:
   * Satisface la regla canónica de diseño relacional para claves foráneas 1 a N.
   * Transforma el JOIN de un escaneo exhaustivo de 3 millones de filas (`Hash Join`) a un acceso puntual logarítmico (`Nested Loop`).
   * Mantiene el índice liviano y compacto, protegiendo el throughput de ingestión transaccional y masiva.

---

## 4. Evidencias Generadas en el Repositorio

Los archivos con las salidas completas y no truncadas de `EXPLAIN (ANALYZE, BUFFERS, VERBOSE)` se encuentran disponibles en:
* [`datos/salida/explain_q5_antes.txt`](file:///c:/Users/emanu/Documentos/Clases/2026-2/Aplicaciones%20y%20sistemas%20escalables/SupplyGrid/datos/salida/explain_q5_antes.txt)
* [`datos/salida/explain_q5_despues.txt`](file:///c:/Users/emanu/Documentos/Clases/2026-2/Aplicaciones%20y%20sistemas%20escalables/SupplyGrid/datos/salida/explain_q5_despues.txt)
* [`datos/salida/explain_q5_alternativas.txt`](file:///c:/Users/emanu/Documentos/Clases/2026-2/Aplicaciones%20y%20sistemas%20escalables/SupplyGrid/datos/salida/explain_q5_alternativas.txt)
* [`datos/salida/explain_q4_antes.txt`](file:///c:/Users/emanu/Documentos/Clases/2026-2/Aplicaciones%20y%20sistemas%20escalables/SupplyGrid/datos/salida/explain_q4_antes.txt)
* [`datos/salida/explain_q4_despues.txt`](file:///c:/Users/emanu/Documentos/Clases/2026-2/Aplicaciones%20y%20sistemas%20escalables/SupplyGrid/datos/salida/explain_q4_despues.txt)
* [`datos/salida/explain_q4_alternativas.txt`](file:///c:/Users/emanu/Documentos/Clases/2026-2/Aplicaciones%20y%20sistemas%20escalables/SupplyGrid/datos/salida/explain_q4_alternativas.txt)

Para reproducir estas mediciones en cualquier momento con un solo comando:
```bash
make explain
```
o directamente:
```bash
python3 datos/medir_explain.py
```
