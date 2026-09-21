# Análisis de Planes de Ejecución: EXPLAIN (ANALYZE, BUFFERS) en Q4 y Q5

Para el informe completo y detallado, consultar:
[docs/analisis_explain_q4_q5.md](../docs/analisis_explain_q4_q5.md)

## Resumen de Resultados

### Q5: Últimas 50 órdenes paginadas (Top 1% Hot Partition)
- **Operación ANTES:** `Parallel Seq Scan` (500.000 filas) + `Sort (top-N heapsort)`.
- **Operación DESPUÉS:** `Index Scan` sobre `idx_ordenes_proveedor_fecha` con parada temprana (`Limit 50`). ¡Cero ordenamiento en memoria!
- **Buffers:** Reducción de 5.754 buffers a **56 buffers** (**-99,03 %**).
- **Tiempo:** Reducción de 14,02 ms a **0,29 ms** (**48,3x más rápido**).
- **Justificación vs Alternativas:**
  - Alternativa `(fecha_orden DESC)`: Descartó 4.607 tuplas de otros proveedores en heap, leyendo 4.685 buffers (inviable para proveedores fríos).
  - Alternativa `(proveedor_id)`: Recupera 5.000 tuplas del heap (3.366 bloques) y requiere `Sort` explícito en memoria para descartar 4.950.
  - El índice compuesto `(proveedor_id, fecha_orden DESC)` entrega los datos pre-ordenados para lectura directa sin tocar filas ajenas ni ordenar en RAM.

### Q4: OTIF y Fill Rate por proveedor y mes (24 meses)
- **Operación ANTES:** `Parallel Seq Scan` sobre 3.000.000 de líneas + `Parallel Hash Join`.
- **Operación DESPUÉS:** `Index Scan` sobre `idx_lineas_orden_id` + **`Nested Loop`**.
- **Buffers:** En proveedor frío (21 órdenes), reducción de 28.206 buffers a **110 buffers** (**-99,61 %**).
- **Tiempo:** En frío pasa de 100,60 ms a **0,34 ms** (**295,8x más rápido**). En caliente pasa de 125,16 ms a **24,30 ms** (**5,1x más rápido**).
- **Justificación vs Alternativas:**
  - Alternativa `(orden_id, cantidad)`: Mayor tamaño en disco e impacto negativo en velocidad de `COPY` e inserción masiva (amplificación de escrituras en tabla de 3M filas), sin beneficio medible en tiempo de consulta.
  - El índice B-tree en `lineas_orden(orden_id)` cubre la clave foránea canónica, optimiza el join relacional y mantiene bajo el costo de mantenimiento.
