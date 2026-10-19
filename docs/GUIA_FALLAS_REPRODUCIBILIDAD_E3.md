# Guía de Fallas Configurables, Reproducibilidad y Momento 1 (Línea Base)

**Proyecto:** SupplyGrid · Entregable 3 (Resiliencia y Observabilidad)  
**Curso:** Aplicaciones y Sistemas Escalables · Universidad EAFIT  
**Módulo:** Fallas Configurables + Ejecución / Reproducibilidad / Momento 1  

---

## 1. Arquitectura y Ubicación de la Implementación

El sistema de inyección de fallas y reproducibilidad se integra sobre la arquitectura real de SupplyGrid respetando los esquemas físicos de PostgreSQL y los programas de datos del proyecto:

```text
SupplyGrid/
├── chaos/                           # Motor de Fallas Configurables (Chaos Engine)
│   ├── __init__.py                  # Exportación de API principal
│   ├── models.py                    # Data classes, enums y validadores de esquemas
│   ├── config.py                    # Gestor dinámico con hot reload vía mtime
│   ├── engine.py                    # Motor de ejecución de las 4 familias de fallas y Kill Switch
│   ├── interceptor.py               # Interceptores para psycopg2 y decoradores de targets
│   ├── observabilidad.py            # Logs JSONL, métricas y trace_id (Persona B)
│   └── cli.py                       # CLI administrativo dinámico (status, activate, kill, etc.)
├── chaos_config.json                # Archivo de configuración en caliente (recarga sin redespliegue)
├── datos/
│   ├── reconstruir_e2.py            # Reconstrucción determinista limpia y verificación del E2
│   ├── generar_linea_base.py        # Generador de línea base del Momento 1 y tabla de objetivos E3
│   ├── generar.py                   # Generador streaming canónico de CSVs (seed 42)
│   ├── medir_consultas.py           # Medición de Q1-Q5 con percentiles (soporta chaos interceptor)
│   ├── verificar.py                 # Verificador canónico de E2 (casos borde y volumen)
│   └── salida/
│       ├── manifest_e2_referencia.json    # Manifest estructurado canónico con checksums SHA-256 y MD5
│       ├── baseline_momento1.json         # Documento estructurado de línea base sin fallas
│       ├── tabla_objetivos_e3.csv         # Tabla oficial de metas E3 = 90% de E2
│       └── metricas_linea_base_e3.csv     # Métricas de percentiles medidas en Momento 1
├── tests/
│   ├── test_chaos_acceptance.py     # Suite de 11 pruebas automatizadas de aceptación
│   └── test_observabilidad.py       # Correlación trace_id y circuit breaker
└── Makefile                         # Metas estandarizadas del proyecto
```

---

## 2. Catálogo Oficial de las Cuatro Familias de Fallas

Cumpliendo con la rúbrica oficial y las diapositivas de referencia, se implementan **2 fallas por cada una de las 4 familias** (8 fallas en total), todas desactivadas por defecto:

| Familia | ID Falla | Tipo / Mecanismo | Parámetros Configurables | Efecto Esperado | Focalización (Targets) |
|---|---|---|---|---|---|
| **A. Latencia inducida** | `A1_latencia_jitter` | Latencia fija con jitter (distribución lognormal o gaussiana) | `mean_ms` (float), `jitter_ms` (float), `distribution` ("lognormal"\|"gaussian") | Incrementa el tiempo de respuesta emulando lentitud en red o almacenamiento | `Q1`, `Q4`, `POST /ordenes-compra` (o custom) |
| **A. Latencia inducida** | `A2_timeout_controlado` | Timeout controlado / interrupción forzada | `timeout_ms` (float) | Lanza `ChaosTimeoutError` si el tiempo acumulado de la operación supera el umbral límite | `Q4`, `POST /ordenes-compra` |
| **B. Falla de base de datos** | `B1_pool_agotado` | Saturación simulada de pool de conexiones | `rejection_probability` (0.0–1.0), `error_message` (str) | Rechaza la conexión o consulta con `ChaosDatabaseError` emulando pool exhaustion | `*` o conexiones nuevas |
| **B. Falla de base de datos** | `B2_bloqueo_recurso` | Contención y espera por bloqueo exclusivo | `lock_wait_ms` (float), `target_key` ("proveedor_3") | Retiene la ejecución emulando contención sobre tupla/recurso caliente | `Q4`, `Q5`, `proveedor_3` |
| **C. Degradación de tiempos** | `C1_saturacion_cpu` | Hilos de cómputo intensivo en segundo plano | `intensity_pct` (0–100%), `duration_s` (float), `threads` (int) | Quema ciclos de CPU en background generando contención de procesador | `*` (afecta al proceso global) |
| **C. Degradación de tiempos** | `C2_presion_memoria` | Presión y asignación de memoria RAM acotada | `size_mb` (float), `duration_s` (float) | Asigna búferes temporales provocando presión de memoria y cache | `*` (afecta memoria física) |
| **D. Riesgo de disponibilidad**| `D1_particion_red` | Partición de red transitoria hacia base de datos | `duration_s` (float) | Emula corte de socket / pérdida de conectividad lanzando excepción de conexión | `*` |
| **D. Riesgo de disponibilidad**| `D2_respuestas_5xx` | Inyección de errores 503 Service Unavailable / 500 | `status_code` (500, 502, 503, 504), `message` (str) | Emula falla en upstream o degradación de servicio lanzando `ChaosHttpError` | `POST /ordenes-compra`, `Q1`, `Q3` |

### Las Cinco Condiciones de Falla Configurable Cumplidas:
1. **Sin redespliegue:** Se activa y desactiva en caliente vía `chaos_config.json` o `python -m chaos.cli`.
2. **Parametrizada:** Intensidad, duración, probabilidad, umbrales y semillas son ajustables.
3. **Focalizable:** Permite filtrar por target (`Q1`–`Q5`, endpoints REST, `proveedor_3`, etc.) y porcentaje de tráfico (`traffic_pct`).
4. **Reversible en segundos:** Kill switch global que anula todas las perturbaciones inmediatamente.
5. **Reproducible:** Semilla determinista fijada por defecto en 42 para reproducir perturbaciones idénticas.

---

## 3. Administración Dinámica y Kill Switch

### Ver el estado actual del catálogo:
```bash
python -m chaos.cli status
```

### Activar una falla en caliente (sin reiniciar procesos):
```bash
# Ejemplo: inyectar 30 ms de latencia con 5 ms de jitter en Q1 y Q4
python -m chaos.cli activate A1_latencia_jitter -p mean_ms=30 jitter_ms=5

# Ejemplo: simular saturación de pool de base de datos con 50% de probabilidad
python -m chaos.cli activate B1_pool_agotado -p rejection_probability=0.5
```

### Desactivar una falla individual:
```bash
python -m chaos.cli deactivate A1_latencia_jitter
```

### Kill Switch Global (Prioridad Absoluta):
El Kill Switch anula inmediatamente **todas** las perturbaciones activas (detiene hilos de CPU, libera búferes de memoria y omite delays y excepciones) sin necesidad de reiniciar la aplicación:

```bash
# Activar Kill Switch
python -m chaos.cli kill

# Desactivar Kill Switch (vuelve a regir la configuración individual)
python -m chaos.cli unkill
```

### Restablecer configuración a estado seguro (todo apagado):
```bash
python -m chaos.cli reset
```

---

## 4. Reconstrucción y Verificación del E2 (Momento 1)

El script [datos/reconstruir_e2.py](file:///c:/Users/emanu/Documentos/Clases/2026-2/Aplicaciones%20y%20sistemas%20escalables/Entrega%203/SupplyGrid/datos/reconstruir_e2.py) garantiza la reconstrucción reproducible del estado del E2.

### Requisitos previos:
- Contenedor de PostgreSQL iniciado: `docker compose up -d postgres-db`
- Esquema base aplicado (si la base está limpia): `init.sql` / `modelo-fisico.sql`.

### Comando de reconstrucción reproducible:
```bash
python datos/reconstruir_e2.py --rebuild --seed 42 --clean
```
**Pasos que ejecuta:**
1. Genera los 7 CSVs deterministas en streaming con `seed=42` (`generar.py`).
2. Ejecuta un `TRUNCATE ... RESTART IDENTITY CASCADE` limpio en PostgreSQL.
3. Deshabilita temporalmente los índices secundarios.
4. Carga las 7 tablas usando `COPY` binario masivo.
5. Recrea los 7 índices secundarios optimizados y ejecuta `ANALYZE`.
6. Valida los conteos de filas por entidad y los 8 casos borde.
7. Calcula el checksum histórico MD5 de idempotencia y el SHA-256 de todas las tablas.
8. Emite el archivo [datos/salida/manifest_e2_referencia.json](file:///c:/Users/emanu/Documentos/Clases/2026-2/Aplicaciones%20y%20sistemas%20escalables/Entrega%203/SupplyGrid/datos/salida/manifest_e2_referencia.json).

### Comando de verificación no destructiva:
```bash
python datos/reconstruir_e2.py --verify
```
Comprueba el estado de la base de datos sin alterar ningún dato.
- **Exit Code 0:** Coincidencia perfecta (reproducible y canónico).
- **Exit Code 2:** Discrepancia detectada en conteos o checksums.
- **Exit Code 1:** Error de conexión o fallo de ejecución.

### Justificación de Seed y Conteos Canónicos:
- **Seed canónica:** `42` (solicitada explícitamente y coincidente con la corrida histórica del E2).
- **Conteos por entidad:**
  - `proveedores.proveedores`: 100.000 filas
  - `proveedores.contratos`: 15.000 filas
  - `catalogo.catalogo_sku`: 400.000 filas
  - `logistica.franjas_descargue`: 150.000 filas
  - `ordenes.ordenes`: 500.000 filas
  - `ordenes.lineas_orden`: 3.000.000 filas
  - `auditoria.eventos`: 800.000 filas
  - **Total de filas:** **4.965.000 filas**

### Algoritmo y Alcance del Checksum:
1. **MD5 Idempotencia Histórico:**
   Concatena en streaming las claves `idempotency_key` de `ordenes.ordenes` ordenadas por `id`.  
   Valor esperado y verificado: `1c1969f67843450e37fc74df05d87db1`.
2. **SHA-256 Integral de Estado:**
   Calcula el hash SHA-256 canónico de todas las tuplas de las 7 tablas, serializadas con delimitador `|` y ordenadas estrictamente por clave primaria (`id`), evitando que variaciones en el orden físico de las páginas alteren el hash.

---

## 5. Momento 1: Línea Base y Tabla de Objetivos de Rendimiento (E3 = 90% de E2)

Para cumplir con el Momento 1 exigido por la rúbrica, se ejecuta:

```bash
python datos/generar_linea_base.py
```

### Tabla Oficial de Objetivos de Rendimiento (E3 $\le 90\%$ de E2):
La rúbrica establece la regla: $\text{Objetivo E3} = \text{Valor E2} \times 0.90$ para cada métrica aplicable (conservando la misma consulta, escenario y métrica):

| Consulta | Escenario | Métrica | Valor E2 (ms) | Objetivo E3 ($\le 90\%$) | Línea Base Actual (ms) |
|---|---|---|---:|---:|---:|
| **Q1** | frío | p95 | 0.164 | **0.148** | 0.802 |
| **Q1** | frío | p99 | 0.215 | **0.194** | 0.972 |
| **Q1** | caliente | p95 | 0.149 | **0.134** | 0.686 |
| **Q1** | caliente | p99 | 0.169 | **0.152** | 0.901 |
| **Q2** | frío | p95 | 0.188 | **0.169** | 0.870 |
| **Q2** | frío | p99 | 0.255 | **0.230** | 1.023 |
| **Q2** | caliente | p95 | 0.177 | **0.159** | 0.821 |
| **Q2** | caliente | p99 | 0.196 | **0.176** | 1.041 |
| **Q3** | frío | p95 | 0.544 | **0.490** | 1.503 |
| **Q3** | frío | p99 | 0.725 | **0.652** | 1.644 |
| **Q3** | caliente | p95 | 0.524 | **0.472** | 1.393 |
| **Q3** | caliente | p99 | 0.599 | **0.539** | 1.645 |
| **Q4** | frío | p95 | 0.843 | **0.759** | 2.483 |
| **Q4** | frío | p99 | 0.956 | **0.860** | 3.115 |
| **Q4** | caliente | p95 | 7.149 | **6.434** | 5.683 |
| **Q4** | caliente | p99 | 10.186 | **9.167** | 7.789 |
| **Q5** | frío | p95 | 0.169 | **0.152** | 0.793 |
| **Q5** | frío | p99 | 0.184 | **0.166** | 1.121 |
| **Q5** | caliente | p95 | 0.598 | **0.538** | 1.334 |
| **Q5** | caliente | p99 | 0.712 | **0.641** | 1.641 |

> [!NOTE]
> De acuerdo con la **Regla de interpretación de la mejora (Rúbrica Sección 9)**: La meta del 10% aplica al **estado estable/corregido** posterior a los experimentos y no a la fase donde la falla está activa degradando el sistema.

### Checklist Oficial del Momento 1:
- [x] **Tenemos identificada la corrida canónica del E2:** Confirmada en `RESULTADOS_E2_05.md` y `manifest_e2_referencia.json`.
- [x] **Conocemos las filas por entidad del volumen inicial:** 4.965.000 tuplas totales (100k, 15k, 400k, 150k, 500k, 3M, 800k).
- [x] **Conservamos seed/parámetros y condiciones de carga:** Seed 42, Zipf $s=0.74$, historia de 730 días.
- [x] **Podemos reproducir el estado inicial:** Verificado exitosamente con `datos/reconstruir_e2.py --verify`.
- [x] **Tenemos las métricas comparables del E2:** Extraídas de `metricas_q1_q5.csv` de E2.
- [x] **Calculamos objetivo = valor E2 × 0,90:** Calculado en `datos/salida/tabla_objetivos_e3.csv`.
- [x] **La comparación futura usará la misma consulta, escenario y métrica:** Rigurosamente apareadas (sin mezclar p95 con promedios ni frío con caliente).

---

## 6. Pruebas de Aceptación Automatizadas

Para validar todos los requisitos sin depender de intervenciones manuales:

```bash
python -m unittest tests/test_chaos_acceptance.py -v
```

Se ejecutan y aprueban 11 pruebas unitarias e integradas:
1. `test_01_configuracion_predeterminada_segura`: Verifica que ninguna falla inicie activa y que el kill switch inicie inactivo.
2. `test_02_activacion_y_desactivacion_en_caliente_sin_redespliegue`: Valida cambio dinámico de estado y parámetros.
3. `test_03_prioridad_absoluta_del_kill_switch`: Valida anulación inmediata de todas las fallas cuando `kill_switch=True`.
4. `test_04_recuperacion_inmediata_sin_redespliegue`: Valida restauración inmediata del comportamiento normal sin reiniciar el proceso.
5. `test_05_cobertura_de_las_cuatro_familias_exigidas`: Comprueba presencia de 2 fallas por cada una de las 4 familias.
6. `test_06_validacion_de_parametros_invalidos`: Rechaza parámetros negativos, tráfico fuera de rango (0–1) o códigos HTTP erróneos.
7. `test_07_inferencia_de_targets_sql`: Valida detección automática de Q1–Q5 en consultas SQL.
8. `test_08_conteos_canonicos_e2_y_semilla`: Comprueba que los conteos esperados coinciden con los 4.965.000 registros del E2.
9. `test_09_checksum_historico_idempotencia_e2`: Valida el hash MD5 histórico `1c1969f67843450e37fc74df05d87db1`.
10. `test_10_calculo_matematico_objetivo_rendimiento_10_porciento`: Comprueba la fórmula de reducción del 10%.
11. `test_11_determinismo_de_generacion_de_claves_idempotencia`: Valida determinismo estricto entre dos generaciones sucesivas con la misma semilla.

---

## 7. Instrucciones para Continuar (Fase Posterior del Equipo)

Los artefactos dejados en este entregable están estructurados para conectarse directamente con las dos responsabilidades posteriores del equipo:

1. **Observabilidad y Análisis (Métricas, Logs, Trazas):**
   - Utilizar `datos/salida/baseline_momento1.json` como punto de anclaje de la línea base estable.
   - Usar `chaos.interceptor` para correlacionar eventos de falla con las métricas observadas.
2. **Diseño Experimental y Corrección:**
   - La tabla de objetivos en `datos/salida/tabla_objetivos_e3.csv` provee la columna obligatoria de meta ($E2 \times 0.90$).
   - Diseñar los 4 a 8 experimentos activando las fallas deseadas con `python -m chaos.cli activate <falla>`, midiendo la degradación, aplicando la corrección arquitectónica (ej. Circuit Breaker, Retry, Bulkhead, Cache o Índices) y comprobando la convergencia hacia el objetivo.
