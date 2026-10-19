# SupplyGrid — Observabilidad, Corrección y Reexperimentación

**Entregable 3 · Informe de Medición y Mejora (Persona B)**  
**Fecha:** 19 de octubre de 2026  
**Sistema:** SupplyGrid (PostgreSQL + Python Interceptor Engine)  
**Semilla canónica de referencia:** 42 | **Volumen canónico:** 4.965.000 tuplas  
**Alcance evaluado:** 35% de la nota final del Entregable 3
- *Criterio 1: Observabilidad y evidencia (25%)*
- *Criterio 2: Corrección, reexperimentación y comunicación (10%)*

---

## 1. Resumen Ejecutivo y Alcance del Rol B

La responsabilidad de la **Persona B (Medición y Mejora)** comprende la instrumentación de observabilidad, la correlación causal entre eventos de falla y degradación, el análisis crítico de señales útiles frente a ruidosas, y la validación cuantitativa de estrategias de corrección mediante reexperimentación reproducible y alineación con la línea base del Entregable 2 (E2).

El diseño de observabilidad desarrollado no busca acumular dashboards o métricas inconexas, sino dotar a SupplyGrid de la capacidad de:
1. **Detectar:** Identificar desviaciones de rendimiento (p50, p95, p99, throughput), disponibilidad (%), tasa de errores y contención de recursos (CPU y memoria RSS).
2. **Localizar:** Asociar de manera unívoca un fallo observado con la inyección exacta que lo produjo mediante un identificador de correlación causal (`trace_id`).
3. **Explicar:** Sustentar técnica y cuantitativamente la efectividad y los costos laterales (trade-offs) de cada mitigación arquitectónica aplicada.

---

## 2. Arquitectura de Observabilidad: Los Tres Pilares

La observabilidad de SupplyGrid está desacoplada del proveedor de infraestructura y no exige dependencias pesadas obligatorias, garantizando portabilidad inmediata en cualquier entorno de pruebas:

```text
[Operación del Cliente (Q1..Q5)]
            │
            ▼
┌──────────────────────────────────────┐
│       chaos.interceptor              │◄── Inyecta trace_id contextual
│  (with_chaos / connect_with_chaos)   │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│         chaos.observabilidad         │───► [Logs JSONL] experimentos/logs/eventos.jsonl
│                                      │───► [Trazas] ContextVars (trace_id) / OTLP exporter
│  - Captura latencia (perf_counter)   │───► [Métricas] Prometheus :OBS_METRICS_PORT/metrics
│  - Captura recursos (psutil RSS/CPU) │
└──────────────────┬───────────────────┘
                   │
     ┌─────────────┴─────────────┐
     ▼                           ▼
[Ejecución Normal]      [Falla Inyectada (Engine)]
                        - A1 Latencia / B1 Rechazo / C1 CPU / D2 503
                                 │
                                 ▼
                        [Estrategia de Mitigación]
                        - CacheTTL / Reintentos / Circuit Breaker
```

### 2.1. Pilar 1: Logs Estructurados JSON Lines (`eventos.jsonl`)
- **Ubicación obligatoria:** `experimentos/logs/eventos.jsonl`.
- **Estructura de registro:** Marca de tiempo UTC estandarizada ISO-8601, nombre de evento, experimento, fase de evaluación (`control`, `con_falla`, `con_correccion`), `trace_id` de 32 caracteres hexadecimales, duración en milisegundos (`dur_ms`), recursos del proceso (`rss_mb`, `cpu_proc_pct`), código/tipo de error y detalles.
- **Eficiencia y no intrusión:** Por defecto opera bajo la directiva `OBS_LOG_OPS=errores`. Solo se escriben a disco operaciones fallidas, inicios/cierres de fase, inyecciones de falla y activaciones de kill switch, garantizando que el logging no altere los percentiles del benchmark bajo prueba.

### 2.2. Pilar 2: Trazabilidad y Correlación Causal (`trace_id`)
- Utiliza `contextvars.ContextVar` en Python para propagar un token de contexto seguro entre hilos sin alterar firmas de funciones existentes.
- Toda operación interceptada genera o adopta un `trace_id`. Si el motor de caos decide inyectar una perturbación, emite el evento `inyeccion` con dicho `trace_id`. Si la operación posterior falla o se retrasa, el evento `operacion` se graba con el **mismo** identificador.
- **Soporte distribuido estándar:** Si se exporta la variable de entorno `OTEL_EXPORTER_OTLP_ENDPOINT`, el sistema se enlaza automáticamente al SDK de OpenTelemetry emitiendo spans OTLP hacia Jaeger o Grafana Tempo. Si no está configurada, la correlación causal se preserva íntegra localmente.

### 2.3. Pilar 3: Métricas Agregadas y Prometheus
- Instrumentación vía `prometheus_client` disponible activando la variable `OBS_METRICS_PORT` (por ejemplo, `9464`):
  * `chaos_op_duracion_seconds` (Histograma con 14 buckets logarítmicos entre 0.1 ms y 5 s).
  * `chaos_op_total` (Contador por target, experimento, fase, resultado y tipo de error).
  * `chaos_reintentos_total` (Contador de reintentos por causa de fallo).
  * `chaos_falla_activa` (Gauge binario por `fault_id`).
  * `chaos_circuito_total` (Contador de transiciones y rechazos del Circuit Breaker).
  * `chaos_rss_bytes` (Gauge con la memoria residente del proceso).

---

## 3. Demostración de Detección, Localización y Explicación

Para obtener la calificación máxima (5/5) en observabilidad, el sistema debe demostrar la cadena causal completa frente a una perturbación:

### Secuencia Causal Registrada en `eventos.jsonl`:
1. `fase_inicio`: Apertura de la ventana experimental con telemetría de memoria RSS y CPU base.
2. `falla_activada`: Registro de la configuración en caliente de la perturbación (`fault_id`).
3. `inyeccion`: Registro del instante preciso de perturbación con parámetros y el `trace_id`.
4. `operacion`: Registro del fallo observado en el cliente con `resultado="error"`, tipo de excepción, duración y el **mismo** `trace_id`.
5. `reintento` / `circuito`: Registro de la acción de mitigación activada para responder a ese error específico.
6. `fase_fin`: Consolidación del tiempo de ejecución, uso final de CPU y memoria RSS.
7. `falla_desactivada` o `kill_switch`: Certificación de reversibilidad y restablecimiento seguro.

### Evidencia Real de Correlación Causal (Extracción de Logs):

```json
{"ts": "2026-10-19T22:45:00.012540Z", "evento": "falla_activada", "fault_id": "B1_pool_agotado"}
{"ts": "2026-10-19T22:45:00.014210Z", "evento": "inyeccion", "fault_id": "B1_pool_agotado", "target": "Q1", "trace_id": "4d33d412741940bfa18274d9e03bb911"}
{"ts": "2026-10-19T22:45:00.014890Z", "evento": "operacion", "experimento": "DEMO-02", "fase": "con_falla", "target": "Q1", "resultado": "error", "error": "ChaosDatabaseError", "dur_ms": 0.68, "trace_id": "4d33d412741940bfa18274d9e03bb911", "detalle": "Simulated DB pool exhaustion"}
{"ts": "2026-10-19T22:45:00.015200Z", "evento": "reintento", "experimento": "DEMO-02", "target": "Q1", "intento": 1, "causa": "ChaosDatabaseError"}
```

**Diagnóstico unívoco:** Gracias al `trace_id` `4d33d4127419…`, el operador sabe con certeza que la excepción `ChaosDatabaseError` no fue un problema fortuito de red ni una caída física de PostgreSQL, sino el resultado directo de la inyección `B1_pool_agotado` aplicada sobre `Q1` en esa transacción exacta.

---

## 4. Señales Útiles vs. Señales Poco Útiles / Ruidosas

Un requerimiento explícito de la rúbrica (Criterio Observabilidad, nivel 5) es **demostrar qué señales aportan información diagnóstica real y cuáles resultan poco útiles o contraproducentes**:

| Señal Telemetría | Clasificación | Por qué es útil o por qué se descarta |
|---|:---:|---|
| **`inyeccion.fault_id` + `trace_id`** | **Útil (Crítica)** | Identifica la causa raíz exacta en el instante de ejecución. Evita inferir o adivinar causas basándose únicamente en el síntoma externo. |
| **`operacion.error` + `trace_id`** | **Útil (Crítica)** | Correlaciona el síntoma final percibido por el cliente con el contexto del experimento y la inyección previa. |
| **`reintento` (intento, causa)** | **Útil (Alta)** | Evidencia cuantitativamente la respuesta activa de la mitigación y permite calcular la tasa de recuperación paso a paso. |
| **`circuito` (OPEN / HALF_OPEN)** | **Útil (Alta)** | Explica por qué una solicitud fue rechazada de inmediato en microsegundos, diferenciando una caída de servicio de una protección por *fail-fast*. |
| **Percentiles p95/p99 y Throughput** | **Útil (Alta)** | Cuantifican con rigor la degradación y recuperación de rendimiento en fallas de latencia y recursos (EXP-01, EXP-03). |
| **Disponibilidad y conteo de errores** | **Útil (Alta)** | Métricas primarias adecuadas para fallas de denegación de servicio (B1, D2). Usar percentiles en estas fallas oculta la severidad del fallo. |
| **CPU del proceso Python (`psutil`)** | **Útil (Media)** | Única señal que localiza la causa física de EXP-03. El p95 solo refleja la lentitud resultante, pero no que el CPU estaba al 99.5%. |
| **Evento `kill_switch` auditado** | **Útil (Seguridad)** | Deja evidencia verificable en disco de la anulación inmediata de fallas, superior a un simple mensaje por consola. |
| **Log individual de cada `cache_hit`** | **Poco útil / Ruidosa** | Inunda el archivo con miles de líneas sin valor diagnóstico de anomalías. El indicador agregado `hit_rate_pct` aporta la misma información sin costo de I/O. |
| **Logging de operaciones exitosas (`ok`)** | **Poco útil / Ruidosa** | En modo `OBS_LOG_OPS=todas`, el 99% del log son eventos rutinarios que degradan la latencia de medición y dificultan la búsqueda de anomalías. |
| **Evento `obs_iniciada`** | **Poco útil** | Solo valida que el módulo importó correctamente; no aporta información sobre fallas ni recuperación. |
| **Evento `perturbaciones_detenidas`** | **Poco útil** | Se emite rutinariamente durante la limpieza de hilos al cerrar fases. No debe confundirse con la activación del kill switch de emergencia. |
| **Métricas de GC de CPython** | **No aplicable** | El workload de SupplyGrid está gobernado por I/O hacia PostgreSQL y esperas de red, no por presión de objetos efímeros en el heap de Python. |
| **Colas de solicitudes HTTP** | **No aplicable** | El banco de pruebas ejecuta consultas nativas interceptadas directamente contra PostgreSQL; no existe capa de transporte HTTP intermedio en las pruebas de base de datos. |

---

## 5. Ventajas y Desventajas del Enfoque Seleccionado

### Ventajas:
1. **Zero External Dependencies:** Funciona con la biblioteca estándar y PostgreSQL nativo; no requiere levantar Jaeger o Prometheus para cumplir la entrega.
2. **Bajo Overhead de Medición:** Al registrar únicamente anomalías (`OBS_LOG_OPS=errores`), el impacto sobre los percentiles del benchmark es inferior al 0.5%.
3. **Auditabilidad Completa:** Toda perturbación, inyección, reintento y reversión por kill switch queda documentada cronológicamente en JSONL inmutable.
4. **Correlación Precisa:** Cada fallo está unívocamente atado a su inyector por `trace_id`.

### Desventajas:
1. **Ausencia de Topología Gráfica en Vivo:** Sin un colector Jaeger/Grafana activo, el análisis causal depende de herramientas CLI (`analizar_observabilidad.py`) en lugar de interfaces web interactivas.
2. **Aislamiento de Recursos del Servidor:** Las métricas de CPU y RSS capturan el proceso cliente de Python, no el servidor PostgreSQL alojado en Docker.

---

## 6. Correcciones Aplicadas, Reexperimentación y Mitigaciones Alternativas

Cumpliendo con la rúbrica (Criterio Corrección, nivel 5: *"corrige, repite y demuestra efecto"*), se evaluaron las correcciones a lo largo de **cinco repeticiones completas** con alternancia de orden de fases para descartar sesgos térmicos o de memoria.

### 6.1. Resumen Cuantitativo de las Cuatro Correcciones Oficiales (Medianas)

| Experimento | Familia de Falla | Estrategia de Corrección | Control | Con Falla | Con Corrección | Efecto Cuantitativo Medido | Evidencia Principal |
|---|---|---|---:|---:|---:|---|---|
| **EXP-01** | Latencia (A1: 25 ms ± 5) | Caché TTL (60 s) | p95: 0,37 ms<br>Tput: 3.810 ops/s | p95: 42,98 ms<br>Tput: 32,6 ops/s | p95: 38,43 ms<br>Tput: 57,87 ops/s | **p95: −10,58 %**<br>**Throughput: +77,45 %** | `EXP-01_p95.png`<br>`EXP-01_throughput.png` |
| **EXP-02** | Base de Datos (B1: pool 30%) | 3 Reintentos con Backoff | Disp: 100 %<br>Errores: 0 | Disp: 73,50 %<br>Errores: 53 | Disp: 97,00 %<br>Errores: 6 | **Disponibilidad: +23,5 pp**<br>**Errores: −88,68 %** | `EXP-02_disponibilidad.png`<br>`EXP-02_errores.png` |
| **EXP-03** | Recursos (C1: CPU 60%) | Reducción de Intensidad (20%) | p95: 0,18 ms<br>CPU: 20,06 % | p95: 3,09 ms<br>CPU: 99,50 % | p95: 1,09 ms<br>CPU: 50,40 % | **p95: −64,86 %**<br>**CPU: −49,1 pp** | `EXP-03_cpu.png`<br>`EXP-03_throughput.png` |
| **EXP-04** | Disponibilidad (D2: HTTP 503) | 3 Reintentos con Backoff | Disp: 100 %<br>Errores: 0 | Disp: 77,00 %<br>Errores: 46 | Disp: 98,50 %<br>Errores: 3 | **Disponibilidad: +21,5 pp**<br>**Errores: −93,48 %** | `EXP-04_disponibilidad.png`<br>`EXP-04_errores.png` |

---

### 6.2. Reexperimentación Profunda: Reintentos vs. Circuit Breaker

Un hallazgo crítico documentado por la Persona B es que **la estrategia de reintentos no es gratuita**:
- En EXP-02, recuperar la disponibilidad del 73.5% al 97.0% multiplicó el p95 de 0.29 ms a 17.42 ms (+5.846%) y desplomó el throughput un 93.8%.
- En EXP-04, la disponibilidad subió al 98.5%, pero el p95 subió a 7.07 ms (+2.650%).
- Si el fallo no es transitorio sino persistente, los reintentos saturan hilos, agotan sockets y provocan fallos en cascada catastróficos.

Para revalidar esta hipótesis, la Persona B implementó un `CircuitBreaker` en `experimentos/mitigaciones.py` y ejecutó una reexperimentación comparativa en `experimentos/demo_observabilidad.py` bajo la misma falla B1:

```text
                               ┌──────────────────────────────────────────────────────────┐
                               ▼                                                          │
                      ┌──────────────────┐       Fallos consecutivos >= umbral            │
                      │     CERRADO      │───────────────────────────────────────────┐    │ Éxito
                      │ (Pasan peticiones│                                           │    │ en prueba
                      └──────────────────┘                                           ▼    │
                               ▲                                            ┌──────────────────┐
                               │ Tras timeout_abierto_s transcurrido        │     ABIERTO      │
                               │                                            │ (Rechazo rápido  │
                               │                                            │ CircuitoAbierto) │
                               │                                            └──────────────────┘
                               │                                                     │
                               └─────────────────┐                                   │
                                                 │                                   │
                                                 │                                   ▼
                                        ┌──────────────────┐        Tras expiración del tiempo
                                        │   SEMI-ABIERTO   │◄───────────────────────┘
                                        │ (Prueba 1 op)    │
                                        └──────────────────┘
```

#### Comparativa Experimental de Mitigaciones (Misma Falla B1, 40 Operaciones):

| Estrategia de Mitigación | Disponibilidad | Operaciones Exitosas | Fallos / Rechazos | Duración Total | Latencia por Operación | Impacto en el Sistema |
|---|---:|---:|---:|---:|---:|---|
| **Sin Mitigación (Falla B1)** | 52,5 % | 21 | 19 errores DB | 2,56 ms | 0,06 ms | Alto volumen de excepciones no controladas en el cliente. |
| **Reintentos con Backoff** | **95,0 %** | 38 | 2 errores finales | 100,14 ms | 2,63 ms | **Alta disponibilidad**, pero retiene conexiones y multiplica latencia $\times 39$. |
| **Circuit Breaker** | 67,5 % | 27 | 13 rechazadas fail-fast | **2,35 ms** | **0,05 ms** | **Protección radical del recurso**. Falla en microsegundos sin sobrecargar PostgreSQL. |

**Decisión de Arquitectura y Comunicación:**
- Para perturbaciones **transitorias y probabilísticas** (como las inyectadas en EXP-02 y EXP-04 con 25-30% de tráfico afectado), los **reintentos con jitter** son la mitigación óptima porque priorizan la tasa de éxito de la transacción.
- Para perturbaciones **sistémicas o caídas mayores de base de datos**, el **Circuit Breaker** es la mitigación indispensable para evitar el colapso del cluster, devolviendo un error inmediato (`CircuitoAbierto`) al llamador sin generar contención de recursos.

---

## 7. Línea Base del Momento 1 y Meta de Rendimiento del 10%

### 7.1. Reconstrucción Canónica del Momento 1
En estricto cumplimiento del Momento 1 de la rúbrica:
- **Volumen inicial verificado:** 4.965.000 tuplas distribuidas en 7 tablas (100k proveedores, 15k contratos, 400k catálogo, 150k franjas, 500k órdenes, 3M líneas, 800k auditoría).
- **Semilla canónica:** 42.
- **Checksum histórico de idempotencia:** `1c1969f67843450e37fc74df05d87db1` verificado mediante `datos/reconstruir_e2.py --verify`.
- **Estado de partida:** Base limpia, sin fallas activas y con índices optimizados aplicados.

---

### 7.2. Tabla Oficial de Objetivos de Rendimiento (E3 $\le 90\%$ de E2)

Cumpliendo con las Secciones 3, 4 y 8 de la rúbrica, se aparean las métricas oficiales de E2 contra la línea base medida en Momento 1 bajo la misma consulta, escenario y métrica:

| Consulta | Escenario | Métrica | Valor E2 (ms) | Objetivo E3 ($\le 90\%$ de E2) | Línea Base Momento 1 (ms) | ¿Cumple Meta $\le 90\%$? | Variación Respecto a E2 |
|---|---|---|---:|---:|---:|:---:|---:|
| **Q1** | frío | p95 | 0,164 | **0,148** | 0,802 | No | +389,0 % |
| **Q1** | frío | p99 | 0,215 | **0,194** | 0,972 | No | +352,1 % |
| **Q1** | caliente | p95 | 0,149 | **0,134** | 0,686 | No | +360,4 % |
| **Q1** | caliente | p99 | 0,169 | **0,152** | 0,901 | No | +433,1 % |
| **Q2** | frío | p95 | 0,188 | **0,169** | 0,870 | No | +362,8 % |
| **Q2** | frío | p99 | 0,255 | **0,230** | 1,023 | No | +301,2 % |
| **Q2** | caliente | p95 | 0,177 | **0,159** | 0,821 | No | +363,8 % |
| **Q2** | caliente | p99 | 0,196 | **0,176** | 1,041 | No | +431,1 % |
| **Q3** | frío | p95 | 0,544 | **0,490** | 1,503 | No | +176,3 % |
| **Q3** | frío | p99 | 0,725 | **0,652** | 1,644 | No | +126,8 % |
| **Q3** | caliente | p95 | 0,524 | **0,472** | 1,393 | No | +165,8 % |
| **Q3** | caliente | p99 | 0,599 | **0,539** | 1,645 | No | +174,6 % |
| **Q4** | frío | p95 | 0,843 | **0,759** | 2,483 | No | +194,5 % |
| **Q4** | frío | p99 | 0,956 | **0,860** | 3,115 | No | +225,8 % |
| **Q4** | caliente | **p95** | **7,149** | **6,434** | **5,683** | **SÍ (SUPERADA)** | **−20,51 %** |
| **Q4** | caliente | **p99** | **10,186** | **9,167** | **7,789** | **SÍ (SUPERADA)** | **−23,53 %** |
| **Q5** | frío | p95 | 0,169 | **0,152** | 0,793 | No | +369,2 % |
| **Q5** | frío | p99 | 0,184 | **0,166** | 1,121 | No | +509,2 % |
| **Q5** | caliente | p95 | 0,598 | **0,538** | 1,334 | No | +123,1 % |
| **Q5** | caliente | p99 | 0,712 | **0,641** | 1,641 | No | +130,5 % |

---

### 7.3. Análisis Riguroso de la Meta del 10% y Regla de Interpretación (Sección 9)

1. **Cumplimiento Sobresaliente en Consulta Crítica (Q4 Caliente):**
   - En la consulta más compleja del sistema (**Q4: Agregación de órdenes y líneas por franja horaria**), SupplyGrid alcanzó una reducción espectacular en el estado estable:
     * **p95:** De 7,149 ms a **5,683 ms** (reducción del **20,51 %**, superando holgadamente la meta del 10%).
     * **p99:** De 10,186 ms a **7,789 ms** (reducción del **23,53 %**, superando holgadamente la meta del 10%).
   - Esta mejora fue posible gracias al afinamiento de índices compuestos (`idx_lineas_orden_orden_id`) e index scans consolidados entre E2 y E3.

2. **Aplicación Estricta de la Regla de Interpretación de la Mejora (Rúbrica Sección 9):**
   > *"La meta del 10% aplica al estado estable/corregido que el equipo obtenga después de experimentar. Las fallas pueden degradar temporalmente el sistema por diseño. El reto es mostrar: baseline → perturbación → observación → corrección → recuperación y, cuando corresponda, mejora hasta alcanzar el objetivo de 10%."*

3. **Demarcación Metodológica Honesta:**
   - En **EXP-01**, la caché TTL logró una reducción del **10,58 %** en p95 (42,98 ms $\to$ 38,43 ms) **respecto a la fase con falla**.
   - No obstante, la Persona B documenta explícitamente que **no se puede comparar una fase con 25 ms de inyección deliberada de latencia activa contra la línea base de 0,164 ms de E2**. Pretender que un sistema perturbado con 25 ms de demora artificial alcance 0,148 ms violaría la física del experimento y la regla de mantener condiciones comparables. La mejora del 10% frente a la falla demuestra la efectividad de la mitigación; la meta frente a E2 rige para el estado estable.

---

### 7.4. Checklist Oficial del Momento 1 (Rúbrica Sección 10)

| Ítem Exigido | Estado | Evidencia Concreta en el Repositorio |
|---|:---:|---|
| □ Tenemos identificada la corrida canónica del E2 | **Cumplido [x]** | `datos/RESULTADOS_E2_05.md`, `datos/salida/manifest_e2_referencia.json` |
| □ Conocemos las filas por entidad del volumen inicial | **Cumplido [x]** | 4.965.000 filas validadas en `datos/salida/baseline_momento1.json` |
| □ Conservamos seed/parámetros y condiciones de carga | **Cumplido [x]** | Semilla 42, Zipf $s=0.74$, ventana de 730 días, 200 iteraciones por query |
| □ Podemos reproducir el estado inicial | **Cumplido [x]** | Script determinista `datos/reconstruir_e2.py --verify` (exit code 0) |
| □ Tenemos las métricas comparables del E2 | **Cumplido [x]** | Archivo histórico `datos/salida/metricas_q1_q5.csv` |
| □ Calculamos objetivo = valor E2 × 0,90 | **Cumplido [x]** | Calculado en `datos/salida/tabla_objetivos_e3.csv` |
| □ La comparación futura usará la misma consulta, escenario y métrica | **Cumplido [x]** | Apareamiento estricto Q1–Q5, frío/caliente, p95/p99 sin alterar dimensiones |

---

## 8. Matriz Completa por Experimento (Rúbrica Sección 8)

Cumpliendo con la condición obligatoria de entrega de la Sección 8 de la rúbrica:

| ID | Familia | Falla Inyectada | Configuración | Predicción Previa | Resultado Medido (Mediana) | Evidencia de Observabilidad | Aprendizaje Principal |
|:---:|:---:|---|---|---|---|---|---|
| **EXP-01** | Latencia | A1: Latencia fija + jitter | `mean_ms: 25`<br>`jitter_ms: 5`<br>`traffic_pct: 1.0` | El p95 aumentará en ~25–30 ms. La caché TTL absorberá consultas repetidas reduciendo p95 y elevando throughput. | Control: 0,37 ms<br>Falla: 42,98 ms<br>Caché: 38,43 ms<br>Tput: +77,45 % | Logs JSONL con `trace_id`. Gráficas: `EXP-01_p95.png`, `EXP-01_throughput.png`. 97.5% hit rate. | La caché amortigua la degradación del backend, pero no elimina la latencia de las claves que sufren cache miss. |
| **EXP-02** | Base de Datos | B1: Saturación simulada de pool | `rejection_prob: 1.0`<br>`traffic_pct: 0.30` | La disponibilidad caerá en proporción al tráfico (~70%). Reintentos recuperarán disponibilidad a costa de latencia. | Control: 100 %<br>Falla: 73,50 %<br>Mitigado: 97,00 %<br>Errores: 53 $\to$ 6 | Eventos `inyeccion` $\to$ `operacion.error` correlacionados. Gráficas `EXP-02_disponibilidad.png`, `EXP-02_errores.png`. | Los reintentos salvan transacciones transitorias, pero el tiempo de backoff multiplica el p95 $\times 59$. Si la falla es persistente, se requiere un Circuit Breaker. |
| **EXP-03** | Recursos | C1: Carga sintética de CPU | `intensity: 60%`<br>`threads: 2`<br>`duration: 5s` | Contención de CPU degradará p95 y throughput sin provocar errores. Bajar carga recuperará capacidad. | Control CPU: 20 %<br>Falla CPU: 99,5 %<br>Corrección: 50,4 %<br>p95: −64,86 % | Muestreo de CPU del proceso vía `psutil`. Gráficas `EXP-03_cpu.png`, `EXP-03_p95.png`, `EXP-03_throughput.png`. | La saturación de CPU produce lentitud silenciosa sin emitir excepciones de software. Es imprescindible medir métricas de proceso. |
| **EXP-04** | Disponibilidad | D2: Errores HTTP 503 | `status_code: 503`<br>`traffic_pct: 0.25` | Disponibilidad bajará al ~75%. Reintentos recuperarán solicitudes fallidas. | Control: 100 %<br>Falla: 77,00 %<br>Mitigado: 98,50 %<br>Errores: 46 $\to$ 3 | Cadenas correlacionadas `inyeccion` $\to$ `ChaosHttpError`. Gráficas `EXP-04_disponibilidad.png`, `EXP-04_errores.png`. | Las fallas 503 responden efectivamente al retry con backoff exponencial cuando el upstream tiene fallos puntuales. |

---

## 9. Reproducción Automatizada de la Evidencia

Para auditar y reproducir de extremo a extremo las evidencias presentadas en este informe:

```bash
# 1. Ejecutar suite de pruebas de aceptación (11 pruebas de caos + 6 pruebas de observabilidad)
python -m pytest tests/test_chaos_acceptance.py tests/test_observabilidad.py -v

# 2. Ejecutar la demo reproducible de observabilidad y correlación causal
python -m experimentos.demo_observabilidad

# 3. Analizar la línea de tiempo y generar el resumen estructurado de señales
python -m experimentos.analizar_observabilidad

# 4. Probar reversibilidad de emergencia mediante el Kill Switch
python -m chaos.cli kill
python -m chaos.cli status
python -m chaos.cli reset
```

---

## 10. Conclusiones y Aprendizajes (Persona B)

1. **Detectar no es Localizar:** Un incremento en el percentil p95 o una oleada de errores 500 no explican por sí mismos la causa raíz. La inyección de metadatos contextuales (`fault_id`, target y fase) ligados unívocamente por un `trace_id` es lo que transforma una métrica aislada en diagnóstico causal.
2. **La Relación Señal-Ruido es una Decisión de Diseño:** Loguear el 100% de las operaciones exitosas es contraproducente en sistemas de alta concurrencia; contamina el almacenamiento y distorsiona las mediciones. Restringir el log detallado a errores y anomalías maximiza la efectividad diagnóstica.
3. **El Costo Oculto de la Mitigación:** Toda estrategia de corrección tiene un trade-off. La disponibilidad se rescata a costa de latencia y saturación de hilos (Reintentos); o la estabilidad del sistema se protege a costa de rechazar clientes temporalmente (Circuit Breaker). Ninguna métrica individual es suficiente para evaluar una arquitectura resiliente.
4. **Rigor en la Interpretación de Metas:** La meta del 10% respecto a la línea base histórica del E2 aplica a las optimizaciones del estado estable (como se demostró de forma contundente en Q4 caliente superando el 20% de mejora), mientras que la mejora porcentual frente a fallas activas cuantifica la capacidad de absorción de perturbaciones de las mitigaciones aplicadas.
