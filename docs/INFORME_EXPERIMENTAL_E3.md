# SupplyGrid — Informe de diseño experimental, evaluación de fallas y estrategias de corrección

**Entregable 3 — Sección C: Diseño experimental, corrección y comunicación de resultados**

**Fecha:** 19 de octubre de 2026
**Sistema evaluado:** SupplyGrid
**Entorno:** Ejecución local con Python y PostgreSQL
**Referencia de datos:** Entregable 2 (E2), semilla 42

---

## 1. Introducción

La evaluación de resiliencia de un sistema requiere analizar su comportamiento no solamente bajo condiciones normales, sino también cuando se presentan perturbaciones que afectan su rendimiento, disponibilidad o capacidad de procesamiento.

En el Entregable 3 de SupplyGrid se implementó una estrategia de experimentación controlada basada en inyección de fallas. El propósito fue observar cómo responde el sistema ante distintos escenarios de degradación y determinar en qué medida las estrategias de corrección permiten recuperar su funcionamiento.

Para ello, se desarrollaron cuatro experimentos correspondientes a las familias de latencia, base de datos, degradación de recursos y disponibilidad.

Cada experimento contempló tres fases: una ejecución de control, una ejecución con falla y una ejecución con corrección. Esta estructura permitió comparar los resultados obtenidos bajo condiciones equivalentes dentro de cada experimento.

Se realizaron cinco repeticiones por experimento, para un total de 20 repeticiones experimentales, cada una con tres fases de evaluación.

---

## 2. Objetivos

### 2.1. Objetivo general

Evaluar el impacto de cuatro tipos de fallas controladas sobre el comportamiento de SupplyGrid y analizar la efectividad de estrategias de mitigación mediante indicadores cuantitativos de rendimiento, disponibilidad y consumo de recursos.

### 2.2. Objetivos específicos

1. Establecer condiciones de control para comparar el comportamiento del sistema antes y después de introducir perturbaciones.
2. Ejecutar fallas controladas correspondientes a cuatro familias de degradación.
3. Aplicar estrategias de corrección apropiadas para cada escenario.
4. Medir latencias p95 y p99, throughput, disponibilidad, errores y utilización de CPU, según corresponda.
5. Comparar cuantitativamente las fases de control, falla y corrección.
6. Documentar las condiciones experimentales, los resultados y las limitaciones para facilitar su revisión y reproducción.

---

## 3. Diseño experimental

### 3.1. Estructura de los experimentos

Los cuatro experimentos utilizaron el siguiente esquema:

**Fase 1. Control:** ejecución sin inyección de fallas para establecer una referencia de comportamiento.

**Fase 2. Falla:** ejecución con una perturbación controlada para observar sus efectos.

**Fase 3. Corrección:** ejecución con la perturbación y una estrategia de mitigación para evaluar la recuperación.

Las mediciones se realizaron sobre operaciones asociadas a la consulta Q1 del proyecto.

Los experimentos utilizaron cinco repeticiones y los resultados se consolidaron mediante la mediana de las métricas correspondientes.

### 3.2. Matriz experimental

| Experimento | Familia | Falla aplicada | Estrategia de corrección |
|---|---|---|---|
| EXP-01 | Latencia | Latencia artificial con jitter | Caché TTL |
| EXP-02 | Base de datos | Rechazos simulados de conexiones | Reintentos con backoff |
| EXP-03 | Degradación | Carga sintética de CPU | Reducción de intensidad |
| EXP-04 | Disponibilidad | Errores 503 simulados | Reintentos con backoff |

### 3.3. Hipótesis experimentales

Se establecieron las siguientes expectativas para interpretar los resultados:

**H1 — Latencia:** la incorporación de una caché TTL reduciría las operaciones que requieren acceder a la base de datos y, en consecuencia, disminuiría la latencia y aumentaría el throughput frente al escenario con falla.

**H2 — Base de datos:** los reintentos con backoff aumentarían la proporción de operaciones exitosas ante rechazos transitorios, aunque podrían incrementar la latencia.

**H3 — CPU:** la reducción de la carga sintética disminuiría la contención de procesamiento y permitiría recuperar parcialmente la latencia y el throughput.

**H4 — Disponibilidad:** los reintentos con backoff reducirían el número de errores finales ante respuestas 503 simuladas, con un posible costo de rendimiento.

Estas hipótesis se utilizan como criterios de interpretación del diseño experimental. El registro disponible no permite demostrar que todas hayan quedado documentadas formalmente antes de las ejecuciones.

---

## 4. Entorno y reproducibilidad

La experimentación se desarrolló en un entorno local de SupplyGrid con Python y PostgreSQL.

La base de datos de referencia corresponde a los datos del Entregable 2, con las siguientes características:

| Elemento | Valor |
|---|---|
| Semilla de referencia | 42 |
| Registros totales | 4.965.000 |
| Número de tablas | 7 |
| Checksum MD5 | `1c1969f67843450e37fc74df05d87db1` |

La integridad de los datos se verificó mediante el procedimiento de comprobación de E2.

La preparación de parámetros utiliza una semilla definida. Sin embargo, las decisiones probabilísticas de inyección de fallas no garantizan que dos ejecuciones independientes produzcan exactamente los mismos eventos o tiempos.

Por esta razón, la reproducibilidad del experimento debe entenderse como la posibilidad de repetir su procedimiento y comparar sus resultados, no como la reproducción exacta de cada medición.

### 4.1. Métricas

Se utilizaron los siguientes indicadores:

- **p95:** tiempo por debajo del cual se encuentra el 95 % de las latencias medidas.
- **p99:** tiempo por debajo del cual se encuentra el 99 % de las latencias medidas.
- **Throughput:** operaciones exitosas procesadas por segundo.
- **Disponibilidad:** porcentaje de operaciones completadas exitosamente.
- **Errores:** número de operaciones fallidas.
- **CPU del proceso:** porcentaje de utilización del procesador asociado al proceso Python durante el experimento.

En los experimentos de rechazos y errores 503, los percentiles de latencia corresponden a operaciones exitosas. Esto debe considerarse al interpretar los resultados.

### 4.2. Momento 1 · Línea base y Tabla de Objetivos de Rendimiento (E3 ≤ 90% de E2)

En cumplimiento del Momento 1 de la rúbrica, se verificó el volumen inicial de 4.965.000 tuplas con la semilla 42 y el checksum MD5 `1c1969f67843450e37fc74df05d87db1` (`datos/reconstruir_e2.py --verify`). 

A continuación se presenta la tabla canónica de objetivos de rendimiento frente al Entregable 2, calculando $\text{Objetivo E3} = \text{Valor E2} \times 0.90$ para cada métrica aplicable:

| Consulta | Escenario | Métrica | Valor E2 (ms) | Objetivo E3 (≤ 90% de E2) | Línea Base Momento 1 (ms) | ¿Cumple Meta ≤ 90%? | Variación vs E2 |
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

**Checklist del Momento 1 (Rúbrica Sección 10):**
- [x] Corrida canónica del E2 identificada (`RESULTADOS_E2_05.md`).
- [x] Filas por entidad del volumen inicial conocidas (4.965.000 tuplas).
- [x] Seed/parámetros y condiciones conservadas (Seed 42, 200 iteraciones).
- [x] Estado inicial reproducible (`datos/reconstruir_e2.py --verify`).
- [x] Métricas comparables del E2 registradas (`metricas_q1_q5.csv`).
- [x] Objetivo calculado = valor E2 × 0,90 (`tabla_objetivos_e3.csv`).
- [x] Comparación rigurosa conservando consulta, escenario y métrica.

---

## 5. Resultados experimentales

### 5.1. EXP-01 — Latencia artificial y caché TTL

El primer experimento evaluó el impacto de introducir latencia artificial con jitter sobre las operaciones de consulta.

Como estrategia de mitigación se utilizó una caché TTL, con el propósito de reducir las consultas repetidas a la base de datos.

**Resultados:**

| Métrica | Control | Con falla | Con corrección |
|---|---:|---:|---:|
| p95 (ms) | 0,374 | 42,975 | 38,429 |
| p99 (ms) | 0,592 | 46,880 | 43,893 |
| Throughput (ops/s) | 3.810,03 | 32,61 | 57,87 |
| Errores | 0 | 0 | 0 |

La inyección de latencia provocó una degradación considerable frente al escenario de control.

La aplicación de la caché redujo el p95 un **10,58 %** y aumentó el throughput un **77,45 %** respecto al escenario con falla, considerando los cambios calculados sobre las medianas consolidadas.

La estrategia fue efectiva para reducir parcialmente el impacto de la perturbación. No obstante, el sistema permaneció lejos de sus niveles de rendimiento originales.

Por tanto, la corrección no debe interpretarse como una recuperación completa, sino como una mitigación parcial.

**Evidencia gráfica:**

- `experimentos/graficas/EXP-01_p95.png`
- `experimentos/graficas/EXP-01_throughput.png`

### 5.2. EXP-02 — Rechazos simulados de base de datos

El segundo experimento evaluó la respuesta ante rechazos transitorios simulados asociados al agotamiento de conexiones.

Se aplicó una estrategia de reintentos con backoff para ofrecer nuevas oportunidades de ejecución a las operaciones inicialmente rechazadas.

**Resultados:**

| Métrica | Control | Con falla | Con corrección |
|---|---:|---:|---:|
| Disponibilidad (%) | 100 | 73,5 | 97 |
| Errores | 0 | 53 | 6 |
| p95 (ms) | 0,299 | 0,293 | 17,423 |
| Throughput (ops/s) | 5.002,30 | 4.690,93 | 291,81 |

La perturbación redujo la disponibilidad al 73,5 %, mientras que la estrategia de corrección permitió elevarla hasta el 97 %.

Esto representa una recuperación de **23,5 puntos porcentuales** y una reducción del **88,68 %** en el número de errores.

Sin embargo, la recuperación de disponibilidad tuvo un costo importante: el p95 aumentó hasta 17,423 ms y el throughput disminuyó un 93,78 % respecto a la fase con falla.

El resultado evidencia que los reintentos pueden recuperar operaciones transitorias, pero también incrementar el tiempo total de procesamiento.

Es importante precisar que se simularon rechazos asociados al agotamiento del pool; no se produjo ni verificó un agotamiento físico del pool de PostgreSQL.

**Evidencia gráfica:**

- `experimentos/graficas/EXP-02_disponibilidad.png`
- `experimentos/graficas/EXP-02_errores.png`

### 5.3. EXP-03 — Saturación de CPU

El tercer experimento estudió la degradación producida por carga sintética de CPU.

La fase con falla utilizó una intensidad de carga mayor, mientras que la fase de corrección redujo dicha intensidad para evaluar la recuperación de rendimiento.

**Resultados:**

| Métrica | Control | Con falla | Con corrección |
|---|---:|---:|---:|
| CPU del proceso (%) | 20,06 | 99,50 | 50,40 |
| p95 (ms) | 0,178 | 3,090 | 1,086 |
| Throughput (ops/s) | 7.025,03 | 326,02 | 3.757,19 |
| Disponibilidad (%) | 100 | 100 | 100 |

La carga sintética elevó la utilización de CPU del proceso hasta aproximadamente el 99,50 %, acompañada de un aumento de la latencia y una reducción pronunciada del throughput.

Al disminuir la intensidad de la perturbación, el p95 se redujo un **64,86 %** y el throughput aumentó aproximadamente **11,52 veces** frente a la fase con falla.

La disponibilidad permaneció en el 100 % durante las tres fases.

Estos resultados muestran que la contención de procesamiento puede perjudicar considerablemente el rendimiento incluso cuando no se producen errores.

La corrección permitió una recuperación parcial, pero el rendimiento no regresó completamente al nivel de control.

Debe señalarse que la carga se generó sobre el proceso Python del experimento. Por tanto, estos resultados no constituyen una medición directa de saturación de CPU del servidor PostgreSQL.

**Evidencia gráfica:**

- `experimentos/graficas/EXP-03_cpu.png`
- `experimentos/graficas/EXP-03_p95.png`
- `experimentos/graficas/EXP-03_throughput.png`

### 5.4. EXP-04 — Errores 503 simulados

El cuarto experimento evaluó la respuesta de SupplyGrid ante errores de disponibilidad simulados mediante el mecanismo de inyección.

Como estrategia de mitigación se implementaron reintentos con backoff.

**Resultados:**

| Métrica | Control | Con falla | Con corrección |
|---|---:|---:|---:|
| Disponibilidad (%) | 100 | 77 | 98,5 |
| Errores | 0 | 46 | 3 |
| p95 (ms) | 0,260 | 0,257 | 7,068 |
| Throughput (ops/s) | 5.452,51 | 5.339,08 | 389,91 |

La inyección redujo la disponibilidad al 77 %. Con la estrategia de reintentos, esta aumentó hasta el 98,5 %.

La recuperación fue de **21,5 puntos porcentuales**, mientras que el número de errores disminuyó un **93,48 %**.

No obstante, el p95 aumentó hasta 7,068 ms y el throughput disminuyó un 92,70 % frente a la fase con falla.

Esto confirma que una estrategia orientada a mejorar la disponibilidad puede introducir costos considerables en el rendimiento.

Los errores 503 fueron simulados mediante el interceptor de operaciones; el experimento no representa una prueba de tráfico HTTP real.

**Evidencia gráfica:**

- `experimentos/graficas/EXP-04_disponibilidad.png`
- `experimentos/graficas/EXP-04_errores.png`

---

## 6. Comparación de estrategias de corrección

La siguiente tabla sintetiza los principales efectos observados.

| Experimento | Beneficio principal | Costo o limitación |
|---|---|---|
| EXP-01 | Reducción del p95 y aumento del throughput | No recupera el rendimiento de control |
| EXP-02 | Mayor disponibilidad y menos errores | Incremento considerable de latencia y reducción del throughput |
| EXP-03 | Recuperación parcial de latencia y throughput | La corrección consiste en reducir la intensidad de la perturbación |
| EXP-04 | Mayor disponibilidad y menos errores | Incremento de latencia y reducción del throughput |

Los resultados permiten identificar dos comportamientos distintos.

En EXP-01 y EXP-03, las correcciones se orientaron principalmente a recuperar rendimiento. En ambos casos se observaron mejoras frente al escenario con falla.

En EXP-02 y EXP-04, las correcciones se orientaron a recuperar operaciones que inicialmente fallaban. En estos casos aumentó la disponibilidad, pero el sistema necesitó más tiempo para completar las operaciones exitosas.

En consecuencia, **no existe una única métrica suficiente para determinar la efectividad de una estrategia de mitigación**. Una corrección puede ser favorable para la disponibilidad y desfavorable para la latencia.

### 6.1. Matriz Consolidada por Experimento (Rúbrica Sección 8)

| ID | Familia | Falla Inyectada | Configuración | Predicción Previa | Resultado Medido (Mediana) | Evidencia Observable | Aprendizaje Principal |
|:---:|:---:|---|---|---|---|---|---|
| **EXP-01** | Latencia | A1: Latencia fija + jitter | `mean_ms: 25`<br>`jitter_ms: 5`<br>`traffic_pct: 1.0` | El p95 aumentará en ~25–30 ms. La caché TTL absorberá consultas repetidas reduciendo p95 y elevando throughput. | Control: 0,37 ms<br>Falla: 42,98 ms<br>Caché: 38,43 ms<br>Tput: +77,45 % | Logs JSONL con `trace_id`. Gráficas: `EXP-01_p95.png`, `EXP-01_throughput.png`. 97.5% hit rate. | La caché amortigua la degradación del backend, pero no elimina la latencia de las claves que sufren cache miss. |
| **EXP-02** | Base de Datos | B1: Saturación simulada de pool | `rejection_prob: 1.0`<br>`traffic_pct: 0.30` | La disponibilidad caerá en proporción al tráfico (~70%). Reintentos recuperarán disponibilidad a costa de latencia. | Control: 100 %<br>Falla: 73,50 %<br>Mitigado: 97,00 %<br>Errores: 53 $\to$ 6 | Eventos `inyeccion` $\to$ `operacion.error` correlacionados. Gráficas `EXP-02_disponibilidad.png`, `EXP-02_errores.png`. | Los reintentos salvan transacciones transitorias, pero el tiempo de backoff multiplica el p95 $\times 59$. Si la falla es persistente, se requiere un Circuit Breaker. |
| **EXP-03** | Recursos | C1: Carga sintética de CPU | `intensity: 60%`<br>`threads: 2`<br>`duration: 5s` | Contención de CPU degradará p95 y throughput sin provocar errores. Bajar carga recuperará capacidad. | Control CPU: 20 %<br>Falla CPU: 99,5 %<br>Corrección: 50,4 %<br>p95: −64,86 % | Muestreo de CPU del proceso vía `psutil`. Gráficas `EXP-03_cpu.png`, `EXP-03_p95.png`, `EXP-03_throughput.png`. | La saturación de CPU produce lentitud silenciosa sin emitir excepciones de software. Es imprescindible medir métricas de proceso. |
| **EXP-04** | Disponibilidad | D2: Errores HTTP 503 | `status_code: 503`<br>`traffic_pct: 0.25` | Disponibilidad bajará al ~75%. Reintentos recuperarán solicitudes fallidas. | Control: 100 %<br>Falla: 77,00 %<br>Mitigado: 98,50 %<br>Errores: 46 $\to$ 3 | Cadenas correlacionadas `inyeccion` $\to$ `ChaosHttpError`. Gráficas `EXP-04_disponibilidad.png`, `EXP-04_errores.png`. | Las fallas 503 responden efectivamente al retry con backoff exponencial cuando el upstream tiene fallos puntuales. |

---

## 7. Seguridad y validación

La experimentación se realizó utilizando mecanismos de activación y desactivación controlada de fallas.

Al finalizar las ejecuciones se comprobó que la inyección global y las fallas individuales estuvieran deshabilitadas.

Adicionalmente, se ejecutó la suite de aceptación:

```bash
python -m pytest tests/test_chaos_acceptance.py -q
```

El resultado fue:

```text
11 passed in 0.04s
```

Las pruebas verificaron aspectos como configuración predeterminada segura, activación y desactivación sin redespliegue, prioridad del kill switch, recuperación, cobertura de familias, validación de parámetros e integridad de referencias de E2.

Este resultado respalda el funcionamiento de los mecanismos incluidos en la suite. No constituye, por sí solo, una validación exhaustiva de todos los componentes del sistema.

---

## 8. Limitaciones

Los resultados deben interpretarse considerando las siguientes limitaciones:

1. Las pruebas se ejecutaron en un entorno local y no permiten extrapolar directamente los resultados a un despliegue distribuido o de producción.
2. EXP-02 utilizó rechazos simulados y no agotó físicamente el pool de conexiones.
3. EXP-03 generó carga sintética en Python y no midió saturación directa del servidor PostgreSQL.
4. EXP-04 simuló errores 503 mediante interceptación y no mediante solicitudes HTTP reales.
5. Los percentiles de latencia de EXP-02 y EXP-04 corresponden a operaciones exitosas.
6. La semilla utilizada no garantiza reproducibilidad exacta de todas las decisiones probabilísticas de inyección.
7. Las correcciones fueron parciales y no recuperaron completamente los niveles de rendimiento de control.
8. Las mejoras porcentuales presentadas se calcularon a partir de medianas consolidadas, por lo que pueden diferir de las mejoras calculadas individualmente por repetición.
9. Los resultados corresponden a la carga y a las condiciones específicas de cada experimento; no deben interpretarse como una comparación directa de capacidad absoluta entre experimentos.
10. La referencia de rendimiento de E2 y los objetivos de mejora del Entregable 3 requieren una evaluación separada. Las mejoras frente a una falla no demuestran automáticamente el cumplimiento de dichos objetivos.

---

## 9. Conclusiones

Los cuatro experimentos permitieron evaluar el comportamiento de SupplyGrid ante perturbaciones controladas de latencia, base de datos, CPU y disponibilidad.

En el experimento de latencia, la caché TTL permitió reducir parcialmente el impacto de las demoras artificiales, mejorando el p95 y el throughput.

En el experimento de base de datos, los reintentos con backoff recuperaron una proporción importante de las operaciones rechazadas, aunque introdujeron costos significativos de latencia y throughput.

En el experimento de CPU, la reducción de la carga sintética permitió recuperar parcialmente el rendimiento sin que se presentaran errores de disponibilidad.

Finalmente, en el experimento de errores 503 simulados, los reintentos mejoraron considerablemente la disponibilidad, pero también produjeron una degradación importante del rendimiento.

En conjunto, los resultados muestran que **la efectividad de una estrategia de corrección depende de la métrica que se busca proteger**.

Las mitigaciones deben evaluarse mediante indicadores complementarios, considerando simultáneamente latencia, throughput, disponibilidad, errores y recursos.

La experimentación también permitió identificar limitaciones del entorno y del mecanismo de simulación, las cuales deben tenerse presentes al interpretar los resultados y al diseñar futuras pruebas.

---

## 10. Evidencias y artefactos

La evidencia del trabajo se encuentra organizada en las siguientes rutas del repositorio:

| Ruta | Contenido |
|---|---|
| `experimentos/runner.py` | Ejecución de EXP-01 |
| `experimentos/runner_b1.py` | Ejecución de EXP-02 |
| `experimentos/runner_cpu.py` | Ejecución de EXP-03 |
| `experimentos/runner_http.py` | Ejecución de EXP-04 |
| `experimentos/resultados/` | Resultados individuales y resúmenes |
| `experimentos/consolidar.py` | Consolidación automática |
| `experimentos/consolidado/` | Tablas comparativas |
| `experimentos/graficar.py` | Generación de gráficas |
| `experimentos/graficas/` | Nueve gráficas experimentales |
| `experimentos/README.md` | Instrucciones de reproducción |
| `chaos/observabilidad.py` | Logs, métricas y `trace_id` (Persona B) |
| `experimentos/logs/` | JSONL, resumen de señales útiles/no útiles |
| `docs/INFORME_PERSONA_B_E3.md` | Observabilidad, corrección y reexperimentación |

---
