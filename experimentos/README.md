# SupplyGrid — Diseño experimental y evaluación de mitigaciones (E3)

## 1. Objetivo

Evaluar el comportamiento de SupplyGrid ante cuatro familias de fallas controladas y analizar el efecto de estrategias de corrección sobre la latencia, el throughput, la disponibilidad y el consumo de recursos.

Los experimentos siguen tres fases:

1. Control: ejecución sin inyección de fallas.
2. Falla: ejecución con perturbación activa.
3. Corrección: ejecución con perturbación y estrategia de mitigación.

Se realizaron cinco repeticiones por experimento.

## 2. Experimentos

| ID | Familia | Falla | Corrección |
|---|---|---|---|
| EXP-01 | Latencia | A1: latencia artificial con jitter | Caché TTL |
| EXP-02 | Base de datos | B1: rechazos simulados asociados al pool | Reintentos con backoff |
| EXP-03 | Degradación | C1: carga sintética de CPU | Reducción de intensidad de carga |
| EXP-04 | Disponibilidad | D2: respuestas 503 simuladas | Reintentos con backoff |

Los experimentos se ejecutaron sobre la consulta Q1 y el entorno local de SupplyGrid.

## 3. Requisitos

- Python 3.14 o una versión compatible con las dependencias del proyecto.
- PostgreSQL configurado con la base de datos de SupplyGrid.
- Entorno virtual de Python.
- Dependencias utilizadas por el proyecto.
- `psutil` para las mediciones de CPU.
- `matplotlib` para generar las gráficas.
- `pytest` para ejecutar las pruebas de aceptación.

Instalación de dependencias adicionales de experimentación:

```bash
python -m pip install psutil matplotlib pytest
```

La ejecución requiere que PostgreSQL esté disponible y que los datos de E2 hayan sido cargados.

## 4. Verificación de la base de datos

Para verificar el estado de los datos de referencia:

```bash
python datos/reconstruir_e2.py --verify
```

La referencia histórica utilizada corresponde a:

- Semilla: 42.
- Total de registros: 4.965.000.
- Tablas: 7.
- Checksum MD5 de referencia:
  `1c1969f67843450e37fc74df05d87db1`.

La verificación de E2 y los experimentos de E3 cumplen funciones distintas: la primera comprueba la referencia de datos; los segundos miden perturbaciones y correcciones.

## 5. Ejecución de experimentos

Desde la raíz del repositorio:

### EXP-01 — Latencia

```bash
python -m experimentos.runner
```

### EXP-02 — Base de datos

```bash
python -m experimentos.runner_b1
```

### EXP-03 — CPU

```bash
python -m experimentos.runner_cpu
```

### EXP-04 — Disponibilidad

```bash
python -m experimentos.runner_http
```

Los resultados se almacenan en:

```text
experimentos/resultados/
```

Cada experimento dispone de cinco archivos de repetición y un resumen consolidado correspondiente a la ejecución definitiva del 19 de octubre de 2026.

## 6. Consolidación

Para generar las tablas comparativas:

```bash
python -m experimentos.consolidar
```

Archivos generados:

```text
experimentos/consolidado/consolidado_experimentos.csv
experimentos/consolidado/comparacion_mitigaciones.csv
experimentos/consolidado/consolidado_experimentos.json
```

El consolidado utiliza la mediana de las cinco repeticiones para cada métrica.

Los cambios porcentuales del archivo comparativo se calculan a partir de esas medianas, no como la mediana de los cambios emparejados de cada repetición.

## 7. Gráficas

Para generar las gráficas:

```bash
python -m experimentos.graficar
```

Las imágenes se guardan en:

```text
experimentos/graficas/
```

Se generan nueve gráficas:

- EXP-01: p95 y throughput.
- EXP-02: disponibilidad y errores.
- EXP-03: p95, throughput y CPU del proceso.
- EXP-04: disponibilidad y errores.

## 8. Resultados

### EXP-01

| Métrica | Control | Falla | Corrección |
|---|---:|---:|---:|
| p95 (ms) | 0.374 | 42.975 | 38.429 |
| p99 (ms) | 0.592 | 46.880 | 43.893 |
| Throughput (ops/s) | 3810.03 | 32.61 | 57.87 |

La caché redujo el p95 un 10.58 % y aumentó el throughput un 77.45 % frente a la falla.

### EXP-02

| Métrica | Control | Falla | Corrección |
|---|---:|---:|---:|
| Disponibilidad (%) | 100 | 73.5 | 97 |
| Errores | 0 | 53 | 6 |
| p95 (ms) | 0.299 | 0.293 | 17.423 |
| Throughput (ops/s) | 5002.30 | 4690.93 | 291.81 |

Los reintentos aumentaron la disponibilidad en 23.5 puntos porcentuales y redujeron los errores un 88.68 %, a costa de mayor latencia y menor throughput.

### EXP-03

| Métrica | Control | Falla | Corrección |
|---|---:|---:|---:|
| CPU del proceso (%) | 20.06 | 99.50 | 50.40 |
| p95 (ms) | 0.178 | 3.090 | 1.086 |
| Throughput (ops/s) | 7025.03 | 326.02 | 3757.19 |

Reducir la carga sintética disminuyó el p95 un 64.86 % y aumentó el throughput aproximadamente 11.52 veces frente a la falla.

### EXP-04

| Métrica | Control | Falla | Corrección |
|---|---:|---:|---:|
| Disponibilidad (%) | 100 | 77 | 98.5 |
| Errores | 0 | 46 | 3 |
| p95 (ms) | 0.260 | 0.257 | 7.068 |
| Throughput (ops/s) | 5452.51 | 5339.08 | 389.91 |

Los reintentos aumentaron la disponibilidad en 21.5 puntos porcentuales y redujeron los errores un 93.48 %, con una degradación significativa del rendimiento.

## 9. Limitaciones

1. EXP-02 utiliza rechazos simulados; no agota físicamente el pool de conexiones PostgreSQL.
2. EXP-03 aplica carga sintética sobre el proceso Python; no mide saturación directa de CPU del servidor PostgreSQL.
3. EXP-04 inyecta errores 503 simulados mediante el interceptor; no realiza solicitudes HTTP reales.
4. Los resultados de latencia de EXP-02 y EXP-04 corresponden a operaciones exitosas.
5. Los experimentos se realizaron en un entorno local; no representan automáticamente el comportamiento de un despliegue distribuido.
6. La semilla 42 se utiliza en la preparación de parámetros, pero las decisiones probabilísticas de inyección no garantizan reproducibilidad bit a bit.
7. Las correcciones mejoraron determinadas métricas, pero no recuperaron completamente el rendimiento de control.
8. Las comparaciones porcentuales del consolidado se calculan sobre medianas de repetición.

## 10. Seguridad y recuperación

Las fallas deben ejecutarse únicamente en un entorno de pruebas controlado.

Después de cada ejecución se debe comprobar que las perturbaciones hayan finalizado y que ninguna falla permanezca activa.

Comando de verificación:

```bash
python -m chaos.cli status
```

La configuración segura tiene:

- Inyección global deshabilitada.
- Fallas individuales deshabilitadas.
- Sin perturbaciones activas.

El mecanismo de kill switch permite interrumpir las perturbaciones y las pruebas de aceptación comprueban su prioridad y recuperación.

## 11. Pruebas de aceptación

Ejecutar:

```bash
python -m pytest tests/test_chaos_acceptance.py -v
```

Resultado verificado:

```text
11 passed in 0.09s
```

Las pruebas cubren configuración segura, activación y desactivación, kill switch, recuperación, cobertura de familias, validación de parámetros, inferencia de targets SQL y verificaciones de reproducibilidad de E2.

## 12. Conclusión

Los cuatro experimentos permitieron observar degradaciones y evaluar correcciones bajo perturbaciones controladas.

Las estrategias de caché y reducción de carga mejoraron el rendimiento respecto al escenario con falla, aunque sin recuperar completamente el control.

Los reintentos mejoraron la disponibilidad frente a rechazos y errores simulados, pero incrementaron la latencia y redujeron significativamente el throughput.

Los resultados muestran que una mitigación debe evaluarse considerando varias métricas simultáneamente y no únicamente la reducción de errores.