# GUÍA DE ESTRATEGIAS DE MITIGACIÓN, DINÁMICA DE THROUGHPUT Y PROTOCOLO KILL SWITCH
## SupplyGrid Enterprise · Arquitectura de Sistemas Escalables y Resiliencia (E3)

---

### Resumen Ejecutivo

Este documento proporciona una fundamentación técnica y operativa sobre tres pilares esenciales de la arquitectura y la ingeniería de confiabilidad (SRE) en **SupplyGrid**:
1. **Estrategias de Mitigación de Arquitectura:** Mecanismos de defensa activa ante fallas distribuidas (Caché con TTL, Reintentos con Backoff Exponencial y Circuit Breaker Fail-Fast).
2. **Dinámica y Variabilidad del Throughput:** Explicación matemática y de sistemas de por qué la tasa de procesamiento (ops/s) sube, cae o se satura bajo condiciones nominales y de degradación.
3. **Protocolo y Arquitectura del Kill Switch Global:** Fundamento del botón de emergencia, garantía de parada atómica e interrupción cooperativa de perturbaciones de caos.

---

## 1. Estrategias de Mitigación: Mecanismos, Funcionamiento y Comparativa

En sistemas B2B modulares de alta concurrencia, las fallas en nodos dependientes (base de datos, pasarelas tributarias, servicios externos) son inevitables. Las estrategias de mitigación evitan que una degradación localizada se transforme en una falla catastrófica en cascada.

```
                                  [ SOLICITUD CLIENTE ]
                                            │
                                            ▼
                           ┌───────────────────────────────────┐
                           │   ESTRATEGIA 1: CACHÉ CON TTL     │
                           │      ¿Dato en memoria local?      │
                           └─────────────────┬─────────────────┘
                                     SI ┌────┴────┐ NO
                                        │         │
                         [ RETORNA CACHE ]        ▼
                                       ┌────────────────────────────────────┐
                                       │    ESTRATEGIA 3: CIRCUIT BREAKER   │
                                       │      ¿Circuito Abierto (Falla)?    │
                                       └──────────────────┬─────────────────┘
                                                  SI ┌────┴────┐ NO (Cerrado)
                                                     │         │
                           [ FAIL-FAST INMEDIATO ] ◄─┘         ▼
                                                  ┌───────────────────────────────┐
                                                  │ ESTRATEGIA 2: REINTENTOS CON  │
                                                  │       BACKOFF EXPONENCIAL     │
                                                  │      ¿Falla transitoria?      │
                                                  └───────────────┬───────────────┘
                                                           Éxito  │  Fallo repetido
                                                                  ▼
                                                      [ LLAMADA AL MÓDULO/BD ]
```

---

### 1.1. Estrategia 1: Caché con TTL (Time-To-Live)

#### A. ¿Cómo funciona?
La caché almacena en la memoria rápida del proceso (RAM local o Redis) el resultado de operaciones de lectura costosas o de alta frecuencia (como la consulta **Q1: Validación de contratos vigentes por proveedor**).
* **Hit de Caché:** Si la clave existe y el tiempo transcurrido es menor a su $TTL$, la respuesta se devuelve en microsegundos ($< 0.1\text{ ms}$), sin tocar la base de datos ni la red.
* **Miss de Caché:** Si la clave no existe o ya expiró ($t > TTL$), la petición viaja al almacenamiento persistente, obtiene el valor fresco y lo almacena en la caché con un nuevo sello de tiempo.

#### B. Impacto ante Fallas
* **Aislamiento de Base de Datos:** Si la base de datos experimenta saturación de conexiones (**Falla B1: Pool Agotado**) o bloqueos (**Falla B2**), la caché sigue atendiendo las consultas calientes desde memoria, reduciendo el tráfico hacia la base de datos en más de un **80%**.
* **Reducción de Latencia:** Convierte operaciones de I/O de $5\text{ ms}$ o más en lecturas de memoria de $0.05\text{ ms}$.

#### C. Cuándo usarla y Cuándo no
* **Cuándo usarla:** Datos con alta tasa de lectura y baja tasa de mutación (catálogo de productos, contratos de proveedores, listas de precios).
* **Cuándo evitarla:** Datos críticos fuertemente transaccionales donde una lectura rancia signifique inconsistencia financiera (por ejemplo, saldo disponible para debitar una orden).

---

### 1.2. Estrategia 2: Reintentos con Backoff Exponencial y Jitter

#### A. ¿Cómo funciona?
Cuando una operación falla debido a una perturbación transitoria (por ejemplo, un fallo de socket temporal en **Falla D1** o un timeout leve en **Falla A2**), el cliente no se rinde de inmediato; reintenta la operación un número finito de veces ($N = 3$) espaciando progresivamente los intentos mediante **Backoff Exponencial**:

$$T_{\text{espera}} = T_0 \cdot 2^{(\text{intento} - 1)} \pm \text{Jitter}$$

Donde:
* $T_0$ es el intervalo base (ej. $3\text{ ms}$).
* Intento 1 espera: $3\text{ ms}$.
* Intento 2 espera: $6\text{ ms}$.
* Intento 3 espera: $12\text{ ms}$.
* **Jitter aleatorio:** Añade una pequeña variación aleatoria al tiempo de espera para evitar que miles de clientes concurrentes reintenten exactamente en el mismo milisegundo (fenómeno conocido como el *Thundering Herd* o tormenta de reintentos).

#### B. Impacto ante Fallas
* **Recuperación Automática de Fallas Breves:** Resuelve de forma transparente cortes de red o pérdidas de paquetes instantáneas sin que el usuario final note ningún error.
* **Riesgo:** Si el sistema de destino está permanentemente caído (ej. **Falla B1 con 100% de pool muerto**), reintentar 3 veces **triplica la carga** sobre el servicio agonizante y amplifica la latencia del cliente ($T_{\text{total}} = 3 \times \text{timeout}$).

---

### 1.3. Estrategia 3: Circuit Breaker (Cortocircuito / Fail-Fast)

#### A. ¿Cómo funciona?
Inspirado en los disyuntores eléctricos que cortan la corriente cuando hay sobrecarga para evitar un incendio, el Circuit Breaker monitorea la tasa de fallos de un servicio remoto y opera mediante una máquina de tres estados:

```
          [ Estado CERRADO ] (Operación Normal)
                 │
      Fallas consecutivas > Umbral (ej. 4)
                 ▼
          [ Estado ABIERTO ] (Fail-Fast inmediato, sin llamadas)
                 │
      Transcurrido Timeout de Enfriamiento (ej. 3.0s)
                 ▼
         [ Estado SEMI-ABIERTO ] (Envía 1 llamada sonda de prueba)
          ┌──────┴──────┐
       Éxito          Fallo
          │             │
          ▼             ▼
   [ CERRADO ]     [ ABIERTO ]
```

1. **Estado Cerrado (Closed):** El sistema opera con normalidad. Todas las peticiones viajan al servicio de destino. Se contabilizan los errores sucesivos.
2. **Estado Abierto (Open):** Si los fallos consecutivos superan el umbral (ej. 4 errores seguidos), el circuito se abre. **A partir de ese instante, ninguna petición toca el servicio caído:** el Circuit Breaker rechaza la petición de forma instantánea (**Fail-Fast en $< 0.05\text{ ms}$**).
3. **Estado Semi-Abierto (Half-Open):** Tras un periodo de enfriamiento (ej. 3 segundos), el circuito deja pasar una única llamada de prueba. Si esa llamada responde con éxito, el servicio se considera recuperado y el circuito se cierra. Si falla, el circuito vuelve a abrirse de inmediato.

#### B. Beneficios Arquitectónicos
* **Protección del Servicio Caído:** Permite que una base de datos o servicio externo saturado se recupere en reposo sin seguir recibiendo miles de peticiones por segundo.
* **Ahorro de Recursos en el Cliente:** Los hilos del cliente no se quedan bloqueados esperando timeouts de 10 o 30 segundos; son liberados de inmediato con una excepción controlada (`CircuitoAbierto`).

---

### Cuadro Comparativo de Mitigaciones

| Característica | Caché con TTL | Reintentos con Backoff | Circuit Breaker |
| :--- | :--- | :--- | :--- |
| **Objetivo Principal** | Reducir carga de lectura y latencia base | Superar fallas transitorias breves | Prevenir colapso en cascada y fail-fast |
| **Costo Computacional** | Memoria RAM para almacenamiento local | Mayor latencia acumulada en reintentos | Mínimo (evaluación atómica de contadores) |
| **Comportamiento ante Caída Total** | Sigue respondiendo lecturas calientes | **Negativo:** Sobrecarga el nodo caído | **Excelente:** Corta el tráfico al 100% |
| **Latencia p99** | La disminuye drásticamente ($< 0.1\text{ ms}$) | La incrementa durante los reintentos | La reduce a nivel microsegundo (fail-fast) |

---

## 2. Dinámica del Throughput: ¿Por qué y Cómo Puede Variar?

El **Throughput** (Rendimiento o Tasa) es la métrica RED que mide el número de **operaciones completadas con éxito por unidad de tiempo** (expresado en `ops/s` o `req/s`).

### 2.1. Factores Fundamentales de Variabilidad

#### 1. La Ley de Little (Relación Latencia - Concurrencia - Throughput)
En teoría de colas y arquitectura de software, la capacidad de un sistema está gobernada por la Ley de Little:

$$L = \lambda \cdot W \quad \implies \quad \lambda = \frac{L}{W}$$

Donde:
* $\lambda$ = Throughput del sistema (ops/s).
* $L$ = Nivel de concurrencia (número de clientes o hilos activos en paralelo).
* $W$ = Latencia media o tiempo de servicio por petición (segundos).

> **Principio Clave:** Si la concurrencia $L$ es fija (por ejemplo, 10 clientes concurrentes) y se inyecta la **Falla A1 (Latencia inducida de 35 ms)**, el tiempo de servicio $W$ aumenta dramáticamente. Como consecuencia matemática directa, **el Throughput $\lambda$ debe descender** porque cada hilo tarda más tiempo en completar una operación antes de poder iniciar la siguiente.

#### 2. Saturación de Recursos Hardware (Métricas USE)
* **Saturación de CPU (Falla C1):** Cuando la CPU alcanza niveles de saturación ($> 70\% - 90\%$), el planificador del sistema operativo debe alternar contextos entre cientos de hilos (*context switching*). El costo de conmutación consume ciclos útiles de cómputo, provocando que el Throughput se degrade o se estanque.
* **Presión de Memoria (Falla C2):** Si la memoria RAM se agota, el sistema operativo recurre a la paginación en disco (*swapping*), multiplicando los tiempos de acceso por miles y desplomando el Throughput.

#### 3. Cuellos de Botella en Recursos Compartidos (Base de Datos)
* **Agotamiento del Pool de Conexiones (Falla B1):** Si el pool de conexiones de PostgreSQL se llena, los nuevos hilos entrantes son rechazados de inmediato o deben esperar a que se libere un slot. Las peticiones rechazadas reducen el Throughput de operaciones exitosas a cero.
* **Bloqueos de Fila o Tabla (Falla B2 / Lock Contention):** Cuando dos o más transacciones intentan modificar o consultar con lock el mismo registro de un proveedor (ej. `proveedor_1`), las transacciones se forman en una cola serial. La concurrencia paralela colapsa a concurrencia serial ($1\text{ op a la vez}$), reduciendo el Throughput.

#### 4. Ruptura de Red o Indisponibilidad de Enlace (Falla D1)
En un corte de red, las peticiones ni siquiera logran establecer el handshake TCP. El throughput efectivo de datos de negocio cae a cero mientras dure la desconexión.

#### 5. Efecto Paradoja del Circuit Breaker en el Throughput
Cuando el **Circuit Breaker** entra en estado `ABIERTO`:
* El throughput de **operaciones de negocio reales** cae a cero.
* Sin embargo, el throughput de **peticiones atendidas con fallo rápido** puede aumentar, dado que el sistema descarta las peticiones en $0.02\text{ ms}$, permitiendo que el servidor responda miles de rechazos por segundo sin consumir CPU ni I/O.

---

## 3. El Kill Switch SRE: Definición, Función y Arquitectura

### 3.1. ¿Qué es el Kill Switch?
En Ingeniería del Caos (Chaos Engineering) y Confiabilidad de Sitios (SRE), el **Kill Switch** es el **mecanismo de parada de emergencia con máxima prioridad**. Es una salvaguarda innegociable que garantiza que cualquier experimento o falla inyectada pueda ser abortada de forma instantánea e irreversible por el operador humano o por un monitor automático de salud.

### 3.2. ¿Por qué es Obligatorio?
* **Seguridad Operacional:** Evita que una prueba de caos descontrolada cause indisponibilidad irreversible, corrupción de datos o viole de forma permanente los Acuerdos de Nivel de Servicio (SLA) con clientes B2B.
* **Prioridad Absoluta sobre la Configuración:** Sin importar cuántas fallas estén encendidas en el catálogo (`A1`, `B1`, `C1`, `D2`), el Kill Switch tiene precedencia jerárquica sobre cualquier otra regla de negocio o interceptor.

### 3.3. ¿Cómo está Implementado en SupplyGrid?

En la arquitectura de SupplyGrid, el Kill Switch no es una simple variable booleana, sino un protocolo coordinado en tres niveles:

```
                           [ BOTÓN KILL SWITCH SRE ]
                                      │
                                      ▼
                        ChaosConfigManager.set_kill_switch(True)
                                      │
             ┌────────────────────────┼────────────────────────┐
             ▼                        ▼                        ▼
     [ 1. HILOS CPU ]        [ 2. MEMORIA RAM ]      [ 3. INTERCEPTOR ]
   stop_event.set()        buffers.clear()         is_kill_switch_active()
   Hilos de quemado        Búferes de 250MB        Cortocircuito inmediato:
   finalizan de inmediato  liberados al GC         bypassa todo el caos
```

1. **Comprobación Prioritaria en Microsegundos:**  
   Todo interceptor de llamadas (`intercept_operation`) y toda verificación de inyección evalúa al inicio:
   ```python
   if self.is_kill_switch_active():
       return operation(*args, **kwargs)
   ```
   Si el Kill Switch está en `True`, el motor de caos se vuelve completamente transparente en $0.001\text{ ms}$ y permite el paso limpio de las transacciones sin aplicar delays, errores ni perturbaciones.
2. **Interrupción Cooperativa en Delays:**  
   Las fallas que inducen latencia artificial (**A1**, **B2**) no realizan un `time.sleep()` rígido. En su lugar, dividen la espera en fragmentos microscópicos de $10\text{ ms}$ y verifican en cada paso si el Kill Switch fue accionado. Si el operador presiona el Kill Switch a la mitad de una espera de $100\text{ ms}$, la espera se interrumpe de inmediato.
3. **Parada Forzada de Perturbaciones de Hardware:**  
   Invoca `stop_all_disturbances()`, el cual activa los `Event` de parada que liquidan los hilos quemadores de CPU (**C1**) y desreferencia los arreglos de memoria (**C2**) para que el recolector de basura de la JVM o del proceso los recupere al instante.
4. **Trazabilidad y Auditoría:**  
   Emite un evento de telemetría estructurado (`obs.kill_switch("activado")`) con sello de tiempo para dejar constancia forense en las bitácoras del sistema.

---

### Resumen para Sustentación ante el Profesor

| Pregunta del Profesor | Respuesta Rápida y Fundamentada |
| :--- | :--- |
| *¿Por qué varía el Throughput?* | Por la Ley de Little: al subir la latencia por una falla ($W$), el Throughput ($\lambda$) desciende para una concurrencia fija. También cae por saturación de CPU/pool o cortes de red. |
| *¿Qué diferencia hay entre Retry y Circuit Breaker?* | Retry insiste ante fallas leves/transitorias; Circuit Breaker corta el paso ante fallas masivas para no saturar al nodo caído y evitar la caída en cascada. |
| *¿Por qué la Caché protege la base de datos?* | Absorbe hasta el 80% de las consultas repetitivas (Q1) en memoria RAM ($< 0.1\text{ ms}$), aislando la base de datos de picos de carga y de agotamiento de conexiones. |
| *¿Qué garantiza el Kill Switch?* | Parada de emergencia atómica: apaga de inmediato los hilos de CPU, libera la memoria y hace que todo el tráfico pase limpio sin perturbaciones. |
