# Resumen de observabilidad

Eventos totales: **1283**.
Cadenas inyección→error correlacionadas por `trace_id`: **327**.

## Conteos

| Evento | Cantidad |
|---|---:|
| `inyeccion` | 608 |
| `operacion` | 327 |
| `reintento` | 134 |
| `perturbaciones_detenidas` | 46 |
| `fase_inicio` | 44 |
| `fase_fin` | 43 |
| `circuito` | 23 |
| `falla_activada` | 19 |
| `falla_desactivada` | 19 |
| `obs_iniciada` | 11 |
| `kill_switch` | 9 |

## Inyecciones por falla

| Falla | Inyecciones |
|---|---:|
| `A1_latencia_jitter` | 276 |
| `B1_pool_agotado` | 332 |

## Ejemplos de localización (trace_id)

| trace_id | fault_id | error | experimento | fase |
|---|---|---|---|---|
| `4d33d4127419…` | `B1_pool_agotado` | `ChaosDatabaseError` | DEMO-02 | con_falla |
| `6002e9d718af…` | `B1_pool_agotado` | `ChaosDatabaseError` | DEMO-02 | con_falla |
| `5b6d58e47668…` | `B1_pool_agotado` | `ChaosDatabaseError` | DEMO-02 | con_falla |
| `9e9dd5101c28…` | `B1_pool_agotado` | `ChaosDatabaseError` | DEMO-02 | con_falla |
| `22df4f87712b…` | `B1_pool_agotado` | `ChaosDatabaseError` | DEMO-02 | con_falla |
| `3864709b6b4e…` | `B1_pool_agotado` | `ChaosDatabaseError` | DEMO-02 | con_falla |
| `fa04debcef2e…` | `B1_pool_agotado` | `ChaosDatabaseError` | DEMO-02 | con_falla |
| `071184d3cf3a…` | `B1_pool_agotado` | `ChaosDatabaseError` | DEMO-02 | con_falla |

## Señales útiles

- `inyeccion`: Localiza la falla (fault_id + target) en el instante en que se aplicó.
- `operacion`: Une el error observado (tipo, duración, target) con el trace_id.
- `reintento`: Muestra si la corrección actuó y cuántas veces.
- `circuito`: Explica rechazos rápidos cuando el error deja de ser transitorio.
- `falla_activada`: Marca el inicio controlado de la perturbación.
- `falla_desactivada`: Marca la reversión; permite comprobar el kill/reset.
- `kill_switch`: Confirma que la prioridad absoluta quedó registrada.
- `fase_inicio`: Delimita control / falla / corrección para comparar.
- `fase_fin`: Cierra la ventana de medición con duración y recursos.

## Señales poco útiles o ruidosas

- `obs_iniciada`: Solo confirma que el canal existe; no localiza una falla.
- `cache_hit`: En modo 'todas' inunda el log y no distingue causa.
- `cache_miss`: Igual que cache_hit: el hit_rate consolidado basta.
- `perturbaciones_detenidas`: Aparece al limpiar hilos; no implica kill switch real.

Una señal es útil si permite detectar el síntoma, localizar la falla (fault_id/target/fase) y explicar la corrección. Es poco útil si no distingue causa o si su volumen oculta la secuencia causal.
