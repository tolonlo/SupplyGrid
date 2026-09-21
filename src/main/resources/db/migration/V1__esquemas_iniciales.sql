-- =====================================================================
-- modelo-fisico.sql
-- SupplyGrid · Entregable 2 · Escalabilidad de los datos
--
-- Reglas respetadas (heredadas del diseño / ADR-01, ADR-02):
--   * Un esquema por módulo. Cero JOIN entre esquemas.
--   * Las relaciones ENTRE módulos se guardan como simple BIGINT
--     (sin FOREIGN KEY), y se resuelven por aplicación (contracts/),
--     nunca por SQL. Las relaciones DENTRO de un mismo módulo sí usan
--     FOREIGN KEY normal, porque no cruzan esquemas.
--   * Cada índice está comentado con la consulta (Q1-Q5) a la que sirve.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS proveedores;
CREATE SCHEMA IF NOT EXISTS catalogo;
CREATE SCHEMA IF NOT EXISTS ordenes;
CREATE SCHEMA IF NOT EXISTS logistica;
-- Esquema transversal de auditoría (append-only). No es un "quinto módulo
-- de negocio": ningún módulo lo consulta vía JOIN, solo escribe eventos.
CREATE SCHEMA IF NOT EXISTS auditoria;

-- =====================================================================
-- MÓDULO proveedores
-- =====================================================================

CREATE TABLE proveedores.proveedores (
    id              BIGSERIAL PRIMARY KEY,
    nombre          VARCHAR(150)    NOT NULL,
    nit             VARCHAR(20)     NOT NULL UNIQUE,
    ciudad          VARCHAR(80),
    fecha_registro  DATE            NOT NULL DEFAULT CURRENT_DATE,
    activo          BOOLEAN         NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE proveedores.proveedores IS
    'Base del sesgo Zipf: el 5% de las filas concentra el 40% de las órdenes.';

CREATE TABLE proveedores.contratos (
    id              BIGSERIAL PRIMARY KEY,
    proveedor_id    BIGINT          NOT NULL
                        REFERENCES proveedores.proveedores(id),
    fecha_inicio    TIMESTAMP       NOT NULL,
    fecha_fin       TIMESTAMP       NOT NULL,
    estado          VARCHAR(20)     NOT NULL DEFAULT 'VIGENTE'
);
-- Q1: "Validar proveedor y contrato vigente por proveedor_id y fecha".
-- Debe responder en <10ms: sin este índice, cada llamada barre TODOS los
-- contratos del proveedor en vez de ir directo a los vigentes.
-- Cubre también el caso borde 2 y 3 (contrato vencido ayer / vence hoy).
CREATE INDEX idx_contratos_proveedor_vigencia
    ON proveedores.contratos (proveedor_id, fecha_inicio, fecha_fin);

-- =====================================================================
-- MÓDULO catalogo
-- =====================================================================

CREATE TABLE catalogo.catalogo_sku (
    id              BIGSERIAL PRIMARY KEY,
    -- proveedor_id y contrato_id pertenecen al módulo `proveedores`.
    -- Se guardan como BIGINT plano (sin FOREIGN KEY) porque cruzan
    -- esquemas: la validación de que existan se hace en la app, no en SQL.
    proveedor_id    BIGINT          NOT NULL,
    contrato_id     BIGINT          NOT NULL,
    sku_codigo      VARCHAR(30)     NOT NULL,
    descripcion     VARCHAR(200),
    precio          NUMERIC(12,2)   NOT NULL,
    fecha_inicio    DATE            NOT NULL,
    fecha_fin       DATE            NOT NULL
);
COMMENT ON TABLE catalogo.catalogo_sku IS
    'El 10% de los SKU aparece en el 60% de las líneas de orden (sesgo).';

-- Q2: "Verificar precio y vigencia de 20 SKU del catálogo negociado de
-- un proveedor". El índice va por proveedor primero porque siempre se
-- filtra por proveedor_id antes de buscar el SKU puntual.
CREATE INDEX idx_catalogo_proveedor_sku
    ON catalogo.catalogo_sku (proveedor_id, sku_codigo);

-- Caso borde 6 (SKU fuera del catálogo negociado del proveedor): la
-- búsqueda inversa usa el mismo índice de arriba, no se necesita otro.

-- =====================================================================
-- MÓDULO ordenes
-- =====================================================================

CREATE TABLE ordenes.ordenes (
    id                  BIGSERIAL PRIMARY KEY,
    -- Cruza a `proveedores`: BIGINT plano, sin FOREIGN KEY.
    proveedor_id        BIGINT          NOT NULL,
    contrato_id         BIGINT          NOT NULL,
    fecha_orden         TIMESTAMP       NOT NULL DEFAULT now(),
    estado              VARCHAR(20)     NOT NULL DEFAULT 'CONFIRMADA',
    -- ADR-05: idempotencia garantizada en la misma transacción de la
    -- orden, sin depender de un caché externo como única fuente de verdad.
    idempotency_key     VARCHAR(64)     NOT NULL UNIQUE
);
COMMENT ON TABLE ordenes.ordenes IS
    'Cabecera. 24 meses de historia. Últimos 3 días hábiles del mes '
    'concentran el 25% de las órdenes (estacionalidad).';

-- Q5: "Últimas 50 órdenes de un proveedor del top 1%, paginadas".
-- DESC porque siempre se pide lo más reciente primero. Este es el índice
-- que más sufre con el hot partition: un proveedor top 1% tiene miles
-- de filas bajo el mismo proveedor_id.
CREATE INDEX idx_ordenes_proveedor_fecha
    ON ordenes.ordenes (proveedor_id, fecha_orden DESC);

-- Q4: "OTIF y fill rate por proveedor y mes, sobre 24 meses". Agrega por
-- mes across TODOS los proveedores, así que necesita poder recorrer por
-- fecha sin ir proveedor por proveedor.
CREATE INDEX idx_ordenes_fecha
    ON ordenes.ordenes (fecha_orden);

CREATE TABLE ordenes.lineas_orden (
    id                  BIGSERIAL PRIMARY KEY,
    -- Mismo esquema que `ordenes`: FOREIGN KEY normal, no cruza módulos.
    orden_id            BIGINT          NOT NULL
                            REFERENCES ordenes.ordenes(id),
    sku_codigo          VARCHAR(30)     NOT NULL,
    cantidad            INTEGER         NOT NULL,
    precio_unitario     NUMERIC(12,2)   NOT NULL
);
COMMENT ON TABLE ordenes.lineas_orden IS
    'Promedio 5 líneas por orden, cola larga hasta 300 (caso borde 1).';

-- Necesario para reconstruir una orden completa y para el cálculo de
-- fill rate (Q4), que suma líneas por orden.
CREATE INDEX idx_lineas_orden_id
    ON ordenes.lineas_orden (orden_id);

-- =====================================================================
-- MÓDULO logistica
-- =====================================================================

CREATE TABLE logistica.franjas_descargue (
    id              BIGSERIAL PRIMARY KEY,
    cedi_id         SMALLINT        NOT NULL,
    fecha           DATE            NOT NULL,
    hora_inicio     TIME            NOT NULL,
    hora_fin        TIME            NOT NULL,
    disponible      BOOLEAN         NOT NULL DEFAULT TRUE,
    -- Cruza a `ordenes` cuando se reserva: BIGINT plano, sin FOREIGN KEY.
    orden_id        BIGINT
);
COMMENT ON TABLE logistica.franjas_descargue IS
    '5 CEDI x 24 meses x franjas/día. Recurso escaso (ADR-04): no se '
    'sobrevende ni se compensa después de asignada.';

-- Q3: "Franjas disponibles de un CEDI para una fecha", con alta
-- concurrencia (varias órdenes peleando por la última franja del día,
-- caso borde 4). Índice parcial: solo indexa filas AÚN disponibles, que
-- es lo único que esta consulta necesita encontrar rápido.
CREATE INDEX idx_franjas_cedi_fecha_disponible
    ON logistica.franjas_descargue (cedi_id, fecha)
    WHERE disponible = TRUE;

-- =====================================================================
-- ESQUEMA TRANSVERSAL auditoria (append-only)
-- =====================================================================

CREATE TABLE auditoria.eventos (
    id              BIGSERIAL PRIMARY KEY,
    entidad         VARCHAR(30)     NOT NULL,
    entidad_id      BIGINT          NOT NULL,
    tipo_evento     VARCHAR(50)     NOT NULL,
    fecha_evento    TIMESTAMP       NOT NULL DEFAULT now(),
    detalle         JSONB
);
COMMENT ON TABLE auditoria.eventos IS
    'Append-only, particionable por mes.';

-- DECISIÓN DE ESCALABILIDAD (para el ítem "modelo físico" de la rúbrica):
-- NO particionamos esta tabla por mes en esta entrega. Con el volumen
-- objetivo (~800k filas) un índice simple por fecha es suficiente y
-- particionar agrega complejidad operativa que no se justifica todavía.
-- Si el volumen crece 10x en una entrega futura, esta tabla ya está
-- preparada para partición por rango de `fecha_evento` sin cambiar el
-- resto del modelo.
CREATE INDEX idx_eventos_fecha
    ON auditoria.eventos (fecha_evento);