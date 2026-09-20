-- =====================================================================
-- V2__modulith_event_publication.sql
--
-- Tabla tecnica que necesita Spring Modulith para registrar los
-- eventos que se mandan entre modulos (ej: OrdenCreadaEvent). No es
-- una tabla de negocio, por eso va en su propia migracion, separada
-- de modelo-fisico.sql (que es solo el diseno que le mostramos al
-- profesor).
--
-- Columnas segun el esquema oficial de Spring Modulith 2.x para
-- PostgreSQL (incluye status/completion_attempts/last_resubmission_date,
-- necesarias para el reintento de eventos fallidos).
-- =====================================================================

CREATE TABLE event_publication (
    id                          UUID            NOT NULL,
    completion_date             TIMESTAMP WITH TIME ZONE,
    event_type                  VARCHAR(512)    NOT NULL,
    listener_id                 VARCHAR(512)    NOT NULL,
    publication_date            TIMESTAMP WITH TIME ZONE NOT NULL,
    serialized_event            VARCHAR(4000)   NOT NULL,
    status                      VARCHAR(20),
    completion_attempts         INT,
    last_resubmission_date      TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id)
);

CREATE INDEX event_publication_by_listener_id_and_serialized_event_idx
    ON event_publication (listener_id, serialized_event);

CREATE INDEX event_publication_by_completion_date_idx
    ON event_publication (completion_date);