package com.supplygrid.supplygrid.ordenes.core;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

/**
 * Mapea 1 a 1 con la tabla ordenes.ordenes creada por
 * src/main/resources/db/migration/V1__esquemas_iniciales.sql (E2-01).
 * ddl-auto=validate: si esta clase no coincide con la tabla, la app no
 * arranca. Es intencional, asi detectamos el desfase de una vez.
 */
@Entity
@Table(name = "ordenes", schema = "ordenes")
public class Orden {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "proveedor_id", nullable = false)
    private Long proveedorId;

    @Column(name = "contrato_id", nullable = false)
    private Long contratoId;

    @Column(name = "fecha_orden", nullable = false)
    private LocalDateTime fechaOrden;

    @Column(name = "estado", nullable = false)
    private String estado;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    protected Orden() {
        // JPA
    }

    public Orden(Long proveedorId, Long contratoId, String idempotencyKey) {
        this.proveedorId = proveedorId;
        this.contratoId = contratoId;
        this.fechaOrden = LocalDateTime.now();
        // E2-03 (stub): estado fijo "CONFIRMADA", sin validar nada
        // todavia. E2-03b (Valentina) agrega los estados de rechazo
        // reales cuando meta las validaciones de negocio.
        this.estado = "CONFIRMADA";
        this.idempotencyKey = idempotencyKey;
    }

    public Long getId() {
        return id;
    }

    public Long getProveedorId() {
        return proveedorId;
    }

    public Long getContratoId() {
        return contratoId;
    }

    public LocalDateTime getFechaOrden() {
        return fechaOrden;
    }

    public String getEstado() {
        return estado;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }
}