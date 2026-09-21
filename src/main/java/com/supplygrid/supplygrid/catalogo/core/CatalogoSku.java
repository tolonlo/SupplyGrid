package com.supplygrid.supplygrid.catalogo.core;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDate;

@Entity
@Table(name = "catalogo_sku", schema = "catalogo")
public class CatalogoSku {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "proveedor_id", nullable = false)
    private Long proveedorId;

    @Column(name = "contrato_id", nullable = false)
    private Long contratoId;

    @Column(name = "sku_codigo", nullable = false)
    private String skuCodigo;

    private String descripcion;

    private BigDecimal precio;

    @Column(name = "fecha_inicio", nullable = false)
    private LocalDate fechaInicio;

    @Column(name = "fecha_fin", nullable = false)
    private LocalDate fechaFin;

    protected CatalogoSku() {
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

    public String getSkuCodigo() {
        return skuCodigo;
    }

    public LocalDate getFechaInicio() {
        return fechaInicio;
    }

    public LocalDate getFechaFin() {
        return fechaFin;
    }
}
