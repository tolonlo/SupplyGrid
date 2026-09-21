package com.supplygrid.supplygrid.logistica.core;

import jakarta.persistence.*;
import java.time.LocalDate;
import java.time.LocalTime;

@Entity
@Table(name = "franjas_descargue", schema = "logistica")
public class FranjaDescargue {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "cedi_id", nullable = false)
    private Short cediId;

    @Column(nullable = false)
    private LocalDate fecha;

    @Column(name = "hora_inicio", nullable = false)
    private LocalTime horaInicio;

    @Column(name = "hora_fin", nullable = false)
    private LocalTime horaFin;

    @Column(nullable = false)
    private Boolean disponible;

    @Column(name = "orden_id")
    private Long ordenId;

    protected FranjaDescargue() {
    }

    public Long getId() {
        return id;
    }

    public Short getCediId() {
        return cediId;
    }

    public LocalDate getFecha() {
        return fecha;
    }

    public LocalTime getHoraInicio() {
        return horaInicio;
    }

    public LocalTime getHoraFin() {
        return horaFin;
    }

    public Boolean getDisponible() {
        return disponible;
    }

    public Long getOrdenId() {
        return ordenId;
    }

    public void reservar(Long ordenId) {
        this.disponible = false;
        this.ordenId = ordenId;
    }
}
