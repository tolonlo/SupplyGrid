package com.supplygrid.supplygrid.ordenes.infra;

import com.supplygrid.supplygrid.ordenes.core.Orden;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface OrdenRepository extends JpaRepository<Orden, Long> {

    // Usado para la idempotencia (ADR-05): si ya existe una orden con
    // esta llave, no se crea otra, se devuelve la que ya estaba.
    Optional<Orden> findByIdempotencyKey(String idempotencyKey);
}