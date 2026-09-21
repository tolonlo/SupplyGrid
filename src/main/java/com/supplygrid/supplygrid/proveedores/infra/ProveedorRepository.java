package com.supplygrid.supplygrid.proveedores.infra;

import com.supplygrid.supplygrid.proveedores.core.Contrato;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;

public interface ProveedorRepository extends JpaRepository<Contrato, Long> {

    @Query("""
        SELECT COUNT(c) > 0
        FROM Contrato c
        WHERE c.id = :contratoId
          AND c.proveedorId = :proveedorId
          AND c.estado = 'VIGENTE'
          AND c.fechaInicio <= :fecha
          AND c.fechaFin >= :fecha
        """)
    boolean existeContratoVigente(
            @Param("proveedorId") Long proveedorId,
            @Param("contratoId") Long contratoId,
            @Param("fecha") LocalDateTime fecha
    );
}