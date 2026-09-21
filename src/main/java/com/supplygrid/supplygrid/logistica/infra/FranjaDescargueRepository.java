package com.supplygrid.supplygrid.logistica.infra;

import com.supplygrid.supplygrid.logistica.core.FranjaDescargue;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.Optional;

public interface FranjaDescargueRepository
        extends JpaRepository<FranjaDescargue, Long> {

    @Query(
        value = """
            SELECT *
            FROM logistica.franjas_descargue
            WHERE cedi_id = :cediId
              AND fecha = :fecha
              AND disponible = TRUE
            ORDER BY hora_inicio
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """,
        nativeQuery = true
    )
    Optional<FranjaDescargue> buscarDisponibleParaReservar(
            @Param("cediId") Short cediId,
            @Param("fecha") LocalDate fecha
    );
}
