package com.supplygrid.supplygrid.catalogo.infra;

import com.supplygrid.supplygrid.catalogo.core.CatalogoSku;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;

public interface CatalogoRepository extends JpaRepository<CatalogoSku, Long> {

    @Query("""
        SELECT COUNT(c) > 0
        FROM CatalogoSku c
        WHERE c.proveedorId = :proveedorId
          AND c.contratoId = :contratoId
          AND c.skuCodigo = :skuCodigo
          AND c.fechaInicio <= :fecha
          AND c.fechaFin >= :fecha
        """)
    boolean existeSkuNegociadoVigente(
            @Param("proveedorId") Long proveedorId,
            @Param("contratoId") Long contratoId,
            @Param("skuCodigo") String skuCodigo,
            @Param("fecha") LocalDate fecha
    );
}
