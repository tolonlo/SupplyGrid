package com.supplygrid.supplygrid.ordenes.infra;

import com.supplygrid.supplygrid.ordenes.core.Orden;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface OrdenRepository extends JpaRepository<Orden, Long> {

    Optional<Orden> findByIdempotencyKey(String idempotencyKey);

    /*
     * Bloqueo transaccional de PostgreSQL.
     *
     * Dos peticiones con la misma Idempotency-Key se serializan.
     * Llaves diferentes pueden continuar ejecutándose concurrentemente.
     *
     * El lock se libera automáticamente al terminar la transacción.
     */
    @Query(
        value = """
            SELECT pg_advisory_xact_lock(
                hashtextextended(:idempotencyKey, 0)
            )
            """,
        nativeQuery = true
    )
    void bloquearIdempotencyKey(
            @Param("idempotencyKey") String idempotencyKey
    );
}