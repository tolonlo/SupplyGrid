package com.supplygrid.supplygrid.ordenes.api;

import com.supplygrid.supplygrid.ordenes.core.Orden;
import com.supplygrid.supplygrid.ordenes.infra.OrdenRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * E2-03 (Tom) — version STUB de la ruta transaccional real.
 *
 * Solo inserta la orden. NO valida todavia:
 *   - que el proveedor tenga contrato vigente (contract de proveedores)
 *   - que el SKU este en el catalogo negociado (contract de catalogo)
 *   - que haya franja de descargue disponible (contract de logistica)
 *
 * Esas tres validaciones, dentro de la MISMA transaccion, son el
 * alcance de E2-03b (Valentina). Este stub existe para:
 *   1) medir la 4ta estrategia de carga en E2-04 (ruta transaccional
 *      real vs COPY/INSERT), y
 *   2) la Fase 4 del enunciado (inyeccion incremental de 5000 ordenes
 *      por la app, no por COPY).
 */
@RestController
@RequestMapping("/ordenes-compra")
public class OrdenController {

    private final OrdenRepository ordenRepository;

    public OrdenController(OrdenRepository ordenRepository) {
        this.ordenRepository = ordenRepository;
    }

    @PostMapping("/{proveedorId}/confirmacion")
    @Transactional
    public ResponseEntity<Orden> confirmar(
            @PathVariable Long proveedorId,
            @RequestParam Long contratoId,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {

        Orden existente = ordenRepository.findByIdempotencyKey(idempotencyKey).orElse(null);
        if (existente != null) {
            // Misma llave -> no se duplica, se devuelve la orden que ya existia.
            return ResponseEntity.ok(existente);
        }

        Orden nueva = new Orden(proveedorId, contratoId, idempotencyKey);
        Orden guardada = ordenRepository.save(nueva);
        return ResponseEntity.ok(guardada);
    }
}