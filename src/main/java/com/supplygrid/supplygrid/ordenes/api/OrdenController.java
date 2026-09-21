package com.supplygrid.supplygrid.ordenes.api;

import com.supplygrid.supplygrid.catalogo.contracts.CatalogoClient;
import com.supplygrid.supplygrid.logistica.contracts.LogisticaClient;
import com.supplygrid.supplygrid.logistica.contracts.SinFranjaDisponibleException;
import com.supplygrid.supplygrid.ordenes.core.Orden;
import com.supplygrid.supplygrid.ordenes.infra.OrdenRepository;
import com.supplygrid.supplygrid.proveedores.contracts.ProveedorClient;

import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

@RestController
@RequestMapping("/ordenes-compra")
public class OrdenController {

    private final OrdenRepository ordenRepository;
    private final ProveedorClient proveedorClient;
    private final CatalogoClient catalogoClient;
    private final LogisticaClient logisticaClient;

    public OrdenController(
            OrdenRepository ordenRepository,
            ProveedorClient proveedorClient,
            CatalogoClient catalogoClient,
            LogisticaClient logisticaClient) {

        this.ordenRepository = ordenRepository;
        this.proveedorClient = proveedorClient;
        this.catalogoClient = catalogoClient;
        this.logisticaClient = logisticaClient;
    }

    @PostMapping("/{proveedorId}/confirmacion")
    @Transactional
    public ResponseEntity<?> confirmar(
            @PathVariable Long proveedorId,
            @RequestParam Long contratoId,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody ConfirmarOrdenRequest request) {

        // 1. Bloquear esta Idempotency-Key durante la transaccion.
        // Dos solicitudes con la misma llave se procesan una despues de la otra.
        ordenRepository.bloquearIdempotencyKey(idempotencyKey);

        // 2. Comprobar si la orden ya fue creada por una solicitud anterior.
        Orden existente = ordenRepository
                .findByIdempotencyKey(idempotencyKey)
                .orElse(null);

        if (existente != null) {
            return ResponseEntity.ok(existente);
        }

        // 3. Validar contrato vigente.
        LocalDateTime ahora = LocalDateTime.now();

        boolean contratoValido = proveedorClient.contratoVigente(
                proveedorId,
                contratoId,
                ahora
        );

        if (!contratoValido) {
            return ResponseEntity
                    .badRequest()
                    .body("El proveedor no tiene un contrato vigente");
        }

        // 4. Validar que exista al menos un SKU.
        if (request == null
                || request.skus() == null
                || request.skus().isEmpty()) {

            return ResponseEntity
                    .badRequest()
                    .body("La orden debe contener al menos un SKU");
        }

        // 5. Validar todos los SKU contra el catalogo negociado vigente.
        LocalDate fechaActual = ahora.toLocalDate();

        for (String sku : request.skus()) {

            boolean skuValido = catalogoClient.skuNegociadoVigente(
                    proveedorId,
                    contratoId,
                    sku,
                    fechaActual
            );

            if (!skuValido) {
                return ResponseEntity
                        .badRequest()
                        .body("SKU invalido o fuera del catalogo negociado: " + sku);
            }
        }

        // 6. Validar los datos necesarios para reservar la franja.
        if (request.cediId() == null || request.fechaDescargue() == null) {
            return ResponseEntity
                    .badRequest()
                    .body("Debe indicar cediId y fechaDescargue");
        }

        // 7. Crear la orden y forzar el INSERT para obtener su ID.
        Orden nueva = new Orden(
                proveedorId,
                contratoId,
                idempotencyKey
        );

        Orden guardada = ordenRepository.saveAndFlush(nueva);

        // 8. Reservar una franja disponible.
        // Logistica utiliza FOR UPDATE SKIP LOCKED para evitar sobreventa.
        Long franjaId = logisticaClient.reservarFranja(
                request.cediId(),
                request.fechaDescargue(),
                guardada.getId()
        );

        // 9. Si no hay franja, lanzar excepcion.
        // La excepcion provoca rollback de toda la transaccion,
        // incluyendo la orden creada anteriormente.
        if (franjaId == null) {
            throw new SinFranjaDisponibleException(
                    "No hay franjas disponibles para el CEDI y fecha solicitados"
            );
        }

        // 10. Orden y franja quedan confirmadas en la misma transaccion.
        return ResponseEntity.ok(guardada);
    }
}