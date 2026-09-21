package com.supplygrid.supplygrid.proveedores.contracts;

import java.time.LocalDateTime;

public interface ProveedorClient {

    boolean contratoVigente(
            Long proveedorId,
            Long contratoId,
            LocalDateTime fecha
    );
}