package com.supplygrid.supplygrid.catalogo.contracts;

import java.time.LocalDate;

public interface CatalogoClient {

    boolean skuNegociadoVigente(
            Long proveedorId,
            Long contratoId,
            String skuCodigo,
            LocalDate fecha
    );
}
