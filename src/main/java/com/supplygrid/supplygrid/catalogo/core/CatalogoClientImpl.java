package com.supplygrid.supplygrid.catalogo.core;

import com.supplygrid.supplygrid.catalogo.contracts.CatalogoClient;
import com.supplygrid.supplygrid.catalogo.infra.CatalogoRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class CatalogoClientImpl implements CatalogoClient {

    private final CatalogoRepository catalogoRepository;

    public CatalogoClientImpl(CatalogoRepository catalogoRepository) {
        this.catalogoRepository = catalogoRepository;
    }

    @Override
    public boolean skuNegociadoVigente(
            Long proveedorId,
            Long contratoId,
            String skuCodigo,
            LocalDate fecha) {

        return catalogoRepository.existeSkuNegociadoVigente(
                proveedorId,
                contratoId,
                skuCodigo,
                fecha
        );
    }
}
