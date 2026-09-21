package com.supplygrid.supplygrid.proveedores.core;

import com.supplygrid.supplygrid.proveedores.contracts.ProveedorClient;
import com.supplygrid.supplygrid.proveedores.infra.ProveedorRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ProveedorClientImpl implements ProveedorClient {

    private final ProveedorRepository proveedorRepository;

    public ProveedorClientImpl(ProveedorRepository proveedorRepository) {
        this.proveedorRepository = proveedorRepository;
    }

    @Override
    public boolean contratoVigente(
            Long proveedorId,
            Long contratoId,
            LocalDateTime fecha) {

        return proveedorRepository.existeContratoVigente(
                proveedorId,
                contratoId,
                fecha
        );
    }
}