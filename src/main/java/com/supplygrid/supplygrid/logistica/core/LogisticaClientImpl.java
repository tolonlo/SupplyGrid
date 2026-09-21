package com.supplygrid.supplygrid.logistica.core;

import com.supplygrid.supplygrid.logistica.contracts.LogisticaClient;
import com.supplygrid.supplygrid.logistica.infra.FranjaDescargueRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class LogisticaClientImpl implements LogisticaClient {

    private final FranjaDescargueRepository franjaRepository;

    public LogisticaClientImpl(
            FranjaDescargueRepository franjaRepository) {
        this.franjaRepository = franjaRepository;
    }

    @Override
    public Long reservarFranja(
            Short cediId,
            LocalDate fecha,
            Long ordenId) {

        FranjaDescargue franja = franjaRepository
                .buscarDisponibleParaReservar(cediId, fecha)
                .orElse(null);

        if (franja == null) {
            return null;
        }

        franja.reservar(ordenId);
        franjaRepository.save(franja);

        return franja.getId();
    }
}
