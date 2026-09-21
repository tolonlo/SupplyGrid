package com.supplygrid.supplygrid.logistica.contracts;

import java.time.LocalDate;

public interface LogisticaClient {

    Long reservarFranja(
            Short cediId,
            LocalDate fecha,
            Long ordenId
    );
}
