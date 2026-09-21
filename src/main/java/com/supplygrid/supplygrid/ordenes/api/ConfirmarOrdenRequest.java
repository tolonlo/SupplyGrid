package com.supplygrid.supplygrid.ordenes.api;

import java.time.LocalDate;
import java.util.List;

public record ConfirmarOrdenRequest(
        List<String> skus,
        Short cediId,
        LocalDate fechaDescargue
) {
}
