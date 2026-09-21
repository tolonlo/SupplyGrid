package com.supplygrid.supplygrid.ordenes.api;

import com.supplygrid.supplygrid.logistica.contracts.SinFranjaDisponibleException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class OrdenExceptionHandler {

    @ExceptionHandler(SinFranjaDisponibleException.class)
    public ResponseEntity<String> manejarSinFranja(
            SinFranjaDisponibleException ex) {

        return ResponseEntity
                .badRequest()
                .body(ex.getMessage());
    }
}
