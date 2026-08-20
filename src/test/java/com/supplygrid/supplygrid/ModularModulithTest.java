package com.supplygrid.supplygrid;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;

class ModularModulithTest {

    ApplicationModules modules = ApplicationModules.of(SupplygridApplication.class);

    @Test
    void verifiesModularStructure() {
        // verifica que lo smodulos no pasen fronteras ilegales
        modules.verify();
    }
}
