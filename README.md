# SupplyGrid - Portal B2B (Monolito Modular)

Repositorio base para la gestión concurrente de proveedores, catálogo, órdenes y logística de la cadena de retail.

---

## 🛑 Reglas Innegociables de Arquitectura (¡LEER ANTES DE PROGRAMAR!)

Este proyecto está construido estrictamente como un **monolito modular**. Para evitar que el profesor nos baje la nota, todos debemos cumplir estas reglas a rajatabla:

1. **Aislamiento de Módulos:** El sistema está dividido en cuatro módulos principales: `proveedores`, `catalogo`, `ordenes` y `logistica`. Está **prohibido** mezclar lógica de un módulo en otro.
2. **Cero JOINs entre esquemas:** Cada módulo posee su propio esquema de base de datos independiente. No se pueden hacer consultas que crucen tablas de esquemas diferentes.
3. **Comunicación exclusiva por Contracts:** Si tu módulo necesita información de otro módulo, **solo** puedes importar y utilizar lo que esté expuesto dentro de la carpeta `contracts/` de ese módulo. Nunca importes clases internas de `api`, `core` o `infra` de otros módulos.
4. **Verificación de fronteras:** Spring Modulith está instalado en el proyecto. Si rompes alguna regla de importación, el programa fallará automáticamente al compilar en el CI.

---

### ⚠️ Nota sobre Relaciones entre Módulos
Para cumplir con la regla de "Cero JOINs entre esquemas", queda estrictamente prohibido utilizar anotaciones `@OneToMany`, `@ManyToOne` o `@ManyToMany` entre entidades que pertenezcan a módulos diferentes.

*   **Si necesitas relacionar algo:** No uses llaves foráneas (`FOREIGN KEY`). En su lugar, utiliza identificadores (IDs) simples (por ejemplo, `Long proveedorId`) y resuelve la comunicación llamando al `contract` del otro módulo o mediante eventos.

---

## 📂 Estructura Interna por Módulo

Cada uno de los cuatro módulos replica la misma arquitectura interna en sus subcarpetas:
*   `api/`: Controladores web y endpoints REST.
*   `core/`: Lógica de negocio pura (Servicios).
*   `infra/`: Conexión a base de datos y repositorios (`JpaRepository`).
*   `contracts/`: Interfaces públicas y eventos permitidos para el consumo de otros módulos.

---

## 🚀 Cómo levantar el proyecto localmente

1. **Levantar la Base de Datos:**
   Asegúrate de tener Docker instalado y ejecuta en la raíz del proyecto:
   ```bash
   docker-compose up -d
