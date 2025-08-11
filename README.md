# Solución de Integración Excel-SQL Server

Una solución robusta de grado de producción para la integración segura de datos entre archivos Excel y SQL Server, desarrollada en Python con interfaz gráfica Tkinter.

## Características Principales

- **Autenticación Segura**: Sistema de login con validación contra SQL Server y protección contra fuerza bruta
- **Integración Dinámica de Excel**: Mapeo automático de hojas de trabajo a tablas SQL con coincidencia difusa de columnas
- **Operaciones CRUD Seguras**: Consultas parametrizadas para prevenir inyección SQL
- **Sistema de Auditoría Completo**: Registro detallado de todas las operaciones con pista de auditoría
- **Interfaz de Usuario Profesional**: GUI Tkinter con barras de progreso y manejo de errores
- **Procesamiento por Lotes**: Optimizado para grandes volúmenes de datos

## Estructura del Proyecto

```
excel_sql_integration/
├── src/
│   ├── auth/                 # Módulo de autenticación
│   ├── excel_integration/    # Procesamiento de archivos Excel
│   ├── database/            # Operaciones de base de datos
│   ├── logging/             # Sistema de auditoría
│   └── ui/                  # Interfaz de usuario Tkinter
├── sql_scripts/             # Scripts SQL (tablas, procedimientos, triggers)
├── tests/                   # Pruebas unitarias e integración
├── docs/                    # Documentación
├── examples/               # Archivos de ejemplo
└── requirements.txt        # Dependencias Python
```

## Requisitos del Sistema

- Python 3.11+
- SQL Server (cualquier versión compatible con pyodbc)
- Windows/Linux/macOS

## Instalación

1. Clonar o descargar el proyecto
2. Crear un entorno virtual: `python -m venv venv`
3. Activar el entorno virtual: `source venv/bin/activate` (Linux/macOS) o `venv\Scripts\activate` (Windows)
4. Instalar dependencias: `pip install -r requirements.txt`
5. Ejecutar los scripts SQL para configurar la base de datos
6. Configurar la cadena de conexión en el archivo de configuración
7. Ejecutar la aplicación: `python src/main.py`

## Uso

1. Iniciar la aplicación
2. Autenticarse con credenciales válidas
3. Seleccionar archivo Excel para procesar
4. Revisar el mapeo automático de columnas
5. Ejecutar la operación de integración
6. Revisar los logs de auditoría

## Seguridad

- Todas las consultas SQL utilizan parámetros para prevenir inyección SQL
- Las contraseñas se almacenan de forma segura (hash)
- Sistema de bloqueo de cuenta después de 3 intentos fallidos
- Registro completo de auditoría para todas las operaciones

## Soporte

Para soporte técnico o reportar problemas, consulte la documentación en el directorio `docs/`.

