-- =============================================
-- Script Maestro: Ejecutar Todos los Scripts de Configuración
-- Descripción: Ejecuta todos los scripts SQL en el orden correcto para configurar la base de datos completa
-- Autor: Manus AI
-- Fecha: 2025-01-08
-- =============================================

PRINT '=== INICIANDO CONFIGURACIÓN DE BASE DE DATOS ===';
PRINT 'Fecha/Hora: ' + CONVERT(NVARCHAR, GETDATE(), 120);
PRINT '';

-- =============================================
-- 1. Crear esquema de base de datos
-- =============================================
PRINT '1. Ejecutando: Creación del esquema de base de datos...';
:r 01_create_database_schema.sql
PRINT '   ✓ Esquema de base de datos configurado.';
PRINT '';

-- =============================================
-- 2. Crear tablas de seguridad
-- =============================================
PRINT '2. Ejecutando: Creación de tablas de seguridad...';
:r 02_create_security_tables.sql
PRINT '   ✓ Tablas de seguridad creadas.';
PRINT '';

-- =============================================
-- 3. Crear tablas de auditoría
-- =============================================
PRINT '3. Ejecutando: Creación de tablas de auditoría...';
:r 03_create_audit_tables.sql
PRINT '   ✓ Tablas de auditoría creadas.';
PRINT '';

-- =============================================
-- 4. Crear procedimientos almacenados
-- =============================================
PRINT '4. Ejecutando: Creación de procedimientos almacenados...';
:r 04_create_stored_procedures.sql
PRINT '   ✓ Procedimientos almacenados creados.';
PRINT '';

-- =============================================
-- 5. Crear triggers de auditoría
-- =============================================
PRINT '5. Ejecutando: Creación de triggers de auditoría...';
:r 05_create_triggers.sql
PRINT '   ✓ Triggers de auditoría creados.';
PRINT '';

-- =============================================
-- 6. Insertar datos de ejemplo
-- =============================================
PRINT '6. Ejecutando: Inserción de datos de ejemplo...';
:r 06_sample_data.sql
PRINT '   ✓ Datos de ejemplo insertados.';
PRINT '';

-- =============================================
-- Verificación final
-- =============================================
PRINT '=== VERIFICACIÓN DE CONFIGURACIÓN ===';

USE ExcelSQLIntegration;

-- Verificar esquemas
PRINT 'Esquemas creados:';
SELECT '  - ' + name as Esquema FROM sys.schemas WHERE name IN ('Security', 'Audit', 'Data');

-- Verificar tablas principales
PRINT '';
PRINT 'Tablas principales:';
SELECT '  - ' + SCHEMA_NAME(schema_id) + '.' + name as Tabla 
FROM sys.tables 
WHERE SCHEMA_NAME(schema_id) IN ('Security', 'Audit', 'Data')
ORDER BY SCHEMA_NAME(schema_id), name;

-- Verificar procedimientos almacenados
PRINT '';
PRINT 'Procedimientos almacenados:';
SELECT '  - ' + SCHEMA_NAME(schema_id) + '.' + name as Procedimiento
FROM sys.procedures 
WHERE SCHEMA_NAME(schema_id) IN ('Security', 'Audit', 'Data')
ORDER BY SCHEMA_NAME(schema_id), name;

-- Verificar triggers
PRINT '';
PRINT 'Triggers creados:';
SELECT '  - ' + OBJECT_SCHEMA_NAME(parent_id) + '.' + OBJECT_NAME(parent_id) + ' -> ' + name as Trigger
FROM sys.triggers 
WHERE is_ms_shipped = 0
ORDER BY OBJECT_SCHEMA_NAME(parent_id), OBJECT_NAME(parent_id);

-- Verificar usuarios
PRINT '';
PRINT 'Usuarios configurados:';
SELECT '  - ' + Username + ' (' + r.RoleName + ')' as Usuario
FROM Security.Users u
LEFT JOIN Security.UserRoles ur ON u.UserID = ur.UserID
LEFT JOIN Security.Roles r ON ur.RoleID = r.RoleID
WHERE u.IsActive = 1
ORDER BY u.Username;

-- Estadísticas de datos
PRINT '';
PRINT 'Datos de ejemplo:';
SELECT '  - Customers: ' + CAST(COUNT(*) AS NVARCHAR) + ' registros' FROM Data.Customers;
SELECT '  - Products: ' + CAST(COUNT(*) AS NVARCHAR) + ' registros' FROM Data.Products;
SELECT '  - Audit Logs: ' + CAST(COUNT(*) AS NVARCHAR) + ' registros' FROM Audit.OperationLog;

PRINT '';
PRINT '=== CONFIGURACIÓN COMPLETADA EXITOSAMENTE ===';
PRINT 'La base de datos ExcelSQLIntegration está lista para usar.';
PRINT '';
PRINT 'Próximos pasos:';
PRINT '1. Configurar la cadena de conexión en la aplicación Python';
PRINT '2. Probar la conectividad desde la aplicación';
PRINT '3. Ejecutar las pruebas de integración';
PRINT '';
PRINT 'Para conectarse desde Python, use:';
PRINT 'Server: [su_servidor]';
PRINT 'Database: ExcelSQLIntegration';
PRINT 'Authentication: SQL Server Authentication o Windows Authentication';
PRINT '';
PRINT 'Usuarios de prueba disponibles:';
PRINT '- admin (contraseña: admin123) - Administrador completo';
PRINT '- dataoperator (contraseña: user123) - Operador de datos';
PRINT '- readonly (contraseña: user123) - Solo lectura';
PRINT '';
PRINT 'NOTA: En producción, cambiar las contraseñas por defecto y usar hash seguro.';
PRINT '=== FIN DE CONFIGURACIÓN ===';

-- Mostrar información de conexión
SELECT 
    'Configuración de conexión completada' as Estado,
    DB_NAME() as BaseDatos,
    @@SERVERNAME as Servidor,
    GETDATE() as FechaConfiguracion;

