-- =============================================
-- Script: Creación del Esquema de Base de Datos
-- Descripción: Crea la base de datos y esquemas necesarios para la solución de integración Excel-SQL Server
-- Autor: Manus AI
-- Fecha: 2025-01-08
-- =============================================

-- Crear la base de datos si no existe
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ExcelSQLIntegration')
BEGIN
    CREATE DATABASE ExcelSQLIntegration;
    PRINT 'Base de datos ExcelSQLIntegration creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La base de datos ExcelSQLIntegration ya existe.';
END
GO

-- Usar la base de datos
USE ExcelSQLIntegration;
GO

-- Crear esquemas para organizar los objetos
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Security')
BEGIN
    EXEC('CREATE SCHEMA Security');
    PRINT 'Esquema Security creado exitosamente.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Audit')
BEGIN
    EXEC('CREATE SCHEMA Audit');
    PRINT 'Esquema Audit creado exitosamente.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Data')
BEGIN
    EXEC('CREATE SCHEMA Data');
    PRINT 'Esquema Data creado exitosamente.';
END
GO

PRINT 'Esquemas de base de datos configurados correctamente.';

