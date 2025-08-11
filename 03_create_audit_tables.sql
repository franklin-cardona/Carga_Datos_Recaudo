-- =============================================
-- Script: Creación de Tablas de Auditoría
-- Descripción: Crea las tablas necesarias para el sistema de auditoría y registro de operaciones
-- Autor: Manus AI
-- Fecha: 2025-01-08
-- =============================================

USE ExcelSQLIntegration;
GO

-- Tabla principal de auditoría
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.OperationLog') AND type in (N'U'))
BEGIN
    CREATE TABLE Audit.OperationLog (
        LogID BIGINT IDENTITY(1,1) PRIMARY KEY,
        SessionID UNIQUEIDENTIFIER,
        UserID INT NOT NULL,
        Username NVARCHAR(50) NOT NULL,
        OperationType NVARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE, SELECT, BULK_INSERT
        TableName NVARCHAR(128) NOT NULL,
        SchemaName NVARCHAR(128) NOT NULL DEFAULT 'dbo',
        RecordCount INT NOT NULL DEFAULT 0,
        OperationStatus NVARCHAR(20) NOT NULL, -- SUCCESS, FAILED, PARTIAL
        ErrorMessage NVARCHAR(MAX),
        ExecutionTimeMs INT,
        SourceFile NVARCHAR(500), -- Nombre del archivo Excel procesado
        WorksheetName NVARCHAR(255), -- Nombre de la hoja de Excel
        OperationTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
        IPAddress NVARCHAR(45),
        ApplicationName NVARCHAR(100) DEFAULT 'Excel-SQL Integration',
        AdditionalInfo NVARCHAR(MAX), -- JSON con información adicional
        CONSTRAINT FK_OperationLog_Users FOREIGN KEY (UserID) REFERENCES Security.Users(UserID)
    );
    
    -- Índices para optimizar consultas de auditoría
    CREATE INDEX IX_OperationLog_UserID ON Audit.OperationLog(UserID);
    CREATE INDEX IX_OperationLog_OperationType ON Audit.OperationLog(OperationType);
    CREATE INDEX IX_OperationLog_TableName ON Audit.OperationLog(TableName);
    CREATE INDEX IX_OperationLog_OperationTimestamp ON Audit.OperationLog(OperationTimestamp);
    CREATE INDEX IX_OperationLog_OperationStatus ON Audit.OperationLog(OperationStatus);
    CREATE INDEX IX_OperationLog_SessionID ON Audit.OperationLog(SessionID);
    
    PRINT 'Tabla Audit.OperationLog creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La tabla Audit.OperationLog ya existe.';
END
GO

-- Tabla de detalles de cambios (para operaciones UPDATE)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.ChangeDetails') AND type in (N'U'))
BEGIN
    CREATE TABLE Audit.ChangeDetails (
        ChangeID BIGINT IDENTITY(1,1) PRIMARY KEY,
        LogID BIGINT NOT NULL,
        RecordID NVARCHAR(50) NOT NULL, -- ID del registro afectado
        ColumnName NVARCHAR(128) NOT NULL,
        OldValue NVARCHAR(MAX),
        NewValue NVARCHAR(MAX),
        DataType NVARCHAR(50),
        ChangeTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT FK_ChangeDetails_OperationLog FOREIGN KEY (LogID) REFERENCES Audit.OperationLog(LogID)
    );
    
    -- Índices para optimizar consultas
    CREATE INDEX IX_ChangeDetails_LogID ON Audit.ChangeDetails(LogID);
    CREATE INDEX IX_ChangeDetails_RecordID ON Audit.ChangeDetails(RecordID);
    CREATE INDEX IX_ChangeDetails_ColumnName ON Audit.ChangeDetails(ColumnName);
    
    PRINT 'Tabla Audit.ChangeDetails creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La tabla Audit.ChangeDetails ya existe.';
END
GO

-- Tabla de errores de validación
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.ValidationErrors') AND type in (N'U'))
BEGIN
    CREATE TABLE Audit.ValidationErrors (
        ErrorID BIGINT IDENTITY(1,1) PRIMARY KEY,
        LogID BIGINT NOT NULL,
        RowNumber INT,
        ColumnName NVARCHAR(128),
        ErrorType NVARCHAR(50), -- DATA_TYPE, CONSTRAINT, BUSINESS_RULE, etc.
        ErrorDescription NVARCHAR(500),
        InvalidValue NVARCHAR(MAX),
        ExpectedFormat NVARCHAR(200),
        ErrorTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT FK_ValidationErrors_OperationLog FOREIGN KEY (LogID) REFERENCES Audit.OperationLog(LogID)
    );
    
    -- Índices para optimizar consultas
    CREATE INDEX IX_ValidationErrors_LogID ON Audit.ValidationErrors(LogID);
    CREATE INDEX IX_ValidationErrors_ErrorType ON Audit.ValidationErrors(ErrorType);
    
    PRINT 'Tabla Audit.ValidationErrors creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La tabla Audit.ValidationErrors ya existe.';
END
GO

-- Tabla de estadísticas de rendimiento
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.PerformanceStats') AND type in (N'U'))
BEGIN
    CREATE TABLE Audit.PerformanceStats (
        StatID BIGINT IDENTITY(1,1) PRIMARY KEY,
        LogID BIGINT NOT NULL,
        ProcessingPhase NVARCHAR(50), -- FILE_READ, DATA_VALIDATION, DB_OPERATION, etc.
        StartTime DATETIME2 NOT NULL,
        EndTime DATETIME2 NOT NULL,
        DurationMs AS DATEDIFF(MILLISECOND, StartTime, EndTime) PERSISTED,
        RecordsProcessed INT DEFAULT 0,
        MemoryUsageMB DECIMAL(10,2),
        AdditionalMetrics NVARCHAR(MAX), -- JSON con métricas adicionales
        CONSTRAINT FK_PerformanceStats_OperationLog FOREIGN KEY (LogID) REFERENCES Audit.OperationLog(LogID)
    );
    
    -- Índices para optimizar consultas
    CREATE INDEX IX_PerformanceStats_LogID ON Audit.PerformanceStats(LogID);
    CREATE INDEX IX_PerformanceStats_ProcessingPhase ON Audit.PerformanceStats(ProcessingPhase);
    CREATE INDEX IX_PerformanceStats_DurationMs ON Audit.PerformanceStats(DurationMs);
    
    PRINT 'Tabla Audit.PerformanceStats creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La tabla Audit.PerformanceStats ya existe.';
END
GO

-- Crear función para particionamiento por fecha (para optimización futura)
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PF_AuditByMonth')
BEGIN
    -- Crear función de partición por mes
    CREATE PARTITION FUNCTION PF_AuditByMonth (DATETIME2)
    AS RANGE RIGHT FOR VALUES (
        '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01',
        '2025-05-01', '2025-06-01', '2025-07-01', '2025-08-01',
        '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01'
    );
    
    PRINT 'Función de partición PF_AuditByMonth creada exitosamente.';
END
GO

-- Vista para consultas de auditoría simplificadas
IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'vw_AuditSummary')
BEGIN
    EXEC('
    CREATE VIEW Audit.vw_AuditSummary AS
    SELECT 
        ol.LogID,
        ol.Username,
        ol.OperationType,
        ol.TableName,
        ol.RecordCount,
        ol.OperationStatus,
        ol.SourceFile,
        ol.WorksheetName,
        ol.OperationTimestamp,
        ol.ExecutionTimeMs,
        CASE 
            WHEN ol.OperationStatus = ''SUCCESS'' THEN ''Exitoso''
            WHEN ol.OperationStatus = ''FAILED'' THEN ''Fallido''
            WHEN ol.OperationStatus = ''PARTIAL'' THEN ''Parcial''
            ELSE ol.OperationStatus
        END AS EstadoOperacion,
        CASE 
            WHEN ol.OperationType = ''INSERT'' THEN ''Inserción''
            WHEN ol.OperationType = ''UPDATE'' THEN ''Actualización''
            WHEN ol.OperationType = ''DELETE'' THEN ''Eliminación''
            WHEN ol.OperationType = ''SELECT'' THEN ''Consulta''
            WHEN ol.OperationType = ''BULK_INSERT'' THEN ''Inserción Masiva''
            ELSE ol.OperationType
        END AS TipoOperacion,
        (SELECT COUNT(*) FROM Audit.ValidationErrors ve WHERE ve.LogID = ol.LogID) as ErroresValidacion
    FROM Audit.OperationLog ol
    ');
    
    PRINT 'Vista Audit.vw_AuditSummary creada exitosamente.';
END
GO

PRINT 'Tablas de auditoría configuradas correctamente.';

