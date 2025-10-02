USE [HISTORICO]
GO

SELECT [Nit]
      ,[NombreBeneficiario]
      ,[CantidadBanco-ADRES-RF]
  FROM 
-- truncate table  
  [dbo].[DimBancos-ADRES-RF]

GO

--alter table [dbo].[DimBancos-ADRES-RF] ADD CONSTRAINT key_DimBancos primary key(Nit)


IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.OperationLog') AND type in (N'U'))
BEGIN
    CREATE TABLE Audit.OperationLog (
        LogID BIGINT IDENTITY(1,1) PRIMARY KEY,
        SessionID UNIQUEIDENTIFIER,
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
        AdditionalInfo NVARCHAR(MAX)--, -- JSON con información adicional
        --CONSTRAINT FK_OperationLog_Users FOREIGN KEY (UserID) REFERENCES Security.Users(UserID)
    );
    
    -- Índices para optimizar consultas de auditoría
    --CREATE INDEX IX_OperationLog_UserID ON Audit.OperationLog(UserID);
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


-- select * from Audit.OperationLog
-- truncate table Audit.OperationLog


IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.OperationLog') AND type in (N'U'))
BEGIN
    CREATE TABLE Audit.OperationLog (
        LogID BIGINT IDENTITY(1,1) PRIMARY KEY,
        SessionID UNIQUEIDENTIFIER,
        --UserID INT NOT NULL,
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
        AdditionalInfo NVARCHAR(MAX)--, -- JSON con información adicional
        --CONSTRAINT FK_OperationLog_Users FOREIGN KEY (UserID) REFERENCES Security.Users(UserID)
    );
    
    -- Índices para optimizar consultas de auditoría
    --CREATE INDEX IX_OperationLog_UserID ON Audit.OperationLog(UserID);
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


SELECT * FROM [Audit].[vw_AuditSummary]





IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.sp_LogOperation') AND type in (N'P', N'PC'))
    DROP PROCEDURE Audit.sp_LogOperation;
GO

CREATE PROCEDURE Audit.sp_LogOperation
    @SessionID UNIQUEIDENTIFIER,
    --@UserID INT,
    @Username NVARCHAR(50),
    @OperationType NVARCHAR(20),
    @TableName NVARCHAR(128),
    @SchemaName NVARCHAR(128) = 'dbo',
    @RecordCount INT = 0,
    @OperationStatus NVARCHAR(20),
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @ExecutionTimeMs INT = NULL,
    @SourceFile NVARCHAR(500) = NULL,
    @WorksheetName NVARCHAR(255) = NULL,
    @IPAddress NVARCHAR(45) = NULL,
    @AdditionalInfo NVARCHAR(MAX) = NULL,
    @LogID BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO Audit.OperationLog (
        SessionID, --UserID,
		Username, OperationType, TableName, SchemaName,
        RecordCount, OperationStatus, ErrorMessage, ExecutionTimeMs,
        SourceFile, WorksheetName, IPAddress, AdditionalInfo
    )
    VALUES (
        @SessionID, --@UserID, 
		@Username, @OperationType, @TableName, @SchemaName,
        @RecordCount, @OperationStatus, @ErrorMessage, @ExecutionTimeMs,
        @SourceFile, @WorksheetName, @IPAddress, @AdditionalInfo
    );
    
    SET @LogID = SCOPE_IDENTITY();
END
GO

-- =============================================
-- Procedimiento: Registrar Error de Validación
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.sp_LogValidationError') AND type in (N'P', N'PC'))
    DROP PROCEDURE Audit.sp_LogValidationError;
GO

CREATE PROCEDURE Audit.sp_LogValidationError
    @LogID BIGINT,
    @RowNumber INT = NULL,
    @ColumnName NVARCHAR(128) = NULL,
    @ErrorType NVARCHAR(50),
    @ErrorDescription NVARCHAR(500),
    @InvalidValue NVARCHAR(MAX) = NULL,
    @ExpectedFormat NVARCHAR(200) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO Audit.ValidationErrors (
        LogID, RowNumber, ColumnName, ErrorType, ErrorDescription, InvalidValue, ExpectedFormat
    )
    VALUES (
        @LogID, @RowNumber, @ColumnName, @ErrorType, @ErrorDescription, @InvalidValue, @ExpectedFormat
    );
END


-- =============================================
-- Procedimiento: Inserción Masiva Segura
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.sp_BulkInsertData') AND type in (N'P', N'PC'))
    DROP PROCEDURE dbo.sp_BulkInsertData;
GO

CREATE PROCEDURE dbo.sp_BulkInsertData
    @TableName NVARCHAR(128),
    @SchemaName NVARCHAR(128) = 'dbo',
    @DataXML XML,
    --@UserID INT,
    @SessionID UNIQUEIDENTIFIER,
    @SourceFile NVARCHAR(500) = NULL,
    @WorksheetName NVARCHAR(255) = NULL,
    @LogID BIGINT OUTPUT,
    @RecordsInserted INT OUTPUT,
    @ErrorMessage NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Username NVARCHAR(50);
	DECLARE @ExecutionTimeMs DATETIME;
    
    -- Obtener nombre de usuario
    SELECT @Username = SUSER_NAME();
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Aquí iría la lógica de inserción masiva usando el XML
        -- Por simplicidad, este es un ejemplo básico
        -- En la implementación real, se parseará el XML y se insertarán los datos
        
        SET @RecordsInserted = 0; -- Placeholder
        SET @ErrorMessage = NULL;
		set @ExecutionTimeMs = DATEDIFF(MILLISECOND, @StartTime, GETDATE());
        
        -- Registrar operación exitosa
        EXEC Audit.sp_LogOperation 
            @SessionID = @SessionID,
            --@UserID = @UserID,
            @Username = @Username,
            @OperationType = 'BULK_INSERT',
            @TableName = @TableName,
            @SchemaName = @SchemaName,
            @RecordCount = @RecordsInserted,
            @OperationStatus = 'SUCCESS',
            --@ExecutionTimeMs = DATEDIFF(MILLISECOND, @StartTime, GETDATE()),
			@ExecutionTimeMs = @ExecutionTimeMs,
            @SourceFile = @SourceFile,
            @WorksheetName = @WorksheetName,
            @LogID = @LogID OUTPUT;
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @RecordsInserted = 0;
        set @ExecutionTimeMs = DATEDIFF(MILLISECOND, @StartTime, GETDATE());
        -- Registrar operación fallida
        EXEC Audit.sp_LogOperation 
            @SessionID = @SessionID,
            --@UserID = @UserID,
            @Username = @Username,
            @OperationType = 'BULK_INSERT',
            @TableName = @TableName,
            @SchemaName = @SchemaName,
            @RecordCount = 0,
            @OperationStatus = 'FAILED',
            @ErrorMessage = @ErrorMessage,
            --@ExecutionTimeMs = DATEDIFF(MILLISECOND, @StartTime, GETDATE()) ,
			@ExecutionTimeMs = @ExecutionTimeMs,
            @SourceFile = @SourceFile,
            @WorksheetName = @WorksheetName,
            @LogID = @LogID OUTPUT;
    END CATCH
END
GO


-- =============================================
-- Trigger para INSERT en dbo.DimBancos-ADRES-RF
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'tr_DimBancos-ADRES-RF_Insert')
    DROP TRIGGER dbo.[tr_DimBancos-ADRES-RF_Insert];
GO

CREATE TRIGGER dbo.[tr_DimBancos-ADRES-RF_Insert]
ON dbo.[DimBancos-ADRES-RF]
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LogID BIGINT;
    DECLARE @RecordCount INT;
    DECLARE @UserContext NVARCHAR(128);
    DECLARE @SessionContext NVARCHAR(128);
    
    -- Obtener contexto del usuario (si está disponible)
    SET @UserContext = CAST(SYSTEM_USER AS NVARCHAR(128));  
    SET @SessionContext = CAST(CURRENT_USER AS NVARCHAR(128));
    
    -- Si no hay contexto de usuario, usar SYSTEM
    IF @UserContext IS NULL SET @UserContext = 'SYSTEM';
    
    SELECT @RecordCount = COUNT(*) FROM inserted;
    
    -- Registrar la operación de inserción
    INSERT INTO Audit.OperationLog (
        SessionID,
        --UserID,
        Username,
        OperationType,
        TableName,
        SchemaName,
        RecordCount,
        OperationStatus,
        OperationTimestamp,
        ApplicationName
    )
    VALUES (
        TRY_CAST(@SessionContext AS UNIQUEIDENTIFIER),
        --ISNULL((SELECT UserID FROM Security.Users WHERE Username = SUSER_NAME()), 0),
        @UserContext,
        'INSERT',
        'DimBancos-ADRES-RF',
        CURRENT_USER,
        @RecordCount,
        'SUCCESS',
        GETDATE(),
        'Excel-SQL Integration - Trigger'
    );
    
    SET @LogID = SCOPE_IDENTITY();
    
    -- Registrar detalles de los nuevos registros
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, NewValue, DataType)
    SELECT 
        @LogID,
        CAST(i.Nit AS NVARCHAR(50)),
        'Nit',
        i.Nit,
        'INT'
    FROM inserted i;
    
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, NewValue, DataType)
    SELECT 
        @LogID,
        CAST(i.Nit AS NVARCHAR(50)),
        'NombreBeneficiario',
        i.NombreBeneficiario,
        'NVARCHAR'
    FROM inserted i;
    
    -- Agregar más columnas según sea necesario...
    
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, NewValue, DataType)
    SELECT 
        @LogID,
        CAST(i.Nit AS NVARCHAR(50)),
        'CantidadBanco-ADRES-RF',
        i.[CantidadBanco-ADRES-RF],
        'tinyint'
    FROM inserted i;

END



--select * from [dbo].[FactRECAUDO2025]
--where [NitBeneficiarioAportante_Key]='899999090'
--and [MesNombreCalendario]='JUNIO'
--and [ValorRecaudo]= '$ 13.528.336,00'



IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'tr_Matriz_Insert')
    DROP TRIGGER dbo.[tr_DimBancos-ADRES-RF_Insert];
GO

CREATE TRIGGER dbo.[tr_DimBancos-ADRES-RF_Insert]
ON dbo.[DimBancos-ADRES-RF]
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LogID BIGINT;
    DECLARE @RecordCount INT;
    DECLARE @UserContext NVARCHAR(128);
    DECLARE @SessionContext NVARCHAR(128);
    
    -- Obtener contexto del usuario (si está disponible)
    SET @UserContext = CAST(SYSTEM_USER AS NVARCHAR(128));  
    SET @SessionContext = CAST(CURRENT_USER AS NVARCHAR(128));
    
    -- Si no hay contexto de usuario, usar SYSTEM
    IF @UserContext IS NULL SET @UserContext = 'SYSTEM';
    
    SELECT @RecordCount = COUNT(*) FROM inserted;


END




CREATE TABLE [Carga].[MATRIZ](
	[ID] [VARCHAR] (50) NOT NULL PRIMARY KEY,
	[MES RECAUDO] [varchar](255) NULL,
	[NUMERO DE CUENTA] [varchar](255) NULL,
	[NOMBRE DE LA CUENTA] [varchar](255) NULL,
	[FECHA DE RECAUDO] DATE NULL,
	[NIT ENTIDAD GIRADORA] [varchar](255) NULL,
	[RAZÓN SOCIAL ENTIDAD GIRADORA] [varchar](255) NULL,
	[VALOR MOVIMIENTO] money NULL,
	[NIT BENEFICIARIO] [varchar](255) NULL,
	[RAZON SOCIAL CAPTURADO] [varchar](255) NULL,
	[VALOR IDENTIFICADO CAPTURADO] money NULL,
	[RUBRO] [varchar](255) NULL,
	[CONCEPTO DE RUBRO] [varchar](255) NULL,
	[CódigoGrupoFuente_Key] [int] NULL,
	[Nombre_Grupo_Fuente] [varchar](255) NULL,
	[CódigoSubGrupoFuente_Key] [int] NULL,
	[Nombre_Subgrupo_Fuente] [varchar](255) NULL,
	[CódigoConcepto_Key] [varchar](255) NULL,
	[Concepto_Recaudo] [varchar](255) NULL,
	[CódigoTipoRegistro] [varchar](255) NULL,
	[Tipo_Registro_Recaudo] [varchar](255) NULL,
	[TIPO DE SOPORTE] [varchar](255) NULL,
	[ID MUI No PAQUETE] [varchar](255) NULL,
	[NIT IDENTIFICACIÓN CAPTURADO] [varchar](255) NULL,
	[VALOR CAPTURADO] money NULL,
	[ID_MUI No DE PAQUETE] [varchar](255) NULL,
	[CHECK NIT BENEFICIARIO] [varchar](255) NULL,
	[CHECK VALOR] [varchar](255) NULL,
	[CHECK ID] [varchar](255) NULL,
	[VAL_CHECK_1] [varchar](255) NULL,
	[DIARIO CXC] [varchar](255) NULL,
	[DIARIO BANCOS] [varchar](255) NULL,
	[# DE ENVIOS A GRUPO CONTABLE] [varchar](255) NULL,
	[Cod Concepto SNS] [varchar](255) NULL,
	[Nombre concepto SNS] [varchar](255) NULL,
	[Es identificación de meses anteriores SI NO] [varchar](255) NULL,
	[OBSERVACIONES] [varchar](255) NULL,
	[REPORTE] [varchar](255) NULL,
	[NIT] [varchar](255) NULL,
	[CHECK RUBRO PRES] [VARCHAR](255),
	[CHECK DIARIO CXC] [VARCHAR](255),
	[VALOR] money NULL
) ON [PRIMARY]
GO


--EXEC sp_RENAME '[Carga].[MATRIZ].[NOMBRE DE  LA CUENTA]' , 'NOMBRE DE LA CUENTA', 'COLUMN'

--select * from [Carga].[MATRIZ]

--select * from dbo.DimRubroPresupuestal rp
--where rp.CódigoRubroPptal_Key= '2-05-3-01'

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_AfterInsertUpdate_MATRIZ')
    DROP TRIGGER [Carga].[trg_AfterInsertUpdate_MATRIZ];
GO

CREATE TRIGGER trg_AfterInsertUpdate_MATRIZ
ON [Carga].[MATRIZ]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
	
	--UPDATE M
 --   SET M.[ID] = ISNULL(CASE WHEN LEN(M.[ID])=0 THEN NULL ELSE M.[ID] END, CONCAT(M.[NIT ENTIDAD GIRADORA], M.[ID MUI No PAQUETE]))
 --   FROM [Carga].[MATRIZ] M
 --   INNER JOIN INSERTED I ON M.[ID] = I.[ID]
 --   WHERE M.[ID] IS NULL OR M.[ID] = '';



    UPDATE M
    SET 
        [CHECK NIT BENEFICIARIO] = CASE 
            WHEN NOT CASE WHEN LEN(M.[NIT ENTIDAD GIRADORA])=0 THEN NULL ELSE M.[NIT ENTIDAD GIRADORA] END IS NULL 
				AND NOT CASE WHEN LEN(M.[NIT BENEFICIARIO])=0 THEN NULL ELSE M.[NIT BENEFICIARIO] END IS NULL 
				AND M.[NIT ENTIDAD GIRADORA] = M.[NIT BENEFICIARIO] THEN 'CORRECTO'
            ELSE 'INCORRECTO'
        END,
        [CHECK VALOR] = CASE 
            WHEN NOT CASE WHEN LEN(M.[VALOR MOVIMIENTO])=0 THEN  NULL ELSE M.[VALOR MOVIMIENTO] END IS NULL
			AND NOT  CASE WHEN LEN(M.[VALOR IDENTIFICADO CAPTURADO])=0 THEN  NULL ELSE M.[VALOR IDENTIFICADO CAPTURADO] END IS NULL
			AND M.[VALOR MOVIMIENTO] = M.[VALOR IDENTIFICADO CAPTURADO] THEN 'CORRECTO'
            ELSE 'INCORRECTO'
        END,
        [CHECK ID] = CASE 
            WHEN NOT CASE WHEN LEN(M.[ID MUI No PAQUETE])=0 THEN NULL ELSE M.[ID MUI No PAQUETE] END IS NULL AND NOT CASE WHEN LEN(M.[ID_MUI No DE PAQUETE])=0 THEN NULL ELSE M.[ID_MUI No DE PAQUETE] END IS NULL AND  M.[ID MUI No PAQUETE]= M.[ID_MUI No DE PAQUETE] THEN 'CORRECTO'
            ELSE 'INCORRECTO'
        END,
        [VAL_CHECK_1] = CASE 
            WHEN 
                [CHECK NIT BENEFICIARIO] = 'CORRECTO' AND [CHECK VALOR]= 'CORRECTO' AND [CHECK ID]='CORRECTO' 
            THEN 'VALIDADO'
            ELSE 'INVALIDO'
        END,
		[NIT IDENTIFICACIÓN CAPTURADO]=M.[NIT ENTIDAD GIRADORA],
		[VALOR CAPTURADO]= M.[VALOR MOVIMIENTO],
        [CONCEPTO DE RUBRO] = DR.[NombreRubroPpptal],
		[NOMBRE DE LA CUENTA] = c.[NombreCuentaBancaria]
    FROM [Carga].[MATRIZ] M
    LEFT JOIN [Recaudo].[DimRubroPresupuestal] DR
        ON M.[RUBRO] = DR.[CódigoRubroPptal_Key]
	LEFT JOIN [Recaudo].[DimCUENTABANCARIA] C
	on  C.NumeroCuentaBancaria_Key= [NUMERO DE CUENTA] AND C.Año=year([FECHA DE RECAUDO]) AND c.Mes= MONTH([FECHA DE RECAUDO])-1
    WHERE M.ID IN (SELECT ID FROM INSERTED)
END;


IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_BeforeInsert_MATRIZ')
    DROP TRIGGER [Carga].[trg_BeforeInsert_MATRIZ];
GO


CREATE TRIGGER trg_BeforeInsert_MATRIZ
ON [Carga].[MATRIZ]
INSTEAD OF INSERT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [Carga].[MATRIZ]
    (
        [ID],
        [MES RECAUDO], [NUMERO DE CUENTA], [NOMBRE DE LA CUENTA], [FECHA DE RECAUDO],
        [NIT ENTIDAD GIRADORA], [RAZÓN SOCIAL ENTIDAD GIRADORA], [VALOR MOVIMIENTO],
        [NIT BENEFICIARIO], [RAZON SOCIAL CAPTURADO], [VALOR IDENTIFICADO CAPTURADO],
        [RUBRO], [CONCEPTO DE RUBRO], [CódigoGrupoFuente_Key], [Nombre_Grupo_Fuente],
        [CódigoSubGrupoFuente_Key], [Nombre_Subgrupo_Fuente], [CódigoConcepto_Key],
        [Concepto_Recaudo], [CódigoTipoRegistro], [Tipo_Registro_Recaudo],
        [TIPO DE SOPORTE], [ID MUI No PAQUETE], [NIT IDENTIFICACIÓN CAPTURADO],
        [VALOR CAPTURADO], [ID_MUI No DE PAQUETE], [CHECK NIT BENEFICIARIO],
        [CHECK VALOR], [CHECK ID], [VAL_CHECK_1], [DIARIO CXC], [DIARIO BANCOS],
        [# DE ENVIOS A GRUPO CONTABLE], [Cod Concepto SNS], [Nombre concepto SNS],
        [Es identificación de meses anteriores SI NO], [OBSERVACIONES], [REPORTE],
        [NIT], [CHECK RUBRO PRES], [CHECK DIARIO CXC], [VALOR]
    )
    SELECT
        ISNULL(CASE WHEN LEN([ID])=0 THEN NULL ELSE [ID] END, CONCAT([NIT ENTIDAD GIRADORA], [ID MUI No PAQUETE])),
        ISNULL(CASE WHEN LEN([MES RECAUDO])=0 THEN NULL ELSE [MES RECAUDO] END, UPPER(DATENAME(month,TRY_CAST([FECHA DE RECAUDO] as date)))), 
		[NUMERO DE CUENTA], [NOMBRE DE LA CUENTA], TRY_CAST([FECHA DE RECAUDO] AS date),
        [NIT ENTIDAD GIRADORA], [RAZÓN SOCIAL ENTIDAD GIRADORA], [VALOR MOVIMIENTO],
        [NIT BENEFICIARIO], [RAZON SOCIAL CAPTURADO], [VALOR IDENTIFICADO CAPTURADO],
        [RUBRO], [CONCEPTO DE RUBRO], [CódigoGrupoFuente_Key], [Nombre_Grupo_Fuente],
        [CódigoSubGrupoFuente_Key], [Nombre_Subgrupo_Fuente], [CódigoConcepto_Key],
        [Concepto_Recaudo], [CódigoTipoRegistro], [Tipo_Registro_Recaudo],
        [TIPO DE SOPORTE], [ID MUI No PAQUETE], [NIT IDENTIFICACIÓN CAPTURADO],
        [VALOR CAPTURADO], [ID_MUI No DE PAQUETE], [CHECK NIT BENEFICIARIO],
        [CHECK VALOR], [CHECK ID], [VAL_CHECK_1], [DIARIO CXC], [DIARIO BANCOS],
        [# DE ENVIOS A GRUPO CONTABLE], [Cod Concepto SNS], [Nombre concepto SNS],
        [Es identificación de meses anteriores SI NO], [OBSERVACIONES], [REPORTE],
        [NIT], [CHECK RUBRO PRES], [CHECK DIARIO CXC], [VALOR]
    FROM INSERTED;
END;


INSERT INTO [Carga].[MATRIZ] ([ID],[MES RECAUDO],[NUMERO DE CUENTA],[NOMBRE DE LA CUENTA],[FECHA DE RECAUDO],[NIT ENTIDAD GIRADORA],[RAZÓN SOCIAL ENTIDAD GIRADORA],[VALOR MOVIMIENTO],[NIT BENEFICIARIO],[RAZON SOCIAL CAPTURADO],[VALOR IDENTIFICADO CAPTURADO],[RUBRO],[CONCEPTO DE RUBRO],[CódigoGrupoFuente_Key],[Nombre_Grupo_Fuente],[CódigoSubGrupoFuente_Key],[Nombre_Subgrupo_Fuente],[CódigoConcepto_Key],[Concepto_Recaudo],[CódigoTipoRegistro],[Tipo_Registro_Recaudo],[TIPO DE SOPORTE],[ID MUI No PAQUETE],[NIT IDENTIFICACIÓN CAPTURADO],[VALOR CAPTURADO],[ID_MUI No DE PAQUETE],[CHECK NIT BENEFICIARIO],[CHECK VALOR],[CHECK ID],[VAL_CHECK_1],[DIARIO CXC],[DIARIO BANCOS],[# DE ENVIOS A GRUPO CONTABLE],[Cod Concepto SNS],[Nombre concepto SNS],[Es identificación de meses anteriores SI NO],[OBSERVACIONES],[REPORTE],[NIT],[CHECK RUBRO PRES],[CHECK DIARIO CXC],[VALOR]) 
VALUES ('','','513845644','IMPUESTO CONSUMO CERVEZAS SIFONES Y REFAJOS','2025-06-27','899999336','GOBERNACION DEL AMAZONAS','139820,63','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','')


select * from 
--truncate table 
carga.MATRIZ

SELECT * FROM [Carga].[MUI] M
WHERE M.[Numero_Identificacion]='800251440'
AND M.[ID_Captura_MUI]='0020000672302'
--M.[Cuenta_bancaria]='513845644'

select * from [Carga].[Ingreso_Presupuesto]



INSERT INTO [Carga].[MATRIZ] ([ID],[NUMERO DE CUENTA],[FECHA DE RECAUDO],[NIT ENTIDAD GIRADORA],[RAZÓN SOCIAL ENTIDAD GIRADORA],[VALOR MOVIMIENTO],[NIT BENEFICIARIO],[RAZON SOCIAL CAPTURADO],[VALOR IDENTIFICADO CAPTURADO],[RUBRO],[ID_MUI No DE PAQUETE],[ID MUI No PAQUETE])
SELECT DISTINCT CONCAT(M.[Numero_Identificacion],TRY_CAST(M.[ID_Captura_MUI] AS bigint)),M.[Cuenta_Bancaria], M.[Fecha_Recaudo], M.[Numero_Identificacion], M.[Nombre_Entidad], M.[Valor], E.ID_TERCERO, SUBSTRING(E.NOMBRE_TERCERO,CHARINDEX('-', E.NOMBRE_TERCERO, CHARINDEX('-', E.NOMBRE_TERCERO) + 1) + 1,LEN(E.NOMBRE_TERCERO)), E.VALOR, E.RUBRO, M.[ID_Captura_MUI],E.[FACTURA_CONTABILIDAD]
-- select *, SUBSTRING(E.NOMBRE_TERCERO,CHARINDEX('-', E.NOMBRE_TERCERO, CHARINDEX('-', E.NOMBRE_TERCERO) + 1) + 1,LEN(E.NOMBRE_TERCERO))

FROM  [Carga].[MUI] AS M
INNER JOIN [Carga].[Ingreso_Presupuesto] E
ON M.[ID_Captura_MUI] = E.[FACTURA_CONTABILIDAD]
--WHERE M.[Numero_Identificacion]='800251440'


SELECT * FROM (select distinct * from [Carga].[MUI]) M --25842
inner JOIN  (select * from [Carga].[Ingreso_Presupuesto]) E --74401
ON M.[ID_Captura_MUI] = E.[FACTURA_CONTABILIDAD] --8993
left join [Recaudo].[DimGrupoFte] R
on R.CódigoRubroPptal_Key= e.RUBRO
LEFT join Recaudo.DimSubGrupoFte sb
on R.CódigoGrupoFuente_Key=sb.CódigoGrupoFuente_Key and sb.CódigoRubroPptal_Key=r.CódigoRubroPptal_Key
left join Recaudo.DimConcepto c
on sb.CódigoSubGrupoFuente_Key=c.CódigoSubGrupoFuente_Key and c.CódigoRubroPptal_Key=r.CódigoRubroPptal_Key
left join (select * from Recaudo.DimTipoRegistro) t 
on t.CódigoConcepto_Key=c.CódigoConcepto_Key and t.CódigoRubroPptal_Key=r.CódigoRubroPptal_Key
WHERE 
--R.CódigoRubroPptal_Key='2-05-3-01'
--M.[ID_Captura_MUI]='0020000672277'
--AND M.[Numero_Identificacion]='830113831'
--and M.[Cuenta_bancaria]='00000003100002387'
--AND 
CHARINDEX(upper(t.TipoRegistro),e.DESCRIPCION_CODIGO_CONTABLE,1)>0
order by ID_TERCERO


