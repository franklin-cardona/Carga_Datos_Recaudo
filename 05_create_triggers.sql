-- =============================================
-- Script: Creación de Triggers de Auditoría
-- Descripción: Crea triggers para el registro automático de cambios en las tablas de datos
-- Autor: Manus AI
-- Fecha: 2025-01-08
-- =============================================

USE ExcelSQLIntegration;
GO

-- =============================================
-- Trigger: Auditoría para tabla de ejemplo (Customers)
-- Nota: Este es un ejemplo. Se deben crear triggers similares para cada tabla de datos
-- =============================================

-- Primero crear una tabla de ejemplo para demostrar los triggers
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Data.Customers') AND type in (N'U'))
BEGIN
    CREATE TABLE Data.Customers (
        CustomerID INT IDENTITY(1,1) PRIMARY KEY,
        CustomerCode NVARCHAR(20) NOT NULL UNIQUE,
        CompanyName NVARCHAR(100) NOT NULL,
        ContactName NVARCHAR(50),
        Email NVARCHAR(100),
        Phone NVARCHAR(20),
        Address NVARCHAR(200),
        City NVARCHAR(50),
        Country NVARCHAR(50),
        IsActive BIT NOT NULL DEFAULT 1,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        ModifiedDate DATETIME2,
        CreatedBy NVARCHAR(50) NOT NULL DEFAULT 'SYSTEM',
        ModifiedBy NVARCHAR(50)
    );
    
    CREATE INDEX IX_Customers_CustomerCode ON Data.Customers(CustomerCode);
    CREATE INDEX IX_Customers_CompanyName ON Data.Customers(CompanyName);
    
    PRINT 'Tabla Data.Customers creada como ejemplo.';
END
GO

-- =============================================
-- Trigger para INSERT en Data.Customers
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'tr_Customers_Insert')
    DROP TRIGGER Data.tr_Customers_Insert;
GO

CREATE TRIGGER Data.tr_Customers_Insert
ON Data.Customers
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LogID BIGINT;
    DECLARE @RecordCount INT;
    DECLARE @UserContext NVARCHAR(128);
    DECLARE @SessionContext NVARCHAR(128);
    
    -- Obtener contexto del usuario (si está disponible)
    SET @UserContext = CAST(SESSION_CONTEXT(N'Username') AS NVARCHAR(128));
    SET @SessionContext = CAST(SESSION_CONTEXT(N'SessionID') AS NVARCHAR(128));
    
    -- Si no hay contexto de usuario, usar SYSTEM
    IF @UserContext IS NULL SET @UserContext = 'SYSTEM';
    
    SELECT @RecordCount = COUNT(*) FROM inserted;
    
    -- Registrar la operación de inserción
    INSERT INTO Audit.OperationLog (
        SessionID,
        UserID,
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
        ISNULL((SELECT UserID FROM Security.Users WHERE Username = @UserContext), 0),
        @UserContext,
        'INSERT',
        'Customers',
        'Data',
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
        CAST(i.CustomerID AS NVARCHAR(50)),
        'CustomerCode',
        i.CustomerCode,
        'NVARCHAR'
    FROM inserted i;
    
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, NewValue, DataType)
    SELECT 
        @LogID,
        CAST(i.CustomerID AS NVARCHAR(50)),
        'CompanyName',
        i.CompanyName,
        'NVARCHAR'
    FROM inserted i;
    
    -- Agregar más columnas según sea necesario...
END
GO

-- =============================================
-- Trigger para UPDATE en Data.Customers
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'tr_Customers_Update')
    DROP TRIGGER Data.tr_Customers_Update;
GO

CREATE TRIGGER Data.tr_Customers_Update
ON Data.Customers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LogID BIGINT;
    DECLARE @RecordCount INT;
    DECLARE @UserContext NVARCHAR(128);
    DECLARE @SessionContext NVARCHAR(128);
    
    -- Obtener contexto del usuario
    SET @UserContext = CAST(SESSION_CONTEXT(N'Username') AS NVARCHAR(128));
    SET @SessionContext = CAST(SESSION_CONTEXT(N'SessionID') AS NVARCHAR(128));
    
    IF @UserContext IS NULL SET @UserContext = 'SYSTEM';
    
    SELECT @RecordCount = COUNT(*) FROM inserted;
    
    -- Registrar la operación de actualización
    INSERT INTO Audit.OperationLog (
        SessionID,
        UserID,
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
        ISNULL((SELECT UserID FROM Security.Users WHERE Username = @UserContext), 0),
        @UserContext,
        'UPDATE',
        'Customers',
        'Data',
        @RecordCount,
        'SUCCESS',
        GETDATE(),
        'Excel-SQL Integration - Trigger'
    );
    
    SET @LogID = SCOPE_IDENTITY();
    
    -- Registrar cambios en CustomerCode
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, OldValue, NewValue, DataType)
    SELECT 
        @LogID,
        CAST(i.CustomerID AS NVARCHAR(50)),
        'CustomerCode',
        d.CustomerCode,
        i.CustomerCode,
        'NVARCHAR'
    FROM inserted i
    INNER JOIN deleted d ON i.CustomerID = d.CustomerID
    WHERE i.CustomerCode != d.CustomerCode OR (i.CustomerCode IS NULL AND d.CustomerCode IS NOT NULL) OR (i.CustomerCode IS NOT NULL AND d.CustomerCode IS NULL);
    
    -- Registrar cambios en CompanyName
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, OldValue, NewValue, DataType)
    SELECT 
        @LogID,
        CAST(i.CustomerID AS NVARCHAR(50)),
        'CompanyName',
        d.CompanyName,
        i.CompanyName,
        'NVARCHAR'
    FROM inserted i
    INNER JOIN deleted d ON i.CustomerID = d.CustomerID
    WHERE i.CompanyName != d.CompanyName OR (i.CompanyName IS NULL AND d.CompanyName IS NOT NULL) OR (i.CompanyName IS NOT NULL AND d.CompanyName IS NULL);
    
    -- Registrar cambios en ContactName
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, OldValue, NewValue, DataType)
    SELECT 
        @LogID,
        CAST(i.CustomerID AS NVARCHAR(50)),
        'ContactName',
        d.ContactName,
        i.ContactName,
        'NVARCHAR'
    FROM inserted i
    INNER JOIN deleted d ON i.CustomerID = d.CustomerID
    WHERE i.ContactName != d.ContactName OR (i.ContactName IS NULL AND d.ContactName IS NOT NULL) OR (i.ContactName IS NOT NULL AND d.ContactName IS NULL);
    
    -- Registrar cambios en Email
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, OldValue, NewValue, DataType)
    SELECT 
        @LogID,
        CAST(i.CustomerID AS NVARCHAR(50)),
        'Email',
        d.Email,
        i.Email,
        'NVARCHAR'
    FROM inserted i
    INNER JOIN deleted d ON i.CustomerID = d.CustomerID
    WHERE i.Email != d.Email OR (i.Email IS NULL AND d.Email IS NOT NULL) OR (i.Email IS NOT NULL AND d.Email IS NULL);
    
    -- Actualizar ModifiedDate automáticamente
    UPDATE Data.Customers 
    SET ModifiedDate = GETDATE()
    WHERE CustomerID IN (SELECT CustomerID FROM inserted);
END
GO

-- =============================================
-- Trigger para DELETE en Data.Customers
-- =============================================
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'tr_Customers_Delete')
    DROP TRIGGER Data.tr_Customers_Delete;
GO

CREATE TRIGGER Data.tr_Customers_Delete
ON Data.Customers
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LogID BIGINT;
    DECLARE @RecordCount INT;
    DECLARE @UserContext NVARCHAR(128);
    DECLARE @SessionContext NVARCHAR(128);
    
    -- Obtener contexto del usuario
    SET @UserContext = CAST(SESSION_CONTEXT(N'Username') AS NVARCHAR(128));
    SET @SessionContext = CAST(SESSION_CONTEXT(N'SessionID') AS NVARCHAR(128));
    
    IF @UserContext IS NULL SET @UserContext = 'SYSTEM';
    
    SELECT @RecordCount = COUNT(*) FROM deleted;
    
    -- Registrar la operación de eliminación
    INSERT INTO Audit.OperationLog (
        SessionID,
        UserID,
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
        ISNULL((SELECT UserID FROM Security.Users WHERE Username = @UserContext), 0),
        @UserContext,
        'DELETE',
        'Customers',
        'Data',
        @RecordCount,
        'SUCCESS',
        GETDATE(),
        'Excel-SQL Integration - Trigger'
    );
    
    SET @LogID = SCOPE_IDENTITY();
    
    -- Registrar los valores eliminados
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, OldValue, DataType)
    SELECT 
        @LogID,
        CAST(d.CustomerID AS NVARCHAR(50)),
        'CustomerCode',
        d.CustomerCode,
        'NVARCHAR'
    FROM deleted d;
    
    INSERT INTO Audit.ChangeDetails (LogID, RecordID, ColumnName, OldValue, DataType)
    SELECT 
        @LogID,
        CAST(d.CustomerID AS NVARCHAR(50)),
        'CompanyName',
        d.CompanyName,
        'NVARCHAR'
    FROM deleted d;
    
    -- Agregar más columnas según sea necesario...
END
GO

-- =============================================
-- Procedimiento para establecer contexto de usuario
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.sp_SetUserContext') AND type in (N'P', N'PC'))
    DROP PROCEDURE Security.sp_SetUserContext;
GO

CREATE PROCEDURE Security.sp_SetUserContext
    @Username NVARCHAR(128),
    @SessionID UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Establecer contexto de usuario para los triggers
    EXEC sp_set_session_context @key = N'Username', @value = @Username;
    
    IF @SessionID IS NOT NULL
        EXEC sp_set_session_context @key = N'SessionID', @value = @SessionID;
END
GO

-- =============================================
-- Procedimiento para limpiar contexto de usuario
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.sp_ClearUserContext') AND type in (N'P', N'PC'))
    DROP PROCEDURE Security.sp_ClearUserContext;
GO

CREATE PROCEDURE Security.sp_ClearUserContext
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Limpiar contexto de usuario
    EXEC sp_set_session_context @key = N'Username', @value = NULL;
    EXEC sp_set_session_context @key = N'SessionID', @value = NULL;
END
GO

-- =============================================
-- Función para generar triggers automáticamente (template)
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.fn_GenerateTriggerScript') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
    DROP FUNCTION Audit.fn_GenerateTriggerScript;
GO

CREATE FUNCTION Audit.fn_GenerateTriggerScript(
    @TableName NVARCHAR(128),
    @SchemaName NVARCHAR(128) = 'Data'
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @Script NVARCHAR(MAX) = '';
    
    -- Este sería el template para generar triggers automáticamente
    -- Por simplicidad, retornamos un mensaje
    SET @Script = 'Template para generar trigger para tabla: ' + @SchemaName + '.' + @TableName;
    
    RETURN @Script;
END
GO

PRINT 'Triggers de auditoría creados exitosamente.';
PRINT 'NOTA: Los triggers mostrados son para la tabla de ejemplo Data.Customers.';
PRINT 'Se deben crear triggers similares para cada tabla de datos que requiera auditoría.';

