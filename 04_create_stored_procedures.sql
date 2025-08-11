-- =============================================
-- Script: Creación de Procedimientos Almacenados
-- Descripción: Crea los procedimientos almacenados para operaciones CRUD seguras y gestión de usuarios
-- Autor: Manus AI
-- Fecha: 2025-01-08
-- =============================================

USE ExcelSQLIntegration;
GO

-- =============================================
-- Procedimiento: Autenticación de Usuario
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.sp_AuthenticateUser') AND type in (N'P', N'PC'))
    DROP PROCEDURE Security.sp_AuthenticateUser;
GO

CREATE PROCEDURE Security.sp_AuthenticateUser
    @Username NVARCHAR(50),
    @PasswordHash NVARCHAR(255),
    @IPAddress NVARCHAR(45) = NULL,
    @UserAgent NVARCHAR(500) = NULL,
    @AuthResult INT OUTPUT,
    @UserID INT OUTPUT,
    @SessionID UNIQUEIDENTIFIER OUTPUT,
    @ErrorMessage NVARCHAR(255) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StoredPasswordHash NVARCHAR(255);
    DECLARE @Salt NVARCHAR(255);
    DECLARE @IsActive BIT;
    DECLARE @IsLocked BIT;
    DECLARE @FailedAttempts INT;
    DECLARE @LockoutTime DATETIME2;
    
    BEGIN TRY
        -- Inicializar variables de salida
        SET @AuthResult = 0; -- 0: Fallo, 1: Éxito, 2: Usuario bloqueado, 3: Usuario inactivo
        SET @UserID = NULL;
        SET @SessionID = NULL;
        SET @ErrorMessage = NULL;
        
        -- Buscar usuario
        SELECT 
            @UserID = UserID,
            @StoredPasswordHash = PasswordHash,
            @Salt = Salt,
            @IsActive = IsActive,
            @IsLocked = IsLocked,
            @FailedAttempts = FailedLoginAttempts,
            @LockoutTime = LockoutTime
        FROM Security.Users 
        WHERE Username = @Username;
        
        -- Verificar si el usuario existe
        IF @UserID IS NULL
        BEGIN
            SET @AuthResult = 0;
            SET @ErrorMessage = 'Usuario no encontrado';
            RETURN;
        END
        
        -- Verificar si el usuario está activo
        IF @IsActive = 0
        BEGIN
            SET @AuthResult = 3;
            SET @ErrorMessage = 'Usuario inactivo';
            RETURN;
        END
        
        -- Verificar si el usuario está bloqueado
        IF @IsLocked = 1 AND @LockoutTime > GETDATE()
        BEGIN
            SET @AuthResult = 2;
            SET @ErrorMessage = 'Usuario bloqueado temporalmente';
            RETURN;
        END
        
        -- Si el bloqueo ha expirado, desbloquearlo
        IF @IsLocked = 1 AND @LockoutTime <= GETDATE()
        BEGIN
            UPDATE Security.Users 
            SET IsLocked = 0, FailedLoginAttempts = 0, LockoutTime = NULL
            WHERE UserID = @UserID;
            SET @IsLocked = 0;
            SET @FailedAttempts = 0;
        END
        
        -- Verificar contraseña (en un entorno real, se haría hash con salt)
        IF @StoredPasswordHash = @PasswordHash
        BEGIN
            -- Autenticación exitosa
            SET @AuthResult = 1;
            SET @SessionID = NEWID();
            
            -- Actualizar información de login exitoso
            UPDATE Security.Users 
            SET LastLoginDate = GETDATE(), 
                FailedLoginAttempts = 0,
                IsLocked = 0,
                LockoutTime = NULL
            WHERE UserID = @UserID;
            
            -- Crear sesión
            INSERT INTO Security.UserSessions (SessionID, UserID, IPAddress, UserAgent)
            VALUES (@SessionID, @UserID, @IPAddress, @UserAgent);
            
        END
        ELSE
        BEGIN
            -- Contraseña incorrecta
            SET @AuthResult = 0;
            SET @ErrorMessage = 'Credenciales inválidas';
            
            -- Incrementar intentos fallidos
            SET @FailedAttempts = @FailedAttempts + 1;
            
            -- Bloquear usuario si supera 3 intentos
            IF @FailedAttempts >= 3
            BEGIN
                UPDATE Security.Users 
                SET FailedLoginAttempts = @FailedAttempts,
                    LastFailedLogin = GETDATE(),
                    IsLocked = 1,
                    LockoutTime = DATEADD(MINUTE, 15, GETDATE()) -- Bloqueo por 15 minutos
                WHERE UserID = @UserID;
                
                SET @AuthResult = 2;
                SET @ErrorMessage = 'Usuario bloqueado por múltiples intentos fallidos';
            END
            ELSE
            BEGIN
                UPDATE Security.Users 
                SET FailedLoginAttempts = @FailedAttempts,
                    LastFailedLogin = GETDATE()
                WHERE UserID = @UserID;
            END
        END
        
    END TRY
    BEGIN CATCH
        SET @AuthResult = 0;
        SET @ErrorMessage = ERROR_MESSAGE();
    END CATCH
END
GO

-- =============================================
-- Procedimiento: Cerrar Sesión
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.sp_LogoutUser') AND type in (N'P', N'PC'))
    DROP PROCEDURE Security.sp_LogoutUser;
GO

CREATE PROCEDURE Security.sp_LogoutUser
    @SessionID UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE Security.UserSessions 
    SET IsActive = 0, LogoutTime = GETDATE()
    WHERE SessionID = @SessionID AND IsActive = 1;
END
GO

-- =============================================
-- Procedimiento: Registrar Operación de Auditoría
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Audit.sp_LogOperation') AND type in (N'P', N'PC'))
    DROP PROCEDURE Audit.sp_LogOperation;
GO

CREATE PROCEDURE Audit.sp_LogOperation
    @SessionID UNIQUEIDENTIFIER,
    @UserID INT,
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
        SessionID, UserID, Username, OperationType, TableName, SchemaName,
        RecordCount, OperationStatus, ErrorMessage, ExecutionTimeMs,
        SourceFile, WorksheetName, IPAddress, AdditionalInfo
    )
    VALUES (
        @SessionID, @UserID, @Username, @OperationType, @TableName, @SchemaName,
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
GO

-- =============================================
-- Procedimiento: Inserción Masiva Segura
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Data.sp_BulkInsertData') AND type in (N'P', N'PC'))
    DROP PROCEDURE Data.sp_BulkInsertData;
GO

CREATE PROCEDURE Data.sp_BulkInsertData
    @TableName NVARCHAR(128),
    @SchemaName NVARCHAR(128) = 'dbo',
    @DataXML XML,
    @UserID INT,
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
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @Username NVARCHAR(50);
    
    -- Obtener nombre de usuario
    SELECT @Username = Username FROM Security.Users WHERE UserID = @UserID;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Aquí iría la lógica de inserción masiva usando el XML
        -- Por simplicidad, este es un ejemplo básico
        -- En la implementación real, se parseará el XML y se insertarán los datos
        
        SET @RecordsInserted = 0; -- Placeholder
        SET @ErrorMessage = NULL;
        
        -- Registrar operación exitosa
        EXEC Audit.sp_LogOperation 
            @SessionID = @SessionID,
            @UserID = @UserID,
            @Username = @Username,
            @OperationType = 'BULK_INSERT',
            @TableName = @TableName,
            @SchemaName = @SchemaName,
            @RecordCount = @RecordsInserted,
            @OperationStatus = 'SUCCESS',
            @ExecutionTimeMs = DATEDIFF(MILLISECOND, @StartTime, GETDATE()),
            @SourceFile = @SourceFile,
            @WorksheetName = @WorksheetName,
            @LogID = @LogID OUTPUT;
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @RecordsInserted = 0;
        
        -- Registrar operación fallida
        EXEC Audit.sp_LogOperation 
            @SessionID = @SessionID,
            @UserID = @UserID,
            @Username = @Username,
            @OperationType = 'BULK_INSERT',
            @TableName = @TableName,
            @SchemaName = @SchemaName,
            @RecordCount = 0,
            @OperationStatus = 'FAILED',
            @ErrorMessage = @ErrorMessage,
            @ExecutionTimeMs = DATEDIFF(MILLISECOND, @StartTime, GETDATE()),
            @SourceFile = @SourceFile,
            @WorksheetName = @WorksheetName,
            @LogID = @LogID OUTPUT;
    END CATCH
END
GO

-- =============================================
-- Procedimiento: Crear Usuario
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.sp_CreateUser') AND type in (N'P', N'PC'))
    DROP PROCEDURE Security.sp_CreateUser;
GO

CREATE PROCEDURE Security.sp_CreateUser
    @Username NVARCHAR(50),
    @PasswordHash NVARCHAR(255),
    @Salt NVARCHAR(255),
    @Email NVARCHAR(100) = NULL,
    @FullName NVARCHAR(100) = NULL,
    @CreatedBy NVARCHAR(50),
    @UserID INT OUTPUT,
    @ErrorMessage NVARCHAR(255) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Verificar si el usuario ya existe
        IF EXISTS (SELECT 1 FROM Security.Users WHERE Username = @Username)
        BEGIN
            SET @ErrorMessage = 'El nombre de usuario ya existe';
            SET @UserID = NULL;
            RETURN;
        END
        
        -- Insertar nuevo usuario
        INSERT INTO Security.Users (Username, PasswordHash, Salt, Email, FullName, CreatedBy)
        VALUES (@Username, @PasswordHash, @Salt, @Email, @FullName, @CreatedBy);
        
        SET @UserID = SCOPE_IDENTITY();
        SET @ErrorMessage = NULL;
        
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @UserID = NULL;
    END CATCH
END
GO

PRINT 'Procedimientos almacenados creados exitosamente.';

