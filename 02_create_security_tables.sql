-- =============================================
-- Script: Creación de Tablas de Seguridad
-- Descripción: Crea las tablas necesarias para el sistema de autenticación y seguridad
-- Autor: Manus AI
-- Fecha: 2025-01-08
-- =============================================

USE ExcelSQLIntegration;
GO

-- Tabla de usuarios para autenticación
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.Users') AND type in (N'U'))
BEGIN
    CREATE TABLE Security.Users (
        UserID INT IDENTITY(1,1) PRIMARY KEY,
        Username NVARCHAR(50) NOT NULL UNIQUE,
        PasswordHash NVARCHAR(255) NOT NULL,
        Salt NVARCHAR(255) NOT NULL,
        Email NVARCHAR(100),
        FullName NVARCHAR(100),
        IsActive BIT NOT NULL DEFAULT 1,
        FailedLoginAttempts INT NOT NULL DEFAULT 0,
        LastFailedLogin DATETIME2,
        IsLocked BIT NOT NULL DEFAULT 0,
        LockoutTime DATETIME2,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        LastLoginDate DATETIME2,
        CreatedBy NVARCHAR(50) NOT NULL DEFAULT 'SYSTEM',
        ModifiedDate DATETIME2,
        ModifiedBy NVARCHAR(50)
    );
    
    -- Índices para optimizar consultas
    CREATE INDEX IX_Users_Username ON Security.Users(Username);
    CREATE INDEX IX_Users_IsActive ON Security.Users(IsActive);
    CREATE INDEX IX_Users_IsLocked ON Security.Users(IsLocked);
    
    PRINT 'Tabla Security.Users creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La tabla Security.Users ya existe.';
END
GO

-- Tabla de roles (para futuras expansiones)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.Roles') AND type in (N'U'))
BEGIN
    CREATE TABLE Security.Roles (
        RoleID INT IDENTITY(1,1) PRIMARY KEY,
        RoleName NVARCHAR(50) NOT NULL UNIQUE,
        Description NVARCHAR(255),
        IsActive BIT NOT NULL DEFAULT 1,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        CreatedBy NVARCHAR(50) NOT NULL DEFAULT 'SYSTEM'
    );
    
    PRINT 'Tabla Security.Roles creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La tabla Security.Roles ya existe.';
END
GO

-- Tabla de asignación de roles a usuarios
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.UserRoles') AND type in (N'U'))
BEGIN
    CREATE TABLE Security.UserRoles (
        UserRoleID INT IDENTITY(1,1) PRIMARY KEY,
        UserID INT NOT NULL,
        RoleID INT NOT NULL,
        AssignedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        AssignedBy NVARCHAR(50) NOT NULL DEFAULT 'SYSTEM',
        CONSTRAINT FK_UserRoles_Users FOREIGN KEY (UserID) REFERENCES Security.Users(UserID),
        CONSTRAINT FK_UserRoles_Roles FOREIGN KEY (RoleID) REFERENCES Security.Roles(RoleID),
        CONSTRAINT UK_UserRoles_UserRole UNIQUE (UserID, RoleID)
    );
    
    PRINT 'Tabla Security.UserRoles creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La tabla Security.UserRoles ya existe.';
END
GO

-- Tabla de sesiones activas (para control de sesiones)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Security.UserSessions') AND type in (N'U'))
BEGIN
    CREATE TABLE Security.UserSessions (
        SessionID UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        UserID INT NOT NULL,
        LoginTime DATETIME2 NOT NULL DEFAULT GETDATE(),
        LastActivityTime DATETIME2 NOT NULL DEFAULT GETDATE(),
        IPAddress NVARCHAR(45),
        UserAgent NVARCHAR(500),
        IsActive BIT NOT NULL DEFAULT 1,
        LogoutTime DATETIME2,
        CONSTRAINT FK_UserSessions_Users FOREIGN KEY (UserID) REFERENCES Security.Users(UserID)
    );
    
    -- Índices para optimizar consultas
    CREATE INDEX IX_UserSessions_UserID ON Security.UserSessions(UserID);
    CREATE INDEX IX_UserSessions_IsActive ON Security.UserSessions(IsActive);
    CREATE INDEX IX_UserSessions_LastActivity ON Security.UserSessions(LastActivityTime);
    
    PRINT 'Tabla Security.UserSessions creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'La tabla Security.UserSessions ya existe.';
END
GO

-- Insertar roles por defecto
IF NOT EXISTS (SELECT * FROM Security.Roles WHERE RoleName = 'Administrator')
BEGIN
    INSERT INTO Security.Roles (RoleName, Description)
    VALUES ('Administrator', 'Administrador del sistema con acceso completo');
    PRINT 'Rol Administrator insertado.';
END

IF NOT EXISTS (SELECT * FROM Security.Roles WHERE RoleName = 'DataOperator')
BEGIN
    INSERT INTO Security.Roles (RoleName, Description)
    VALUES ('DataOperator', 'Operador de datos con permisos de lectura y escritura');
    PRINT 'Rol DataOperator insertado.';
END

IF NOT EXISTS (SELECT * FROM Security.Roles WHERE RoleName = 'ReadOnly')
BEGIN
    INSERT INTO Security.Roles (RoleName, Description)
    VALUES ('ReadOnly', 'Usuario de solo lectura');
    PRINT 'Rol ReadOnly insertado.';
END

PRINT 'Tablas de seguridad configuradas correctamente.';

