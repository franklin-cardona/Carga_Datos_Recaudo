-- =============================================
-- Script: Datos de Ejemplo
-- Descripción: Inserta datos de ejemplo para pruebas y demostración
-- Autor: Manus AI
-- Fecha: 2025-01-08
-- =============================================

USE ExcelSQLIntegration;
GO

-- =============================================
-- Insertar usuarios de ejemplo
-- =============================================

-- Función simple para generar hash (en producción usar bcrypt o similar)
-- Por simplicidad, usaremos hash básico para demostración
DECLARE @AdminSalt NVARCHAR(255) = 'admin_salt_123';
DECLARE @AdminHash NVARCHAR(255) = 'admin_hash_456'; -- En producción: hash de 'admin123' + salt

DECLARE @UserSalt NVARCHAR(255) = 'user_salt_789';
DECLARE @UserHash NVARCHAR(255) = 'user_hash_012'; -- En producción: hash de 'user123' + salt

-- Insertar usuario administrador
IF NOT EXISTS (SELECT * FROM Security.Users WHERE Username = 'admin')
BEGIN
    INSERT INTO Security.Users (
        Username, PasswordHash, Salt, Email, FullName, CreatedBy
    )
    VALUES (
        'admin', @AdminHash, @AdminSalt, 'admin@company.com', 'Administrador del Sistema', 'SYSTEM'
    );
    
    DECLARE @AdminUserID INT = SCOPE_IDENTITY();
    
    -- Asignar rol de administrador
    INSERT INTO Security.UserRoles (UserID, RoleID)
    SELECT @AdminUserID, RoleID FROM Security.Roles WHERE RoleName = 'Administrator';
    
    PRINT 'Usuario administrador creado: admin / admin123';
END

-- Insertar usuario operador
IF NOT EXISTS (SELECT * FROM Security.Users WHERE Username = 'dataoperator')
BEGIN
    INSERT INTO Security.Users (
        Username, PasswordHash, Salt, Email, FullName, CreatedBy
    )
    VALUES (
        'dataoperator', @UserHash, @UserSalt, 'operator@company.com', 'Operador de Datos', 'admin'
    );
    
    DECLARE @OperatorUserID INT = SCOPE_IDENTITY();
    
    -- Asignar rol de operador
    INSERT INTO Security.UserRoles (UserID, RoleID)
    SELECT @OperatorUserID, RoleID FROM Security.Roles WHERE RoleName = 'DataOperator';
    
    PRINT 'Usuario operador creado: dataoperator / user123';
END

-- Insertar usuario de solo lectura
IF NOT EXISTS (SELECT * FROM Security.Users WHERE Username = 'readonly')
BEGIN
    INSERT INTO Security.Users (
        Username, PasswordHash, Salt, Email, FullName, CreatedBy
    )
    VALUES (
        'readonly', @UserHash, @UserSalt, 'readonly@company.com', 'Usuario Solo Lectura', 'admin'
    );
    
    DECLARE @ReadOnlyUserID INT = SCOPE_IDENTITY();
    
    -- Asignar rol de solo lectura
    INSERT INTO Security.UserRoles (UserID, RoleID)
    SELECT @ReadOnlyUserID, RoleID FROM Security.Roles WHERE RoleName = 'ReadOnly';
    
    PRINT 'Usuario solo lectura creado: readonly / user123';
END

-- =============================================
-- Insertar datos de ejemplo en la tabla Customers
-- =============================================

IF NOT EXISTS (SELECT * FROM Data.Customers WHERE CustomerCode = 'CUST001')
BEGIN
    INSERT INTO Data.Customers (
        CustomerCode, CompanyName, ContactName, Email, Phone, Address, City, Country, CreatedBy
    )
    VALUES 
    ('CUST001', 'Acme Corporation', 'John Smith', 'john.smith@acme.com', '+1-555-0101', '123 Main St', 'New York', 'USA', 'SYSTEM'),
    ('CUST002', 'Global Industries', 'Maria Garcia', 'maria.garcia@global.com', '+1-555-0102', '456 Oak Ave', 'Los Angeles', 'USA', 'SYSTEM'),
    ('CUST003', 'Tech Solutions Ltd', 'David Johnson', 'david.johnson@techsol.com', '+44-20-7946-0958', '789 High Street', 'London', 'UK', 'SYSTEM'),
    ('CUST004', 'Innovate Systems', 'Sarah Wilson', 'sarah.wilson@innovate.com', '+1-555-0104', '321 Pine Rd', 'Chicago', 'USA', 'SYSTEM'),
    ('CUST005', 'Digital Dynamics', 'Michael Brown', 'michael.brown@digital.com', '+49-30-12345678', 'Unter den Linden 1', 'Berlin', 'Germany', 'SYSTEM');
    
    PRINT 'Datos de ejemplo insertados en la tabla Customers.';
END

-- =============================================
-- Crear tabla de ejemplo para productos
-- =============================================

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Data.Products') AND type in (N'U'))
BEGIN
    CREATE TABLE Data.Products (
        ProductID INT IDENTITY(1,1) PRIMARY KEY,
        ProductCode NVARCHAR(20) NOT NULL UNIQUE,
        ProductName NVARCHAR(100) NOT NULL,
        Category NVARCHAR(50),
        UnitPrice DECIMAL(10,2) NOT NULL DEFAULT 0,
        UnitsInStock INT NOT NULL DEFAULT 0,
        ReorderLevel INT NOT NULL DEFAULT 0,
        Discontinued BIT NOT NULL DEFAULT 0,
        SupplierID INT,
        Description NVARCHAR(500),
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        ModifiedDate DATETIME2,
        CreatedBy NVARCHAR(50) NOT NULL DEFAULT 'SYSTEM',
        ModifiedBy NVARCHAR(50)
    );
    
    CREATE INDEX IX_Products_ProductCode ON Data.Products(ProductCode);
    CREATE INDEX IX_Products_Category ON Data.Products(Category);
    CREATE INDEX IX_Products_UnitPrice ON Data.Products(UnitPrice);
    
    PRINT 'Tabla Data.Products creada exitosamente.';
END

-- Insertar productos de ejemplo
IF NOT EXISTS (SELECT * FROM Data.Products WHERE ProductCode = 'PROD001')
BEGIN
    INSERT INTO Data.Products (
        ProductCode, ProductName, Category, UnitPrice, UnitsInStock, ReorderLevel, Description, CreatedBy
    )
    VALUES 
    ('PROD001', 'Laptop Professional', 'Electronics', 1299.99, 50, 10, 'High-performance laptop for business use', 'SYSTEM'),
    ('PROD002', 'Wireless Mouse', 'Electronics', 29.99, 200, 25, 'Ergonomic wireless mouse with USB receiver', 'SYSTEM'),
    ('PROD003', 'Office Chair', 'Furniture', 249.99, 30, 5, 'Ergonomic office chair with lumbar support', 'SYSTEM'),
    ('PROD004', 'Desk Lamp', 'Furniture', 79.99, 75, 15, 'LED desk lamp with adjustable brightness', 'SYSTEM'),
    ('PROD005', 'Notebook Set', 'Stationery', 15.99, 150, 30, 'Set of 3 professional notebooks', 'SYSTEM');
    
    PRINT 'Datos de ejemplo insertados en la tabla Products.';
END

-- =============================================
-- Crear tabla de ejemplo para órdenes
-- =============================================

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Data.Orders') AND type in (N'U'))
BEGIN
    CREATE TABLE Data.Orders (
        OrderID INT IDENTITY(1,1) PRIMARY KEY,
        OrderNumber NVARCHAR(20) NOT NULL UNIQUE,
        CustomerID INT NOT NULL,
        OrderDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        RequiredDate DATETIME2,
        ShippedDate DATETIME2,
        ShipAddress NVARCHAR(200),
        ShipCity NVARCHAR(50),
        ShipCountry NVARCHAR(50),
        OrderStatus NVARCHAR(20) NOT NULL DEFAULT 'Pending', -- Pending, Processing, Shipped, Delivered, Cancelled
        TotalAmount DECIMAL(12,2) NOT NULL DEFAULT 0,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        ModifiedDate DATETIME2,
        CreatedBy NVARCHAR(50) NOT NULL DEFAULT 'SYSTEM',
        ModifiedBy NVARCHAR(50),
        CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID) REFERENCES Data.Customers(CustomerID)
    );
    
    CREATE INDEX IX_Orders_OrderNumber ON Data.Orders(OrderNumber);
    CREATE INDEX IX_Orders_CustomerID ON Data.Orders(CustomerID);
    CREATE INDEX IX_Orders_OrderDate ON Data.Orders(OrderDate);
    CREATE INDEX IX_Orders_OrderStatus ON Data.Orders(OrderStatus);
    
    PRINT 'Tabla Data.Orders creada exitosamente.';
END

-- =============================================
-- Crear vista de ejemplo para reportes
-- =============================================

IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'vw_CustomerOrderSummary')
BEGIN
    EXEC('
    CREATE VIEW Data.vw_CustomerOrderSummary AS
    SELECT 
        c.CustomerID,
        c.CustomerCode,
        c.CompanyName,
        c.ContactName,
        c.Email,
        c.City,
        c.Country,
        COUNT(o.OrderID) as TotalOrders,
        ISNULL(SUM(o.TotalAmount), 0) as TotalOrderValue,
        MAX(o.OrderDate) as LastOrderDate,
        CASE 
            WHEN MAX(o.OrderDate) >= DATEADD(MONTH, -3, GETDATE()) THEN ''Active''
            WHEN MAX(o.OrderDate) >= DATEADD(MONTH, -12, GETDATE()) THEN ''Inactive''
            ELSE ''Dormant''
        END as CustomerStatus
    FROM Data.Customers c
    LEFT JOIN Data.Orders o ON c.CustomerID = o.CustomerID
    WHERE c.IsActive = 1
    GROUP BY c.CustomerID, c.CustomerCode, c.CompanyName, c.ContactName, c.Email, c.City, c.Country
    ');
    
    PRINT 'Vista Data.vw_CustomerOrderSummary creada exitosamente.';
END
GO

-- =============================================
-- Insertar algunos registros de auditoría de ejemplo
-- =============================================

-- Simular algunas operaciones de auditoría
DECLARE @SampleLogID BIGINT;

INSERT INTO Audit.OperationLog (
    UserID, Username, OperationType, TableName, SchemaName, RecordCount, 
    OperationStatus, SourceFile, WorksheetName, ApplicationName
)
VALUES 
(1, 'admin', 'BULK_INSERT', 'Customers', 'Data', 5, 'SUCCESS', 'customers_import.xlsx', 'Sheet1', 'Excel-SQL Integration'),
(1, 'admin', 'INSERT', 'Products', 'Data', 1, 'SUCCESS', NULL, NULL, 'Excel-SQL Integration'),
(2, 'dataoperator', 'UPDATE', 'Customers', 'Data', 2, 'SUCCESS', 'customer_updates.xlsx', 'Updates', 'Excel-SQL Integration');

PRINT 'Datos de ejemplo insertados exitosamente.';
PRINT '';
PRINT '=== RESUMEN DE CONFIGURACIÓN ===';
PRINT 'Base de datos: ExcelSQLIntegration';
PRINT 'Esquemas: Security, Audit, Data';
PRINT '';
PRINT 'Usuarios de prueba creados:';
PRINT '  - admin / admin123 (Administrador)';
PRINT '  - dataoperator / user123 (Operador de Datos)';
PRINT '  - readonly / user123 (Solo Lectura)';
PRINT '';
PRINT 'Tablas de ejemplo:';
PRINT '  - Data.Customers (5 registros)';
PRINT '  - Data.Products (5 registros)';
PRINT '  - Data.Orders (estructura creada)';
PRINT '';
PRINT 'Sistema de auditoría configurado y listo para usar.';
PRINT 'La base de datos está lista para la integración con la aplicación Python.';

