import pandas as pd
import os

# Crear datos de ejemplo para clientes
customers_data = {
    'CustomerCode': ['CUST006', 'CUST007', 'CUST008', 'CUST009', 'CUST010'],
    'CompanyName': ['ABC Corp', 'XYZ Industries', 'Tech Innovations', 'Global Solutions', 'Future Systems'],
    'ContactName': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson'],
    'Email': ['john@abc.com', 'jane@xyz.com', 'bob@tech.com', 'alice@global.com', 'charlie@future.com'],
    'Phone': ['+1-555-1001', '+1-555-1002', '+1-555-1003', '+1-555-1004', '+1-555-1005'],
    'Address': ['123 Main St', '456 Oak Ave', '789 Pine Rd', '321 Elm St', '654 Maple Dr'],
    'City': ['New York', 'Chicago', 'San Francisco', 'Boston', 'Seattle'],
    'Country': ['USA', 'USA', 'USA', 'USA', 'USA']
}

# Crear DataFrame
df = pd.DataFrame(customers_data)

# Crear directorio si no existe
# os.makedirs('examples', exist_ok=True)

# Guardar como Excel
df.to_excel('sample_customers.xlsx', index=False)

print("Archivo Excel de ejemplo creado exitosamente")
