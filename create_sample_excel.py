#!/usr/bin/env python3
"""
Script para crear archivos Excel de ejemplo para pruebas.
"""

import pandas as pd
import os
from datetime import datetime, date

def create_customers_excel():
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
    
    df = pd.DataFrame(customers_data)
    file_path = '/home/ubuntu/excel_sql_integration/examples/sample_customers.xlsx'
    df.to_excel(file_path, index=False, sheet_name='Customers')
    print(f"Archivo creado: {file_path}")
    return file_path

def create_products_excel():
    products_data = {
        'ProductCode': ['PROD006', 'PROD007', 'PROD008', 'PROD009', 'PROD010'],
        'ProductName': ['Smart Phone', 'Tablet Pro', 'Wireless Headphones', 'Smart Watch', 'Bluetooth Speaker'],
        'Category': ['Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics'],
        'UnitPrice': [699.99, 899.99, 199.99, 299.99, 149.99],
        'UnitsInStock': [25, 15, 100, 50, 75],
        'ReorderLevel': [5, 3, 20, 10, 15],
        'Discontinued': [False, False, False, False, False]
    }
    
    df = pd.DataFrame(products_data)
    file_path = '/home/ubuntu/excel_sql_integration/examples/sample_products.xlsx'
    df.to_excel(file_path, index=False, sheet_name='Products')
    print(f"Archivo creado: {file_path}")
    return file_path

def main():
    print("Creando archivos Excel de ejemplo...")
    os.makedirs('/home/ubuntu/excel_sql_integration/examples', exist_ok=True)
    create_customers_excel()
    create_products_excel()
    print("Archivos Excel de ejemplo creados exitosamente!")

if __name__ == "__main__":
    main()

