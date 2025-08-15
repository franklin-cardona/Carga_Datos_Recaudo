#!/usr/bin/env python3
"""
Script de ejecución principal para Excel-SQL Integration
Este script resuelve los problemas de importación relativa ejecutando la aplicación correctamente.

Autor: FRANKLIN ANDRES CARDONA YARA
Fecha: 2025-01-08
"""

import sys
import os

# Agregar el directorio del proyecto al path de Python
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

# Importar y ejecutar la aplicación
if __name__ == "__main__":
    try:
        from src.main import main
        main()
    except ImportError as e:
        print(f"Error de importación: {e}")
        print("Asegúrate de que todas las dependencias estén instaladas:")
        print("pip install pyodbc pandas fuzzywuzzy python-Levenshtein openpyxl")
        sys.exit(1)
    except Exception as e:
        print(f"Error ejecutando aplicación: {e}")
        sys.exit(1)
