"""
Módulo de mapeo de tablas para la integración Excel-SQL Server.
Proporciona funcionalidades para mapear columnas de Excel a columnas de SQL Server.

Autor: Manus AI
Fecha: 2025-01-08
"""

from typing import List, Dict, Any
import logging
import difflib


class TableMapper:
    """
    Clase para mapear columnas de Excel a columnas de SQL Server.
    """

    def __init__(self, db_connection):
        """
        Inicializa el mapeador de tablas.

        Args:
            db_connection: Conexión a la base de datos
        """
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)

    def suggest_column_mappings(self, excel_columns: List[str],
                                sql_columns: List[str],
                                table_structure: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sugiere mapeos entre columnas de Excel y columnas de SQL Server.

        Args:
            excel_columns: Lista de nombres de columnas de Excel
            sql_columns: Lista de nombres de columnas de SQL Server
            table_structure: Estructura de la tabla SQL

        Returns:
            Lista de mapeos sugeridos con confianza
        """
        mappings = []

        # Crear diccionario de tipos de datos para referencia rápida
        data_types = {col['COLUMN_NAME']: col['DATA_TYPE']
                      for col in table_structure}

        # Para cada columna de Excel, encontrar la mejor coincidencia en SQL
        for excel_col in excel_columns:
            # Usar difflib para encontrar coincidencias aproximadas
            matches = difflib.get_close_matches(
                excel_col, sql_columns, n=3, cutoff=0.6)

            if matches:
                best_match = matches[0]
                # Calcular puntuación de similitud
                similarity = difflib.SequenceMatcher(
                    None, excel_col.lower(), best_match.lower()).ratio()

                mappings.append({
                    'excel_column': excel_col,
                    'sql_column': best_match,
                    'data_type': data_types.get(best_match, 'unknown'),
                    'confidence': similarity,
                    'matches': matches
                })
            else:
                # No se encontraron coincidencias buenas
                mappings.append({
                    'excel_column': excel_col,
                    'sql_column': '',
                    'data_type': '',
                    'confidence': 0.0,
                    'matches': []
                })

        return mappings
