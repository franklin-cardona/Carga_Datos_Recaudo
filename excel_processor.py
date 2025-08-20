"""
Procesador de Archivos Excel
Proporciona funcionalidad para leer, procesar y mapear archivos Excel a estructuras de base de datos.

Autor: FRANKLIN ANDRES CARDONA YARA
Fecha: 2025-01-08
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Union
import logging
import os
from datetime import datetime, date
import re
from fuzzywuzzy import fuzz, process
from dataclasses import dataclass
from enum import Enum


class DataType(Enum):
    """Enumeración de tipos de datos soportados."""
    STRING = "NVARCHAR"
    INTEGER = "INT"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BIT"
    DATE = "DATE"
    DATETIME = "DATETIME2"
    UNKNOWN = "NVARCHAR"


@dataclass
class ColumnMapping:
    """Representa el mapeo entre una columna de Excel y una columna de base de datos."""
    excel_column: str
    db_column: str
    excel_index: int
    data_type: DataType
    max_length: Optional[int] = None
    is_nullable: bool = True
    confidence_score: float = 0.0
    validation_errors: List[str] = None

    def __post_init__(self):
        if self.validation_errors is None:
            self.validation_errors = []


@dataclass
class WorksheetMapping:
    """Representa el mapeo entre una hoja de Excel y una tabla de base de datos."""
    worksheet_name: str
    table_name: str
    schema_name: str = "Data"
    column_mappings: List[ColumnMapping] = None
    confidence_score: float = 0.0
    row_count: int = 0
    has_headers: bool = True
    header_row: int = 0
    data_start_row: int = 1

    def __post_init__(self):
        if self.column_mappings is None:
            self.column_mappings = []


@dataclass
class ValidationResult:
    """Resultado de validación de datos."""
    is_valid: bool
    errors: List[Dict[str, Any]] = None
    warnings: List[Dict[str, Any]] = None
    processed_rows: int = 0
    valid_rows: int = 0

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []


class ExcelProcessor:
    """
    Procesador principal para archivos Excel.
    """

    def __init__(self, fuzzy_threshold: float = 0.8):
        """
        Inicializa el procesador de Excel.

        Args:
            fuzzy_threshold: Umbral para coincidencia difusa (0.0 - 1.0)
        """
        self.fuzzy_threshold = fuzzy_threshold
        self.logger = logging.getLogger(__name__)
        self.supported_extensions = ['.xlsx', '.xls', '.xlsm']

        # Patrones para detección de tipos de datos
        self.date_patterns = [
            r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$',  # MM/DD/YYYY, DD/MM/YYYY
            r'^\d{4}[/-]\d{1,2}[/-]\d{1,2}$',    # YYYY/MM/DD
            r'^\d{1,2}-\w{3}-\d{2,4}$',          # DD-MMM-YYYY
        ]

        self.datetime_patterns = [
            r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\s+\d{1,2}:\d{2}(:\d{2})?(\s*(AM|PM))?$',
        ]

        self.number_patterns = [
            r'^-?\d+$',                          # Enteros
            r'^-?\d+\.\d+$',                     # Decimales
            r'^-?\d{1,3}(,\d{3})*(\.\d+)?$',    # Números con comas
        ]

        self.boolean_values = {
            'true', 'false', 'yes', 'no', 'y', 'n', '1', '0',
            'verdadero', 'falso', 'sí', 'si', 'no'
        }

    def validate_file(self, file_path: str) -> Tuple[bool, Optional[str]]:
        """
        Valida si el archivo Excel es válido y accesible.

        Args:
            file_path: Ruta al archivo Excel

        Returns:
            Tupla (es_válido, mensaje_error)
        """
        try:
            # Verificar existencia del archivo
            if not os.path.exists(file_path):
                return False, "El archivo no existe"

            # Verificar extensión
            _, ext = os.path.splitext(file_path.lower())
            if ext not in self.supported_extensions:
                return False, f"Extensión no soportada. Extensiones válidas: {', '.join(self.supported_extensions)}"

            # Verificar tamaño del archivo
            file_size = os.path.getsize(file_path)
            max_size = 1000 * 1024 * 1024  # 1000MB
            if file_size > max_size:
                return False, f"El archivo es demasiado grande ({file_size / 1024 / 1024:.1f}MB). Máximo permitido: 1000MB"

            # Intentar abrir el archivo
            try:
                pd.read_excel(file_path, sheet_name=None, nrows=1)
            except Exception as e:
                return False, f"Error leyendo archivo Excel: {str(e)}"

            return True, None

        except Exception as e:
            return False, f"Error validando archivo: {str(e)}"

    def get_worksheet_info(self, file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Obtiene información básica de todas las hojas del archivo Excel.

        Args:
            file_path: Ruta al archivo Excel

        Returns:
            Diccionario con información de cada hoja
        """
        try:
            # Leer información de todas las hojas
            excel_file = pd.ExcelFile(file_path)
            worksheet_info = {}

            for sheet_name in excel_file.sheet_names:
                try:
                    # Leer solo las primeras filas para obtener información básica
                    df = pd.read_excel(
                        file_path, sheet_name=sheet_name, nrows=10)

                    # Obtener información completa de la hoja
                    full_df = pd.read_excel(file_path, sheet_name=sheet_name)

                    worksheet_info[sheet_name] = {
                        'row_count': len(full_df),
                        'column_count': len(full_df.columns),
                        'columns': list(full_df.columns),
                        'has_data': len(full_df) > 0,
                        'sample_data': df.head(5).to_dict('records') if len(df) > 0 else []
                    }

                except Exception as e:
                    self.logger.warning(
                        f"Error leyendo hoja '{sheet_name}': {str(e)}")
                    worksheet_info[sheet_name] = {
                        'error': str(e),
                        'row_count': 0,
                        'column_count': 0,
                        'columns': [],
                        'has_data': False,
                        'sample_data': []
                    }

            return worksheet_info

        except Exception as e:
            self.logger.error(
                f"Error obteniendo información de hojas: {str(e)}")
            raise

    def detect_column_types(self, df: pd.DataFrame) -> Dict[str, DataType]:
        """
        Detecta automáticamente los tipos de datos de las columnas.

        Args:
            df: DataFrame de pandas

        Returns:
            Diccionario con tipos de datos detectados
        """
        column_types = {}

        for column in df.columns:
            series = df[column].dropna()  # Ignorar valores nulos

            if len(series) == 0:
                column_types[column] = DataType.STRING
                continue

            # Convertir a string para análisis de patrones
            string_series = series.astype(str)

            # Contadores para cada tipo
            type_counts = {
                DataType.INTEGER: 0,
                DataType.DECIMAL: 0,
                DataType.DATE: 0,
                DataType.DATETIME: 0,
                DataType.BOOLEAN: 0,
                DataType.STRING: 0
            }

            for value in string_series:
                value = value.strip()

                # Verificar booleano
                if value.lower() in self.boolean_values:
                    type_counts[DataType.BOOLEAN] += 1
                    continue

                # Verificar fecha/hora
                is_datetime = any(re.match(pattern, value, re.IGNORECASE)
                                  for pattern in self.datetime_patterns)
                if is_datetime:
                    type_counts[DataType.DATETIME] += 1
                    continue

                # Verificar fecha
                is_date = any(re.match(pattern, value, re.IGNORECASE)
                              for pattern in self.date_patterns)
                if is_date:
                    type_counts[DataType.DATE] += 1
                    continue

                # Verificar número
                is_integer = re.match(
                    self.number_patterns[0], value.replace(',', ''))
                if is_integer:
                    type_counts[DataType.INTEGER] += 1
                    continue

                is_decimal = re.match(self.number_patterns[1], value.replace(',', '')) or \
                    re.match(self.number_patterns[2], value)
                if is_decimal:
                    type_counts[DataType.DECIMAL] += 1
                    continue

                # Por defecto, string
                type_counts[DataType.STRING] += 1

            # Determinar tipo predominante
            total_values = sum(type_counts.values())
            if total_values == 0:
                column_types[column] = DataType.STRING
            else:
                # Calcular porcentajes
                percentages = {dtype: count / total_values
                               for dtype, count in type_counts.items()}

                # Seleccionar tipo con mayor porcentaje (mínimo 60%)
                max_type = max(percentages, key=percentages.get)
                if percentages[max_type] >= 0.6:
                    column_types[column] = max_type
                else:
                    column_types[column] = DataType.STRING

        return column_types

    def fuzzy_match_columns(self, excel_columns: List[str],
                            db_columns: List[str]) -> List[ColumnMapping]:
        """
        Realiza coincidencia difusa entre columnas de Excel y base de datos.

        Args:
            excel_columns: Lista de nombres de columnas de Excel
            db_columns: Lista de nombres de columnas de base de datos

        Returns:
            Lista de mapeos de columnas
        """
        mappings = []
        used_db_columns = set()

        for i, excel_col in enumerate(excel_columns):
            best_match = None
            best_score = 0

            # Buscar la mejor coincidencia
            for db_col in db_columns:
                if db_col in used_db_columns:
                    continue

                # Calcular diferentes tipos de similitud
                ratio_score = fuzz.ratio(excel_col.lower(), db_col.lower())
                partial_score = fuzz.partial_ratio(
                    excel_col.lower(), db_col.lower())
                token_sort_score = fuzz.token_sort_ratio(
                    excel_col.lower(), db_col.lower())
                token_set_score = fuzz.token_set_ratio(
                    excel_col.lower(), db_col.lower())

                # Promedio ponderado de los scores
                combined_score = (ratio_score * 0.4 +
                                  partial_score * 0.2 +
                                  token_sort_score * 0.2 +
                                  token_set_score * 0.2)

                if combined_score > best_score:
                    best_score = combined_score
                    best_match = db_col

            # Crear mapeo si supera el umbral
            if best_match and best_score >= (self.fuzzy_threshold * 100):
                mapping = ColumnMapping(
                    excel_column=excel_col,
                    db_column=best_match,
                    excel_index=i,
                    data_type=DataType.UNKNOWN,  # Se determinará después
                    confidence_score=best_score / 100
                )
                mappings.append(mapping)
                used_db_columns.add(best_match)
            else:
                # Crear mapeo con baja confianza
                mapping = ColumnMapping(
                    excel_column=excel_col,
                    db_column=excel_col,  # Usar nombre original
                    excel_index=i,
                    data_type=DataType.UNKNOWN,
                    confidence_score=0.0
                )
                mapping.validation_errors.append(
                    f"No se encontró coincidencia para la columna '{excel_col}'"
                )
                mappings.append(mapping)

        return mappings

    def map_worksheet_to_table(self, file_path: str, worksheet_name: str,
                               available_tables: Dict[str, List[str]]) -> WorksheetMapping:
        """
        Mapea una hoja de Excel a una tabla de base de datos.

        Args:
            file_path: Ruta al archivo Excel
            worksheet_name: Nombre de la hoja
            available_tables: Diccionario {tabla: [columnas]}

        Returns:
            Mapeo de la hoja de trabajo
        """
        try:
            # Leer la hoja de Excel
            df = pd.read_excel(file_path, sheet_name=worksheet_name)

            if df.empty:
                raise ValueError(f"La hoja '{worksheet_name}' está vacía")

            # Detectar tipos de columnas
            column_types = self.detect_column_types(df)

            # Encontrar la mejor tabla coincidente
            best_table = None
            best_score = 0

            for table_name, table_columns in available_tables.items():
                # Calcular similitud entre nombre de hoja y tabla
                name_score = fuzz.ratio(
                    worksheet_name.lower(), table_name.lower())

                # Calcular similitud de columnas
                excel_columns = list(df.columns)
                column_mappings = self.fuzzy_match_columns(
                    excel_columns, table_columns)

                # Calcular score promedio de mapeo de columnas
                if column_mappings:
                    avg_column_score = sum(
                        m.confidence_score for m in column_mappings) / len(column_mappings)
                    combined_score = (name_score * 0.3 +
                                      avg_column_score * 100 * 0.7)
                else:
                    combined_score = name_score * 0.3

                if combined_score > best_score:
                    best_score = combined_score
                    best_table = table_name

            # Crear mapeo de hoja de trabajo
            if best_table:
                table_columns = available_tables[best_table]
                column_mappings = self.fuzzy_match_columns(
                    list(df.columns), table_columns)

                # Asignar tipos de datos detectados
                for mapping in column_mappings:
                    if mapping.excel_column in column_types:
                        mapping.data_type = column_types[mapping.excel_column]

                worksheet_mapping = WorksheetMapping(
                    worksheet_name=worksheet_name,
                    table_name=best_table,
                    column_mappings=column_mappings,
                    confidence_score=best_score / 100,
                    row_count=len(df),
                    has_headers=True,
                    header_row=0,
                    data_start_row=1
                )
            else:
                # No se encontró tabla coincidente
                worksheet_mapping = WorksheetMapping(
                    worksheet_name=worksheet_name,
                    table_name=worksheet_name,  # Usar nombre de hoja como tabla
                    column_mappings=[],
                    confidence_score=0.0,
                    row_count=len(df)
                )

            return worksheet_mapping

        except Exception as e:
            self.logger.error(
                f"Error mapeando hoja '{worksheet_name}': {str(e)}")
            raise

    def validate_data(self, file_path: str, worksheet_mapping: WorksheetMapping) -> ValidationResult:
        """
        Valida los datos de una hoja de Excel según el mapeo definido.

        Args:
            file_path: Ruta al archivo Excel
            worksheet_mapping: Mapeo de la hoja de trabajo

        Returns:
            Resultado de validación
        """
        try:
            # Leer datos de la hoja
            df = pd.read_excel(
                file_path, sheet_name=worksheet_mapping.worksheet_name)

            result = ValidationResult(
                is_valid=True,
                processed_rows=len(df),
                valid_rows=0
            )

            # Validar cada fila
            for row_idx, row in df.iterrows():
                row_valid = True

                for mapping in worksheet_mapping.column_mappings:
                    if mapping.excel_column not in df.columns:
                        continue

                    value = row[mapping.excel_column]

                    # Saltar valores nulos si la columna es nullable
                    if pd.isna(value) and mapping.is_nullable:
                        continue

                    # Validar según tipo de datos
                    validation_error = self._validate_cell_value(
                        value, mapping.data_type, mapping.max_length
                    )

                    if validation_error:
                        row_valid = False
                        result.errors.append({
                            'row': row_idx + 2,  # +2 porque Excel empieza en 1 y hay header
                            'column': mapping.excel_column,
                            'value': str(value),
                            'error': validation_error,
                            'expected_type': mapping.data_type.value
                        })

                if row_valid:
                    result.valid_rows += 1

            # Determinar si la validación general es exitosa
            if result.errors:
                result.is_valid = False

                # Si hay demasiados errores, marcar como crítico
                error_rate = len(result.errors) / result.processed_rows
                if error_rate > 0.1:  # Más del 10% de errores
                    result.warnings.append({
                        'type': 'HIGH_ERROR_RATE',
                        'message': f'Alta tasa de errores: {error_rate:.1%}',
                        'suggestion': 'Revisar el mapeo de columnas y formato de datos'
                    })

            return result

        except Exception as e:
            self.logger.error(f"Error validando datos: {str(e)}")
            return ValidationResult(
                is_valid=False,
                errors=[{
                    'row': 0,
                    'column': 'GENERAL',
                    'value': '',
                    'error': f'Error de validación: {str(e)}',
                    'expected_type': 'N/A'
                }]
            )

    def _validate_cell_value(self, value: Any, expected_type: DataType,
                             max_length: Optional[int] = None) -> Optional[str]:
        """
        Valida un valor de celda individual.

        Args:
            value: Valor a validar
            expected_type: Tipo de datos esperado
            max_length: Longitud máxima (para strings)

        Returns:
            Mensaje de error o None si es válido
        """
        if pd.isna(value):
            return None  # Los nulos se manejan por separado

        try:
            if expected_type == DataType.INTEGER:
                if isinstance(value, (int, np.integer)):
                    return None
                if isinstance(value, (float, np.floating)) and value.is_integer():
                    return None
                try:
                    int(str(value).replace(',', ''))
                    return None
                except ValueError:
                    return f"No es un número entero válido"

            elif expected_type == DataType.DECIMAL:
                if isinstance(value, (int, float, np.number)):
                    return None
                try:
                    float(str(value).replace(',', ''))
                    return None
                except ValueError:
                    return f"No es un número decimal válido"

            elif expected_type == DataType.BOOLEAN:
                if isinstance(value, bool):
                    return None
                if str(value).lower().strip() in self.boolean_values:
                    return None
                return f"No es un valor booleano válido"

            elif expected_type in [DataType.DATE, DataType.DATETIME]:
                if isinstance(value, (datetime, date)):
                    return None
                try:
                    pd.to_datetime(value)
                    return None
                except:
                    return f"No es una fecha/hora válida"

            elif expected_type == DataType.STRING:
                str_value = str(value)
                if max_length and len(str_value) > max_length:
                    return f"Excede la longitud máxima de {max_length} caracteres"
                return None

            return None

        except Exception as e:
            return f"Error de validación: {str(e)}"

    def convert_dataframe_types(self, df: pd.DataFrame,
                                column_mappings: List[ColumnMapping]) -> pd.DataFrame:
        """
        Convierte los tipos de datos del DataFrame según los mapeos definidos.

        Args:
            df: DataFrame original
            column_mappings: Lista de mapeos de columnas

        Returns:
            DataFrame con tipos convertidos
        """
        df_converted = df.copy()

        for mapping in column_mappings:
            if mapping.excel_column not in df_converted.columns:
                continue

            try:
                if mapping.data_type == DataType.INTEGER:
                    # Convertir a entero, manejando valores nulos
                    df_converted[mapping.excel_column] = pd.to_numeric(
                        df_converted[mapping.excel_column], errors='coerce'
                    ).astype('Int64')  # Nullable integer

                elif mapping.data_type == DataType.DECIMAL:
                    df_converted[mapping.excel_column] = pd.to_numeric(
                        df_converted[mapping.excel_column], errors='coerce'
                    )

                elif mapping.data_type == DataType.BOOLEAN:
                    # Convertir valores booleanos
                    def convert_bool(x):
                        if pd.isna(x):
                            return None
                        str_val = str(x).lower().strip()
                        if str_val in ['true', 'yes', 'y', '1', 'verdadero', 'sí', 'si']:
                            return True
                        elif str_val in ['false', 'no', 'n', '0', 'falso']:
                            return False
                        return None

                    df_converted[mapping.excel_column] = df_converted[mapping.excel_column].apply(
                        convert_bool)

                elif mapping.data_type in [DataType.DATE, DataType.DATETIME]:
                    df_converted[mapping.excel_column] = pd.to_datetime(
                        df_converted[mapping.excel_column], errors='coerce'
                    )

                elif mapping.data_type == DataType.STRING:
                    df_converted[mapping.excel_column] = df_converted[mapping.excel_column].astype(
                        str)
                    # Reemplazar 'nan' string con None
                    df_converted[mapping.excel_column] = df_converted[mapping.excel_column].replace(
                        'nan', None)

            except Exception as e:
                self.logger.warning(
                    f"Error convirtiendo columna '{mapping.excel_column}': {str(e)}")

        return df_converted

    def process_excel_file(self, file_path: str,
                           available_tables: Dict[str, List[str]]) -> Dict[str, WorksheetMapping]:
        """
        Procesa un archivo Excel completo y genera mapeos para todas las hojas.

        Args:
            file_path: Ruta al archivo Excel
            available_tables: Diccionario {tabla: [columnas]}

        Returns:
            Diccionario con mapeos de todas las hojas
        """
        try:
            # Validar archivo
            is_valid, error_msg = self.validate_file(file_path)
            if not is_valid:
                raise ValueError(error_msg)

            # Obtener información de hojas
            worksheet_info = self.get_worksheet_info(file_path)

            # Procesar cada hoja
            worksheet_mappings = {}

            for worksheet_name, info in worksheet_info.items():
                if not info.get('has_data', False):
                    self.logger.warning(
                        f"Saltando hoja '{worksheet_name}' - sin datos")
                    continue

                try:
                    mapping = self.map_worksheet_to_table(
                        file_path, worksheet_name, available_tables
                    )
                    worksheet_mappings[worksheet_name] = mapping

                    self.logger.info(
                        f"Hoja '{worksheet_name}' mapeada a tabla '{mapping.table_name}' "
                        f"con confianza {mapping.confidence_score:.2%}"
                    )

                except Exception as e:
                    self.logger.error(
                        f"Error procesando hoja '{worksheet_name}': {str(e)}")

            return worksheet_mappings

        except Exception as e:
            self.logger.error(f"Error procesando archivo Excel: {str(e)}")
            raise
