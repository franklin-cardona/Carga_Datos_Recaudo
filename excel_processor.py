"""
Procesador Unificado de Archivos Excel
Incluye procesamiento avanzado, mapeo, validación y transformación de datos.

Autor: Franklin Cardona / Manus AI
Fecha: 2025-08-20
"""

import pandas as pd
import numpy as np
import logging
import re
from typing import Dict, List, Any, Optional, Tuple, Union
from pathlib import Path
from datetime import datetime, date
import warnings
from dataclasses import dataclass
from enum import Enum


# --- Modelos de datos ---
class DataType(Enum):
    STRING = "NVARCHAR"
    INTEGER = "INT"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BIT"
    DATE = "DATE"
    DATETIME = "DATETIME2"
    UNKNOWN = "NVARCHAR"


@dataclass
class ColumnMapping:
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


@dataclass
class ProcessingResult:
    success: bool
    processed_rows: int
    skipped_rows: int
    errors: List[str]
    warnings: List[str]
    data: Optional[pd.DataFrame]
    processing_time: float
    validation_results: Dict[str, Any]


# --- Procesador Unificado ---
class ExcelProcessor:
    def __init__(self, db_connection=None, fuzzy_threshold: float = 0.8):
        self.db_connection = db_connection
        self.fuzzy_threshold = fuzzy_threshold
        self.logger = logging.getLogger(__name__)
        self.max_file_size_mb = 1000
        self.max_rows = 100000
        self.chunk_size = 1000
        self.supported_extensions = ['.xlsx', '.xls', '.xlsm']
        self.validation_patterns = {
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'phone': re.compile(r'^[\+]?[1-9][\d]{0,15}$'),
            'numeric': re.compile(r'^-?\d*\.?\d+$'),
            'date': re.compile(r'^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$|^\d{2}-\d{2}-\d{4}$'),
        }
        self.type_inference_config = {
            'date_formats': ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y'],
            'boolean_values': {
                'true': ['true', '1', 'yes', 'si', 'verdadero', 'sí'],
                'false': ['false', '0', 'no', 'falso'],
            },
            'null_values': ['', 'null', 'none', 'n/a', 'na', '#n/a', 'nan'],
        }
        self.boolean_values = set(
            self.type_inference_config['boolean_values']['true'] + self.type_inference_config['boolean_values']['false'])

    # --- Validación de archivo ---
    def validate_file(self, file_path: str) -> Tuple[bool, Optional[str]]:
        try:
            if not Path(file_path).exists():
                return False, "El archivo no existe"
            ext = Path(file_path).suffix.lower()
            if ext not in self.supported_extensions:
                return False, f"Extensión no soportada. Extensiones válidas: {', '.join(self.supported_extensions)}"
            file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)
            if file_size_mb > self.max_file_size_mb:
                return False, f"El archivo excede el tamaño máximo de {self.max_file_size_mb} MB"
            return True, None
        except Exception as e:
            return False, f"Error validando archivo: {str(e)}"

    # --- Lectura de hojas y columnas ---
    def get_worksheet_info(self, file_path: str) -> Dict[str, Dict[str, Any]]:
        try:
            excel_file = pd.ExcelFile(file_path)
            worksheet_info = {}
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(
                        file_path, sheet_name=sheet_name, nrows=10)
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

    # --- Detección de tipos de columnas ---
    def detect_column_types(self, df: pd.DataFrame) -> Dict[str, DataType]:
        column_types = {}
        for column in df.columns:
            series = df[column].dropna()
            if len(series) == 0:
                column_types[column] = DataType.STRING
                continue
            string_series = series.astype(str)
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
                if value.lower() in self.boolean_values:
                    type_counts[DataType.BOOLEAN] += 1
                    continue
                if re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$', value) or re.match(r'^\d{4}[/-]\d{1,2}[/-]\d{1,2}$', value):
                    type_counts[DataType.DATE] += 1
                    continue
                if re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\s+\d{1,2}:\d{2}(:\d{2})?(\s*(AM|PM))?$', value):
                    type_counts[DataType.DATETIME] += 1
                    continue
                if re.match(r'^-?\d+$', value.replace(',', '')):
                    type_counts[DataType.INTEGER] += 1
                    continue
                if re.match(r'^-?\d+\.\d+$', value.replace(',', '')) or re.match(r'^-?\d{1,3}(,\d{3})*(\.\d+)?$', value):
                    type_counts[DataType.DECIMAL] += 1
                    continue
                type_counts[DataType.STRING] += 1
            total_values = sum(type_counts.values())
            if total_values == 0:
                column_types[column] = DataType.STRING
            else:
                percentages = {
                    dtype: count / total_values for dtype, count in type_counts.items()}
                max_type = max(percentages, key=lambda k: percentages[k])
                if percentages[max_type] >= 0.6:
                    column_types[column] = max_type
                else:
                    column_types[column] = DataType.STRING
        return column_types

    # --- Coincidencia difusa de columnas ---
    def fuzzy_match_columns(self, excel_columns: List[str], db_columns: List[str]) -> List[ColumnMapping]:
        mappings = []
        used_db_columns = set()
        for i, excel_col in enumerate(excel_columns):
            best_match = None
            best_score = 0
            for db_col in db_columns:
                if db_col in used_db_columns:
                    continue
                ratio_score = self._fuzzy_ratio(excel_col, db_col)
                if ratio_score > best_score:
                    best_score = ratio_score
                    best_match = db_col
            if best_match and best_score >= (self.fuzzy_threshold * 100):
                mapping = ColumnMapping(
                    excel_column=excel_col,
                    db_column=best_match,
                    excel_index=i,
                    data_type=DataType.UNKNOWN,
                    confidence_score=best_score / 100
                )
                mappings.append(mapping)
                used_db_columns.add(best_match)
            else:
                mapping = ColumnMapping(
                    excel_column=excel_col,
                    db_column=excel_col,
                    excel_index=i,
                    data_type=DataType.UNKNOWN,
                    confidence_score=0.0
                )
                mapping.validation_errors.append(
                    f"No se encontró coincidencia para la columna '{excel_col}'")
                mappings.append(mapping)
        return mappings

    def _fuzzy_ratio(self, a: str, b: str) -> float:
        a, b = a.lower(), b.lower()
        return (self._simple_ratio(a, b) + self._partial_ratio(a, b)) / 2

    def _simple_ratio(self, a, b):
        return 100 * (1 - (levenshtein(a, b) / max(len(a), len(b), 1)))

    def _partial_ratio(self, a, b):
        if a in b or b in a:
            return 100
        return self._simple_ratio(a, b)

    # --- Procesamiento principal ---
    def process_excel_file(self, file_path: str, sheet_name: Optional[str] = None, column_mappings: Optional[Dict[str, Dict[str, Any]]] = None, validation_rules: Optional[Dict[str, Any]] = None) -> ProcessingResult:
        start_time = datetime.now()
        try:
            self.logger.info(
                f"Iniciando procesamiento de archivo: {file_path}")
            is_valid, error_msg = self.validate_file(file_path)
            if not is_valid:
                return ProcessingResult(False, 0, 0, [error_msg if error_msg else "Error desconocido"], [], None, 0, {})
            df = self._read_excel_file(file_path, sheet_name)
            if df is None:
                return ProcessingResult(False, 0, 0, ["No se pudo leer el archivo Excel"], [], None, 0, {})
            original_rows = len(df)
            self.logger.info(
                f"Archivo leído: {original_rows} filas, {len(df.columns)} columnas")
            df_cleaned = self._clean_dataframe(df)
            if column_mappings:
                df_mapped = self._apply_column_mappings(
                    df_cleaned, column_mappings)
            else:
                df_mapped = df_cleaned
            validation_results = self._validate_dataframe(
                df_mapped, validation_rules)
            df_transformed = self._transform_data_types(
                df_mapped, column_mappings)
            df_valid = self._filter_valid_rows(
                df_transformed, validation_results)
            processed_rows = len(df_valid)
            skipped_rows = original_rows - processed_rows
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"Procesamiento completado: {processed_rows} filas procesadas, {skipped_rows} filas omitidas")
            return ProcessingResult(True, processed_rows, skipped_rows, [], validation_results.get("warnings", []), df_valid, processing_time, validation_results)
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Error procesando archivo Excel: {str(e)}"
            self.logger.error(error_msg)
            return ProcessingResult(False, 0, 0, [error_msg], [], None, processing_time, {})

    def _read_excel_file(self, file_path: str, sheet_name: Optional[str] = None) -> Optional[pd.DataFrame]:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                if sheet_name:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                else:
                    df = pd.read_excel(file_path)
                if len(df) > self.max_rows:
                    self.logger.warning(
                        f"El archivo tiene {len(df)} filas, se procesarán solo las primeras {self.max_rows}")
                    df = df.head(self.max_rows)
                return df
        except Exception as e:
            self.logger.error(f"Error leyendo archivo Excel: {str(e)}")
            return None

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df_clean = df.copy()
            df_clean = df_clean.dropna(axis=1, how="all")
            df_clean = df_clean.dropna(axis=0, how="all")
            df_clean.columns = [self._clean_column_name(
                col) for col in df_clean.columns]
            df_clean = df_clean.reset_index(drop=True)
            self.logger.info(
                f"DataFrame limpio: {len(df_clean)} filas, {len(df_clean.columns)} columnas")
            return df_clean
        except Exception as e:
            self.logger.error(f"Error limpiando DataFrame: {str(e)}")
            return df

    def _clean_column_name(self, column_name: str) -> str:
        try:
            clean_name = str(column_name).strip()
            clean_name = re.sub(r'[^\w\s-]', '', clean_name)
            clean_name = re.sub(r'\s+', ' ', clean_name)
            if not clean_name:
                clean_name = f"Column_{hash(column_name) % 1000}"
            return clean_name
        except Exception as e:
            self.logger.warning(
                f"Error limpiando nombre de columna {column_name}: {str(e)}")
            return str(column_name)

    def _apply_column_mappings(self, df: pd.DataFrame, column_mappings: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        try:
            df_mapped = df.copy()
            rename_dict = {}
            for excel_col, mapping_info in column_mappings.items():
                if mapping_info.get("sql_column") and mapping_info.get("confidence", 0) > 0.5:
                    if excel_col in df_mapped.columns:
                        rename_dict[excel_col] = mapping_info["sql_column"]
            if rename_dict:
                df_mapped = df_mapped.rename(columns=rename_dict)
                self.logger.info(f"Columnas renombradas: {len(rename_dict)}")
            return df_mapped
        except Exception as e:
            self.logger.error(f"Error aplicando mapeos de columnas: {str(e)}")
            return df

    def _validate_dataframe(self, df: pd.DataFrame, validation_rules: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            validation_results = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "column_validations": {},
                "row_validations": [],
            }
            for column in df.columns:
                column_validation = self._validate_column(
                    df[column], column, validation_rules)
                validation_results["column_validations"][column] = column_validation
                if not column_validation["is_valid"]:
                    validation_results["is_valid"] = False
                    validation_results["errors"].extend(
                        column_validation["errors"])
                validation_results["warnings"].extend(
                    column_validation["warnings"])
            duplicates = df.duplicated()
            if duplicates.any():
                duplicate_count = duplicates.sum()
                validation_results["warnings"].append(
                    f"Se encontraron {duplicate_count} filas duplicadas")
            return validation_results
        except Exception as e:
            self.logger.error(f"Error validando DataFrame: {str(e)}")
            return {
                "is_valid": False,
                "errors": [f"Error en validación: {str(e)}"],
                "warnings": [],
                "column_validations": {},
                "row_validations": [],
            }

    def _validate_column(self, series: pd.Series, column_name: str, validation_rules: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            validation_result = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "null_count": series.isnull().sum(),
                "unique_count": series.nunique(),
                "data_type": str(series.dtype),
            }
            null_percentage = (
                validation_result["null_count"] / len(series)) * 100
            if null_percentage > 50:
                validation_result["warnings"].append(
                    f"Columna '{column_name}' tiene {null_percentage:.1f}% de valores nulos")
            elif null_percentage > 80:
                validation_result["is_valid"] = False
                validation_result["errors"].append(
                    f"Columna '{column_name}' tiene demasiados valores nulos ({null_percentage:.1f}%)")
            inferred_type = self._infer_column_type(series)
            validation_result["inferred_type"] = inferred_type
            if inferred_type == "email":
                email_validation = self._validate_email_column(series)
                validation_result.update(email_validation)
            elif inferred_type == "phone":
                phone_validation = self._validate_phone_column(series)
                validation_result.update(phone_validation)
            elif inferred_type == "numeric":
                numeric_validation = self._validate_numeric_column(series)
                validation_result.update(numeric_validation)
            elif inferred_type == "date":
                date_validation = self._validate_date_column(series)
                validation_result.update(date_validation)
            if validation_rules and column_name in validation_rules:
                custom_validation = self._apply_custom_validation(
                    series, validation_rules[column_name])
                validation_result.update(custom_validation)
            return validation_result
        except Exception as e:
            return {
                "is_valid": False,
                "errors": [f"Error validando columna {column_name}: {str(e)}"],
                "warnings": [],
                "null_count": 0,
                "unique_count": 0,
                "data_type": "unknown",
                "inferred_type": "unknown",
            }

    def _infer_column_type(self, series: pd.Series) -> str:
        try:
            non_null_sample = series.dropna().astype(str).head(100)
            if len(non_null_sample) == 0:
                return "unknown"
            type_counts = {"email": 0, "phone": 0,
                           "numeric": 0, "date": 0, "boolean": 0}
            for value in non_null_sample:
                value_str = str(value).strip().lower()
                if self.validation_patterns["email"].match(value_str):
                    type_counts["email"] += 1
                elif self.validation_patterns["phone"].match(value_str):
                    type_counts["phone"] += 1
                elif self.validation_patterns["numeric"].match(value_str):
                    type_counts["numeric"] += 1
                elif self.validation_patterns["date"].match(value_str):
                    type_counts["date"] += 1
                elif value_str in self.type_inference_config["boolean_values"]["true"] + self.type_inference_config["boolean_values"]["false"]:
                    type_counts["boolean"] += 1
            total_sample = len(non_null_sample)
            for data_type, count in type_counts.items():
                if count / total_sample >= 0.7:
                    return data_type
            return "string"
        except Exception as e:
            self.logger.warning(f"Error infiriendo tipo de columna: {str(e)}")
            return "string"

    def _validate_email_column(self, series: pd.Series) -> Dict[str, Any]:
        try:
            result = {"email_validation": {"valid_emails": 0,
                                           "invalid_emails": 0}, "warnings": []}
            for value in series.dropna():
                if self.validation_patterns["email"].match(str(value).strip()):
                    result["email_validation"]["valid_emails"] += 1
                else:
                    result["email_validation"]["invalid_emails"] += 1
            invalid_rate = result["email_validation"]["invalid_emails"] / \
                max(len(series.dropna()), 1)
            if invalid_rate > 0.1:
                result["warnings"].append(
                    f"La columna tiene {invalid_rate:.1%} de emails inválidos")
            return result
        except Exception as e:
            return {"email_validation": {"error": str(e)}}

    def _validate_phone_column(self, series: pd.Series) -> Dict[str, Any]:
        try:
            result = {"phone_validation": {
                "valid_phones": 0, "invalid_phones": 0}}
            for value in series.dropna():
                clean_phone = re.sub(r'[^\d+]', '', str(value))
                if self.validation_patterns["phone"].match(clean_phone):
                    result["phone_validation"]["valid_phones"] += 1
                else:
                    result["phone_validation"]["invalid_phones"] += 1
            return result
        except Exception as e:
            return {"phone_validation": {"error": str(e)}}

    def _validate_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        try:
            result = {"numeric_validation": {}, "warnings": []}
            numeric_series = pd.to_numeric(series, errors="coerce")
            result["numeric_validation"]["min_value"] = float(
                numeric_series.min())
            result["numeric_validation"]["max_value"] = float(
                numeric_series.max())
            result["numeric_validation"]["mean_value"] = float(
                numeric_series.mean())
            result["numeric_validation"]["std_value"] = float(
                numeric_series.std())
            q1 = numeric_series.quantile(0.25)
            q3 = numeric_series.quantile(0.75)
            iqr = q3 - q1
            outliers = numeric_series[(
                numeric_series < q1 - 1.5 * iqr) | (numeric_series > q3 + 1.5 * iqr)]
            if len(outliers) > 0:
                result["warnings"].append(
                    f"Se detectaron {len(outliers)} valores atípicos")
            return result
        except Exception as e:
            return {"numeric_validation": {"error": str(e)}}

    def _validate_date_column(self, series: pd.Series) -> Dict[str, Any]:
        try:
            result = {"date_validation": {"valid_dates": 0,
                                          "invalid_dates": 0}, "warnings": []}
            for value in series.dropna():
                try:
                    pd.to_datetime(str(value))
                    result["date_validation"]["valid_dates"] += 1
                except:
                    result["date_validation"]["invalid_dates"] += 1
            return result
        except Exception as e:
            return {"date_validation": {"error": str(e)}}

    def _apply_custom_validation(self, series: pd.Series, rules: Dict[str, Any]) -> Dict[str, Any]:
        try:
            result = {"custom_validation": {}}
            return result
        except Exception as e:
            return {"custom_validation": {"error": str(e)}}

    def _transform_data_types(self, df: pd.DataFrame, column_mappings: Optional[Dict[str, Dict[str, Any]]] = None) -> pd.DataFrame:
        try:
            df_transformed = df.copy()
            for column in df_transformed.columns:
                try:
                    target_type = None
                    if column_mappings:
                        for excel_col, mapping_info in column_mappings.items():
                            if mapping_info.get("sql_column") == column:
                                target_type = mapping_info.get("data_type")
                                break
                    if target_type == "int" or target_type == "integer":
                        df_transformed[column] = pd.to_numeric(
                            df_transformed[column], errors="coerce").astype("Int64")
                    elif target_type == "float":
                        df_transformed[column] = pd.to_numeric(
                            df_transformed[column], errors="coerce")
                    elif target_type == "datetime":
                        df_transformed[column] = pd.to_datetime(
                            df_transformed[column], errors="coerce")
                    elif target_type == "boolean":
                        df_transformed[column] = self._convert_to_boolean(
                            df_transformed[column])
                    else:
                        df_transformed[column] = df_transformed[column].astype(
                            str).str.strip()
                        df_transformed[column] = df_transformed[column].replace(
                            "nan", np.nan)
                except Exception as col_error:
                    self.logger.warning(
                        f"Error transformando columna {column}: {str(col_error)}")
            return df_transformed
        except Exception as e:
            self.logger.error(f"Error transformando tipos de datos: {str(e)}")
            return df

    def _convert_to_boolean(self, series: pd.Series) -> pd.Series:
        try:
            def convert_value(value):
                if pd.isna(value):
                    return None
                str_value = str(value).lower().strip()
                if str_value in self.type_inference_config["boolean_values"]["true"]:
                    return True
                elif str_value in self.type_inference_config["boolean_values"]["false"]:
                    return False
                else:
                    return None
            return series.apply(convert_value)
        except Exception as e:
            self.logger.warning(f"Error convirtiendo a booleano: {str(e)}")
            return series

    def _filter_valid_rows(self, df: pd.DataFrame, validation_results: Dict[str, Any]) -> pd.DataFrame:
        try:
            df_valid = df.dropna(how="all")
            return df_valid
        except Exception as e:
            self.logger.error(f"Error filtrando filas válidas: {str(e)}")
            return df

# --- Utilidad para coincidencia difusa ---


def levenshtein(a, b):
    if a == b:
        return 0
    if len(a) == 0:
        return len(b)
    if len(b) == 0:
        return len(a)
    v0 = [i for i in range(len(b) + 1)]
    v1 = [0] * (len(b) + 1)
    for i in range(len(a)):
        v1[0] = i + 1
        for j in range(len(b)):
            cost = 0 if a[i] == b[j] else 1
            v1[j + 1] = min(v1[j] + 1, v0[j + 1] + 1, v0[j] + cost)
        v0, v1 = v1, v0
    return v0[len(b)]
