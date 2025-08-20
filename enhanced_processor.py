"""
Procesador Mejorado de Excel
Proporciona funcionalidad avanzada para procesar archivos Excel con validación y transformación de datos.

Autor: Manus AI
Fecha: 2025-01-08
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Union
from pathlib import Path
import re
from datetime import datetime, date
import warnings
from dataclasses import dataclass


@dataclass
class ProcessingResult:
    """
    Resultado del procesamiento de un archivo Excel.
    """
    success: bool
    processed_rows: int
    skipped_rows: int
    errors: List[str]
    warnings: List[str]
    data: Optional[pd.DataFrame]
    processing_time: float
    validation_results: Dict[str, Any]


class EnhancedExcelProcessor:
    """
    Procesador avanzado de archivos Excel con validación y transformación de datos.
    """

    def __init__(self, db_connection):
        """
        Inicializa el procesador de Excel.

        Args:
            db_connection: Conexión a la base de datos
        """
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)

        # Configuración de procesamiento
        self.max_file_size_mb = 1000  # Tamaño máximo de archivo en MB
        self.max_rows = 100000  # Número máximo de filas a procesar
        self.chunk_size = 1000  # Tamaño de chunk para procesamiento por lotes

        # Patrones de validación
        self.validation_patterns = {
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'phone': re.compile(r'^[\+]?[1-9][\d]{0,15}$'),
            'numeric': re.compile(r'^-?\d*\.?\d+$'),
            'date': re.compile(r'^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$|^\d{2}-\d{2}-\d{4}$'),
        }

        # Configuración de tipos de datos
        self.type_inference_config = {
            'date_formats': ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y'],
            'boolean_values': {
                'true': ['true', '1', 'yes', 'si', 'verdadero', 'sí'],
                'false': ['false', '0', 'no', 'falso'],
            },
            'null_values': ['', 'null', 'none', 'n/a', 'na', '#n/a', 'nan'],
        }

    def process_excel_file(
        self,
        file_path: str,
        sheet_name: str = None,
        column_mappings: Dict[str, Dict[str, Any]] = None,
        validation_rules: Dict[str, Any] = None,
    ) -> ProcessingResult:
        """
        Procesa un archivo Excel completo con validación y transformación.

        Args:
            file_path: Ruta del archivo Excel
            sheet_name: Nombre de la hoja a procesar (None para la primera)
            column_mappings: Mapeos de columnas Excel a SQL
            validation_rules: Reglas de validación personalizadas

        Returns:
            Resultado del procesamiento
        """
        start_time = datetime.now()

        try:
            self.logger.info(
                f"Iniciando procesamiento de archivo: {file_path}")

            # Validar archivo
            file_validation = self._validate_file(file_path)
            if not file_validation["is_valid"]:
                return ProcessingResult(
                    success=False,
                    processed_rows=0,
                    skipped_rows=0,
                    errors=file_validation["errors"],
                    warnings=[],
                    data=None,
                    processing_time=0,
                    validation_results=file_validation,
                )

            # Leer archivo Excel
            df = self._read_excel_file(file_path, sheet_name)
            if df is None:
                return ProcessingResult(
                    success=False,
                    processed_rows=0,
                    skipped_rows=0,
                    errors=["No se pudo leer el archivo Excel"],
                    warnings=[],
                    data=None,
                    processing_time=0,
                    validation_results={},
                )

            original_rows = len(df)
            self.logger.info(
                f"Archivo leído: {original_rows} filas, {len(df.columns)} columnas")

            # Limpiar y preparar datos
            df_cleaned = self._clean_dataframe(df)

            # Aplicar mapeos de columnas si se proporcionan
            if column_mappings:
                df_mapped = self._apply_column_mappings(
                    df_cleaned, column_mappings)
            else:
                df_mapped = df_cleaned

            # Validar datos
            validation_results = self._validate_dataframe(
                df_mapped, validation_rules)

            # Transformar tipos de datos
            df_transformed = self._transform_data_types(
                df_mapped, column_mappings)

            # Filtrar filas válidas
            df_valid = self._filter_valid_rows(
                df_transformed, validation_results)

            processed_rows = len(df_valid)
            skipped_rows = original_rows - processed_rows

            processing_time = (datetime.now() - start_time).total_seconds()

            self.logger.info(
                f"Procesamiento completado: {processed_rows} filas procesadas, {skipped_rows} filas omitidas"
            )

            return ProcessingResult(
                success=True,
                processed_rows=processed_rows,
                skipped_rows=skipped_rows,
                errors=[],
                warnings=validation_results.get("warnings", []),
                data=df_valid,
                processing_time=processing_time,
                validation_results=validation_results,
            )

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Error procesando archivo Excel: {str(e)}"
            self.logger.error(error_msg)

            return ProcessingResult(
                success=False,
                processed_rows=0,
                skipped_rows=0,
                errors=[error_msg],
                warnings=[],
                data=None,
                processing_time=processing_time,
                validation_results={},
            )

    def _validate_file(self, file_path: str) -> Dict[str, Any]:
        """
        Valida el archivo Excel antes del procesamiento.

        Args:
            file_path: Ruta del archivo

        Returns:
            Diccionario con resultado de validación
        """
        try:
            validation_result = {"is_valid": True,
                                 "errors": [], "warnings": []}

            file_path_obj = Path(file_path)

            # Verificar existencia
            if not file_path_obj.exists():
                validation_result["is_valid"] = False
                validation_result["errors"].append("El archivo no existe")
                return validation_result

            # Verificar extensión
            if file_path_obj.suffix.lower() not in [".xlsx", ".xls"]:
                validation_result["is_valid"] = False
                validation_result["errors"].append(
                    "El archivo debe ser un archivo Excel (.xlsx o .xls)"
                )
                return validation_result

            # Verificar tamaño
            file_size_mb = file_path_obj.stat().st_size / (1024 * 1024)
            if file_size_mb > self.max_file_size_mb:
                validation_result["is_valid"] = False
                validation_result["errors"].append(
                    f"El archivo excede el tamaño máximo de {self.max_file_size_mb} MB"
                )
                return validation_result
            elif file_size_mb > self.max_file_size_mb * 0.8:
                validation_result["warnings"].append(
                    f"El archivo es grande ({file_size_mb:.1f} MB), el procesamiento puede ser lento"
                )

            return validation_result

        except Exception as e:
            return {
                "is_valid": False,
                "errors": [f"Error validando archivo: {str(e)}"],
                "warnings": [],
            }

    def _read_excel_file(self, file_path: str, sheet_name: str = None) -> Optional[pd.DataFrame]:
        """
        Lee un archivo Excel con manejo robusto de errores.

        Args:
            file_path: Ruta del archivo
            sheet_name: Nombre de la hoja

        Returns:
            DataFrame de pandas o None si hay error
        """
        try:
            # Suprimir warnings de pandas
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                # Leer archivo
                if sheet_name:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                else:
                    df = pd.read_excel(file_path)

                # Verificar límite de filas
                if len(df) > self.max_rows:
                    self.logger.warning(
                        f"El archivo tiene {len(df)} filas, se procesarán solo las primeras {self.max_rows}"
                    )
                    df = df.head(self.max_rows)

                return df

        except Exception as e:
            self.logger.error(f"Error leyendo archivo Excel: {str(e)}")
            return None

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia el DataFrame eliminando filas y columnas vacías.

        Args:
            df: DataFrame original

        Returns:
            DataFrame limpio
        """
        try:
            # Crear copia para no modificar el original
            df_clean = df.copy()

            # Eliminar columnas completamente vacías
            df_clean = df_clean.dropna(axis=1, how="all")

            # Eliminar filas completamente vacías
            df_clean = df_clean.dropna(axis=0, how="all")

            # Limpiar nombres de columnas
            df_clean.columns = [self._clean_column_name(
                col) for col in df_clean.columns]

            # Resetear índice
            df_clean = df_clean.reset_index(drop=True)

            self.logger.info(
                f"DataFrame limpio: {len(df_clean)} filas, {len(df_clean.columns)} columnas")

            return df_clean

        except Exception as e:
            self.logger.error(f"Error limpiando DataFrame: {str(e)}")
            return df

    def _clean_column_name(self, column_name: str) -> str:
        """
        Limpia el nombre de una columna.

        Args:
            column_name: Nombre original

        Returns:
            Nombre limpio
        """
        try:
            # Convertir a string si no lo es
            clean_name = str(column_name).strip()

            # Remover caracteres especiales problemáticos
            clean_name = re.sub(r'[^\w\s-]', '', clean_name)

            # Reemplazar espacios múltiples con uno solo
            clean_name = re.sub(r'\s+', ' ', clean_name)

            # Si el nombre está vacío, generar uno genérico
            if not clean_name:
                clean_name = f"Column_{hash(column_name) % 1000}"

            return clean_name

        except Exception as e:
            self.logger.warning(
                f"Error limpiando nombre de columna {column_name}: {str(e)}")
            return str(column_name)

    def _apply_column_mappings(
        self, df: pd.DataFrame, column_mappings: Dict[str, Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Aplica los mapeos de columnas al DataFrame.

        Args:
            df: DataFrame original
            column_mappings: Mapeos de columnas

        Returns:
            DataFrame con columnas mapeadas
        """
        try:
            df_mapped = df.copy()

            # Crear diccionario de renombrado
            rename_dict = {}
            for excel_col, mapping_info in column_mappings.items():
                if mapping_info.get("sql_column") and mapping_info.get("confidence", 0) > 0.5:
                    if excel_col in df_mapped.columns:
                        rename_dict[excel_col] = mapping_info["sql_column"]

            # Renombrar columnas
            if rename_dict:
                df_mapped = df_mapped.rename(columns=rename_dict)
                self.logger.info(f"Columnas renombradas: {len(rename_dict)}")

            return df_mapped

        except Exception as e:
            self.logger.error(f"Error aplicando mapeos de columnas: {str(e)}")
            return df

    def _validate_dataframe(
        self, df: pd.DataFrame, validation_rules: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Valida los datos del DataFrame.

        Args:
            df: DataFrame a validar
            validation_rules: Reglas de validación personalizadas

        Returns:
            Diccionario con resultados de validación
        """
        try:
            validation_results = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "column_validations": {},
                "row_validations": [],
            }

            # Validar cada columna
            for column in df.columns:
                column_validation = self._validate_column(
                    df[column], column, validation_rules
                )
                validation_results["column_validations"][column] = column_validation

                if not column_validation["is_valid"]:
                    validation_results["is_valid"] = False
                    validation_results["errors"].extend(
                        column_validation["errors"])

                validation_results["warnings"].extend(
                    column_validation["warnings"])

            # Validar filas duplicadas
            duplicates = df.duplicated()
            if duplicates.any():
                duplicate_count = duplicates.sum()
                validation_results["warnings"].append(
                    f"Se encontraron {duplicate_count} filas duplicadas"
                )

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

    def _validate_column(
        self, series: pd.Series, column_name: str, validation_rules: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Valida una columna específica.

        Args:
            series: Serie de pandas a validar
            column_name: Nombre de la columna
            validation_rules: Reglas de validación

        Returns:
            Diccionario con resultado de validación
        """
        try:
            validation_result = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "null_count": series.isnull().sum(),
                "unique_count": series.nunique(),
                "data_type": str(series.dtype),
            }

            # Calcular porcentaje de nulos
            null_percentage = (
                validation_result["null_count"] / len(series)) * 100

            if null_percentage > 50:
                validation_result["warnings"].append(
                    f"Columna '{column_name}' tiene {null_percentage:.1f}% de valores nulos"
                )
            elif null_percentage > 80:
                validation_result["is_valid"] = False
                validation_result["errors"].append(
                    f"Columna '{column_name}' tiene demasiados valores nulos ({null_percentage:.1f}%)"
                )

            # Validaciones específicas por tipo de dato inferido
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

            # Aplicar reglas de validación personalizadas
            if validation_rules and column_name in validation_rules:
                custom_validation = self._apply_custom_validation(
                    series, validation_rules[column_name]
                )
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
        """
        Infiere el tipo de datos de una columna.

        Args:
            series: Serie de pandas

        Returns:
            Tipo de dato inferido
        """
        try:
            # Obtener muestra de datos no nulos
            non_null_sample = series.dropna().astype(str).head(100)

            if len(non_null_sample) == 0:
                return "unknown"

            # Contadores para cada tipo
            type_counts = {
                "email": 0,
                "phone": 0,
                "numeric": 0,
                "date": 0,
                "boolean": 0,
            }

            for value in non_null_sample:
                value_str = str(value).strip().lower()

                # Verificar email
                if self.validation_patterns["email"].match(value_str):
                    type_counts["email"] += 1
                # Verificar teléfono
                elif self.validation_patterns["phone"].match(value_str):
                    type_counts["phone"] += 1
                # Verificar numérico
                elif self.validation_patterns["numeric"].match(value_str):
                    type_counts["numeric"] += 1
                # Verificar fecha
                elif self.validation_patterns["date"].match(value_str):
                    type_counts["date"] += 1
                # Verificar booleano
                elif (
                    value_str
                    in self.type_inference_config["boolean_values"]["true"]
                    + self.type_inference_config["boolean_values"]["false"]
                ):
                    type_counts["boolean"] += 1

            # Determinar tipo predominante
            total_sample = len(non_null_sample)
            for data_type, count in type_counts.items():
                if count / total_sample >= 0.7:  # 70% de coincidencia
                    return data_type

            return "string"  # Tipo por defecto

        except Exception as e:
            self.logger.warning(f"Error infiriendo tipo de columna: {str(e)}")
            return "string"

    def _validate_email_column(self, series: pd.Series) -> Dict[str, Any]:
        """Valida una columna de emails."""
        try:
            result = {"email_validation": {
                "valid_emails": 0, "invalid_emails": 0}}

            for value in series.dropna():
                if self.validation_patterns["email"].match(str(value).strip()):
                    result["email_validation"]["valid_emails"] += 1
                else:
                    result["email_validation"]["invalid_emails"] += 1

            invalid_rate = result["email_validation"]["invalid_emails"] / \
                len(series.dropna())
            if invalid_rate > 0.1:  # Más del 10% inválidos
                result["warnings"] = result.get("warnings", [])
                result["warnings"].append(
                    f"La columna tiene {invalid_rate:.1%} de emails inválidos")

            return result

        except Exception as e:
            return {"email_validation": {"error": str(e)}}

    def _validate_phone_column(self, series: pd.Series) -> Dict[str, Any]:
        """Valida una columna de teléfonos."""
        try:
            result = {"phone_validation": {
                "valid_phones": 0, "invalid_phones": 0}}

            for value in series.dropna():
                # Limpiar número de teléfono
                clean_phone = re.sub(r'[^\d+]', '', str(value))
                if self.validation_patterns["phone"].match(clean_phone):
                    result["phone_validation"]["valid_phones"] += 1
                else:
                    result["phone_validation"]["invalid_phones"] += 1

            return result

        except Exception as e:
            return {"phone_validation": {"error": str(e)}}

    def _validate_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Valida una columna numérica."""
        try:
            result = {"numeric_validation": {}}

            numeric_series = pd.to_numeric(series, errors="coerce")
            result["numeric_validation"]["min_value"] = float(
                numeric_series.min())
            result["numeric_validation"]["max_value"] = float(
                numeric_series.max())
            result["numeric_validation"]["mean_value"] = float(
                numeric_series.mean())
            result["numeric_validation"]["std_value"] = float(
                numeric_series.std())

            # Detectar outliers
            q1 = numeric_series.quantile(0.25)
            q3 = numeric_series.quantile(0.75)
            iqr = q3 - q1
            outliers = numeric_series[(
                numeric_series < q1 - 1.5 * iqr) | (numeric_series > q3 + 1.5 * iqr)]

            if len(outliers) > 0:
                result["warnings"] = result.get("warnings", [])
                result["warnings"].append(
                    f"Se detectaron {len(outliers)} valores atípicos")

            return result

        except Exception as e:
            return {"numeric_validation": {"error": str(e)}}

    def _validate_date_column(self, series: pd.Series) -> Dict[str, Any]:
        """Valida una columna de fechas."""
        try:
            result = {"date_validation": {
                "valid_dates": 0, "invalid_dates": 0}}

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
        """
Aplica reglas de validación personalizadas.
        """
        try:
            result = {"custom_validation": {}}

            # Implementar reglas personalizadas según sea necesario
            # Por ahora, estructura básica

            return result

        except Exception as e:
            return {"custom_validation": {"error": str(e)}}

    def _transform_data_types(
        self, df: pd.DataFrame, column_mappings: Dict[str, Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Transforma los tipos de datos del DataFrame.

        Args:
            df: DataFrame original
            column_mappings: Mapeos con información de tipos SQL

        Returns:
            DataFrame con tipos transformados
        """
        try:
            df_transformed = df.copy()

            for column in df_transformed.columns:
                try:
                    # Obtener tipo objetivo si existe mapeo
                    target_type = None
                    if column_mappings:
                        for excel_col, mapping_info in column_mappings.items():
                            if mapping_info.get("sql_column") == column:
                                target_type = mapping_info.get("data_type")
                                break

                    # Transformar según tipo objetivo o inferido
                    if target_type == "int" or target_type == "integer":
                        df_transformed[column] = pd.to_numeric(
                            df_transformed[column], errors="coerce"
                        ).astype("Int64")
                    elif target_type == "float":
                        df_transformed[column] = pd.to_numeric(
                            df_transformed[column], errors="coerce"
                        )
                    elif target_type == "datetime":
                        df_transformed[column] = pd.to_datetime(
                            df_transformed[column], errors="coerce"
                        )
                    elif target_type == "boolean":
                        df_transformed[column] = self._convert_to_boolean(
                            df_transformed[column]
                        )
                    else:
                        # Mantener como string pero limpiar
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
        """
        Convierte una serie a booleanos.
        """
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

    def _filter_valid_rows(
        self, df: pd.DataFrame, validation_results: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Filtra las filas válidas del DataFrame.

        Args:
            df: DataFrame original
            validation_results: Resultados de validación

        Returns:
            DataFrame con solo filas válidas
        """
        try:
            # Por ahora, filtrar solo filas completamente vacías
            # En el futuro, se pueden aplicar filtros más sofisticados
            df_valid = df.dropna(how="all")

            return df_valid

        except Exception as e:
            self.logger.error(f"Error filtrando filas válidas: {str(e)}")
            return df
