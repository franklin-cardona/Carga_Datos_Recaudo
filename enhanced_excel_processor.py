"""
Procesador Mejorado de Excel con Filtrado de Duplicados
Extiende el ExcelProcessor original con capacidades de filtrado de duplicados.

Autor: Manus AI
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
from dataclasses import dataclass, field
from enum import Enum

# Importar clases base
try:
    from excel_processor import ExcelProcessor, ProcessingResult, DataType, ColumnMapping
    from duplicate_filter import DuplicateFilter, DuplicateFilterResult
    from metadata_utils import DatabaseMetadataUtils
except ImportError as e:
    logging.error(f"Error de importación en EnhancedExcelProcessor: {e}")
    raise


@dataclass
class EnhancedProcessingResult:
    """
    Resultado extendido del procesamiento con información de duplicados.
    """
    success: bool
    original_rows: int
    processed_rows: int
    skipped_rows: int
    duplicate_rows: int
    new_rows: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    data: Optional[pd.DataFrame] = None
    processing_time: float = 0.0
    validation_results: Dict[str, Any] = field(default_factory=dict)
    duplicate_filter_results: Optional[DuplicateFilterResult] = None
    identifier_info: Dict[str, Any] = field(default_factory=dict)


class EnhancedExcelProcessor(ExcelProcessor):
    """
    Procesador mejorado de Excel con capacidades de filtrado de duplicados.
    """

    def __init__(self, db_connection=None, fuzzy_threshold: float = 0.8):
        """
        Inicializa el procesador mejorado.

        Args:
            db_connection: Conexión a la base de datos
            fuzzy_threshold: Umbral para coincidencia difusa
        """
        super().__init__(db_connection, fuzzy_threshold)

        # Inicializar utilidades adicionales
        if self.db_connection:
            self.metadata_utils = DatabaseMetadataUtils(self.db_connection)
            self.duplicate_filter = DuplicateFilter(
                self.db_connection, self.metadata_utils)
        else:
            self.metadata_utils = None
            self.duplicate_filter = None

        # Configuración específica para filtrado de duplicados
        self.enable_duplicate_filtering = True
        self.remove_internal_duplicates = True
        self.internal_duplicate_strategy = 'first'  # 'first', 'last', False

    def process_excel_file_enhanced(self, file_path: str, sheet_name: Optional[str] = None,
                                    column_mappings: Optional[Dict[str,
                                                                   Dict[str, Any]]] = None,
                                    validation_rules: Optional[Dict[str, Any]] = None,
                                    target_schema: Optional[str] = None,
                                    target_table: Optional[str] = None,
                                    filter_duplicates: bool = True) -> EnhancedProcessingResult:
        """
        Procesa un archivo Excel con capacidades mejoradas incluyendo filtrado de duplicados.

        Args:
            file_path: Ruta del archivo Excel
            sheet_name: Nombre de la hoja (opcional)
            column_mappings: Mapeos de columnas
            validation_rules: Reglas de validación
            target_schema: Esquema de la tabla destino
            target_table: Nombre de la tabla destino
            filter_duplicates: Si filtrar duplicados contra la base de datos

        Returns:
            Resultado mejorado del procesamiento
        """
        start_time = datetime.now()

        try:
            self.logger.info(
                f"Iniciando procesamiento mejorado de archivo: {file_path}")

            # Validación inicial
            if filter_duplicates and (not target_schema or not target_table):
                return EnhancedProcessingResult(
                    success=False,
                    original_rows=0,
                    processed_rows=0,
                    skipped_rows=0,
                    duplicate_rows=0,
                    new_rows=0,
                    errors=[
                        "Para filtrar duplicados se requiere especificar target_schema y target_table"],
                    processing_time=(datetime.now() -
                                     start_time).total_seconds()
                )

            if filter_duplicates and not self.duplicate_filter:
                return EnhancedProcessingResult(
                    success=False,
                    original_rows=0,
                    processed_rows=0,
                    skipped_rows=0,
                    duplicate_rows=0,
                    new_rows=0,
                    errors=[
                        "Filtro de duplicados no disponible - conexión a base de datos requerida"],
                    processing_time=(datetime.now() -
                                     start_time).total_seconds()
                )

            # Procesamiento base usando el método padre
            base_result = self.process_excel_file(
                file_path=file_path,
                sheet_name=sheet_name,
                column_mappings=column_mappings,
                validation_rules=validation_rules
            )

            if not base_result.success or base_result.data is None:
                # Convertir resultado base a resultado mejorado
                return EnhancedProcessingResult(
                    success=base_result.success,
                    original_rows=base_result.processed_rows + base_result.skipped_rows,
                    processed_rows=base_result.processed_rows,
                    skipped_rows=base_result.skipped_rows,
                    duplicate_rows=0,
                    new_rows=base_result.processed_rows,
                    errors=base_result.errors,
                    warnings=base_result.warnings,
                    data=base_result.data,
                    processing_time=base_result.processing_time,
                    validation_results=base_result.validation_results
                )

            processed_data = base_result.data
            original_count = len(processed_data)

            self.logger.info(
                f"Procesamiento base completado: {original_count} registros")

            # Filtrado de duplicados internos (opcional)
            internal_duplicate_stats = {}
            if self.remove_internal_duplicates and filter_duplicates:
                processed_data, internal_duplicate_stats = self._remove_internal_duplicates(
                    processed_data, target_schema, target_table
                )
                self.logger.info(
                    f"Duplicados internos removidos: {internal_duplicate_stats.get('removed_count', 0)}")

            # Filtrado de duplicados contra base de datos
            duplicate_filter_result = None
            if filter_duplicates:
                duplicate_filter_result = self.duplicate_filter.filter_duplicates(
                    target_schema, target_table, processed_data
                )

                if duplicate_filter_result.success:
                    processed_data = duplicate_filter_result.filtered_data
                    self.logger.info(
                        f"Filtrado de duplicados completado: {duplicate_filter_result.new_records_count}/{duplicate_filter_result.original_count} registros son nuevos")
                else:
                    self.logger.warning(
                        f"Error en filtrado de duplicados: {duplicate_filter_result.errors}")

            # Calcular estadísticas finales
            final_count = len(
                processed_data) if processed_data is not None else 0
            duplicate_count = duplicate_filter_result.duplicate_count if duplicate_filter_result else 0

            processing_time = (datetime.now() - start_time).total_seconds()

            # Preparar resultado mejorado
            result = EnhancedProcessingResult(
                success=True,
                original_rows=original_count,
                processed_rows=final_count,
                skipped_rows=base_result.skipped_rows,
                duplicate_rows=duplicate_count,
                new_rows=final_count,
                errors=base_result.errors,
                warnings=base_result.warnings,
                data=processed_data,
                processing_time=processing_time,
                validation_results=base_result.validation_results,
                duplicate_filter_results=duplicate_filter_result,
                identifier_info=duplicate_filter_result.identifier_info if duplicate_filter_result else {}
            )

            # Agregar advertencias del filtrado de duplicados
            if duplicate_filter_result and duplicate_filter_result.warnings:
                result.warnings.extend(duplicate_filter_result.warnings)

            # Agregar errores del filtrado de duplicados
            if duplicate_filter_result and duplicate_filter_result.errors:
                result.errors.extend(duplicate_filter_result.errors)
                if duplicate_filter_result.errors:
                    result.success = False

            # Agregar información de duplicados internos
            if internal_duplicate_stats:
                if 'removed_count' in internal_duplicate_stats and internal_duplicate_stats['removed_count'] > 0:
                    result.warnings.append(
                        f"Se removieron {internal_duplicate_stats['removed_count']} duplicados internos")

            self.logger.info(
                f"Procesamiento mejorado completado: {result.new_rows}/{result.original_rows} registros finales")

            return result

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Error en procesamiento mejorado: {str(e)}"
            self.logger.error(error_msg)

            return EnhancedProcessingResult(
                success=False,
                original_rows=0,
                processed_rows=0,
                skipped_rows=0,
                duplicate_rows=0,
                new_rows=0,
                errors=[error_msg],
                processing_time=processing_time
            )

    def _remove_internal_duplicates(self, data: pd.DataFrame, schema_name: str,
                                    table_name: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Remueve duplicados internos del DataFrame.

        Args:
            data: DataFrame con los datos
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla

        Returns:
            Tupla con (DataFrame sin duplicados internos, estadísticas)
        """
        try:
            if not self.metadata_utils:
                return data, {'error': 'Utilidades de metadatos no disponibles'}

            # Obtener identificador único de la tabla
            best_identifier = self.metadata_utils.get_best_unique_identifier(
                schema_name, table_name)

            if not best_identifier:
                return data, {'warning': 'No se encontró identificador único para remover duplicados internos'}

            key_columns = best_identifier['columns']

            # Usar el filtro de duplicados para remover duplicados internos
            return self.duplicate_filter.remove_internal_duplicates(
                data, key_columns, self.internal_duplicate_strategy
            )

        except Exception as e:
            self.logger.error(
                f"Error removiendo duplicados internos: {str(e)}")
            return data, {'error': str(e)}

    def get_table_info_for_processing(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Obtiene información de la tabla necesaria para el procesamiento.

        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla

        Returns:
            Diccionario con información de la tabla
        """
        try:
            if not self.metadata_utils:
                return {'error': 'Utilidades de metadatos no disponibles'}

            # Obtener información de columnas
            column_info = self.metadata_utils.get_table_column_info(
                schema_name, table_name)

            # Obtener identificadores únicos
            unique_identifiers = self.metadata_utils.get_all_unique_identifiers(
                schema_name, table_name)

            # Obtener el mejor identificador
            best_identifier = self.metadata_utils.get_best_unique_identifier(
                schema_name, table_name)

            return {
                'schema_name': schema_name,
                'table_name': table_name,
                'columns': column_info,
                'unique_identifiers': unique_identifiers,
                'best_identifier': best_identifier,
                'has_unique_identifier': best_identifier is not None,
                'can_filter_duplicates': best_identifier is not None
            }

        except Exception as e:
            self.logger.error(
                f"Error obteniendo información de tabla: {str(e)}")
            return {'error': str(e)}

    def preview_duplicate_filtering(self, data: pd.DataFrame, schema_name: str,
                                    table_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Realiza una vista previa del filtrado de duplicados sin procesar todos los datos.

        Args:
            data: DataFrame con los datos
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            sample_size: Tamaño de la muestra para la vista previa

        Returns:
            Diccionario con información de la vista previa
        """
        try:
            if not self.duplicate_filter:
                return {'error': 'Filtro de duplicados no disponible'}

            # Tomar una muestra de los datos
            sample_data = data.head(sample_size) if len(
                data) > sample_size else data

            # Realizar filtrado en la muestra
            filter_result = self.duplicate_filter.filter_duplicates(
                schema_name, table_name, sample_data
            )

            # Calcular estadísticas proyectadas
            if filter_result.success and filter_result.original_count > 0:
                duplicate_rate = filter_result.duplicate_count / filter_result.original_count
                projected_duplicates = int(len(data) * duplicate_rate)
                projected_new = len(data) - projected_duplicates
            else:
                duplicate_rate = 0
                projected_duplicates = 0
                projected_new = len(data)

            return {
                'success': filter_result.success,
                'sample_size': len(sample_data),
                'total_records': len(data),
                'sample_duplicates': filter_result.duplicate_count,
                'sample_new': filter_result.new_records_count,
                'duplicate_rate': duplicate_rate,
                'projected_duplicates': projected_duplicates,
                'projected_new': projected_new,
                'identifier_info': filter_result.identifier_info,
                'errors': filter_result.errors,
                'warnings': filter_result.warnings
            }

        except Exception as e:
            self.logger.error(f"Error en vista previa de filtrado: {str(e)}")
            return {'error': str(e)}

    def get_processing_summary(self, result: EnhancedProcessingResult) -> str:
        """
        Genera un resumen legible del procesamiento.

        Args:
            result: Resultado del procesamiento mejorado

        Returns:
            String con resumen del procesamiento
        """
        try:
            if not result.success:
                return f"❌ Error en el procesamiento: {', '.join(result.errors)}"

            summary_lines = []
            summary_lines.append("📊 Resumen del Procesamiento de Excel:")
            summary_lines.append(
                f"   • Registros originales: {result.original_rows:,}")

            if result.skipped_rows > 0:
                summary_lines.append(
                    f"   • Registros omitidos: {result.skipped_rows:,}")

            if result.duplicate_rows > 0:
                summary_lines.append(
                    f"   • Registros duplicados: {result.duplicate_rows:,}")
                percentage = (result.duplicate_rows /
                              result.original_rows) * 100
                summary_lines.append(
                    f"   • Porcentaje de duplicados: {percentage:.1f}%")

            summary_lines.append(
                f"   • Registros nuevos para insertar: {result.new_rows:,}")

            if result.identifier_info and 'type' in result.identifier_info:
                identifier = result.identifier_info
                summary_lines.append(
                    f"   • Identificador usado: {identifier['type']} ({', '.join(identifier.get('columns', []))})")

            summary_lines.append(
                f"   • Tiempo de procesamiento: {result.processing_time:.2f} segundos")

            if result.warnings:
                summary_lines.append(
                    f"   ⚠️ Advertencias: {len(result.warnings)}")
                # Mostrar solo las primeras 3
                for warning in result.warnings[:3]:
                    summary_lines.append(f"      - {warning}")
                if len(result.warnings) > 3:
                    summary_lines.append(
                        f"      ... y {len(result.warnings) - 3} más")

            return "\n".join(summary_lines)

        except Exception as e:
            return f"Error generando resumen: {str(e)}"
