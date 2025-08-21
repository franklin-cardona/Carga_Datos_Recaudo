"""
Filtro de Duplicados para Integración Excel-SQL
Proporciona funcionalidades para filtrar registros duplicados antes de la inserción en la base de datos.

Autor: Manus AI
Fecha: 2025-08-20
"""

import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime


@dataclass
class DuplicateFilterResult:
    """
    Resultado del filtrado de duplicados.
    """
    success: bool
    original_count: int
    new_records_count: int
    duplicate_count: int
    filtered_data: Optional[pd.DataFrame]
    identifier_info: Dict[str, Any]
    processing_time: float
    errors: List[str]
    warnings: List[str]


class DuplicateFilter:
    """
    Clase para filtrar registros duplicados basándose en claves únicas de la base de datos.
    """
    
    def __init__(self, db_connection, metadata_utils):
        """
        Inicializa el filtro de duplicados.
        
        Args:
            db_connection: Conexión a la base de datos
            metadata_utils: Instancia de DatabaseMetadataUtils
        """
        self.db_connection = db_connection
        self.metadata_utils = metadata_utils
        self.logger = logging.getLogger(__name__)
        
        # Configuración
        self.batch_size = 100  # Tamaño de lote para consultas de existencia
        self.max_records_for_filtering = 10000  # Máximo de registros para filtrar
    
    def filter_duplicates(self, schema_name: str, table_name: str, 
                         data: pd.DataFrame) -> DuplicateFilterResult:
        """
        Filtra registros duplicados de un DataFrame basándose en las claves únicas de la tabla.
        
        Args:
            schema_name: Nombre del esquema de la tabla destino
            table_name: Nombre de la tabla destino
            data: DataFrame con los datos a filtrar
            
        Returns:
            Resultado del filtrado con estadísticas y datos filtrados
        """
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Iniciando filtrado de duplicados para {schema_name}.{table_name}")
            self.logger.info(f"Registros originales: {len(data)}")
            
            # Validaciones iniciales
            if data.empty:
                return DuplicateFilterResult(
                    success=True,
                    original_count=0,
                    new_records_count=0,
                    duplicate_count=0,
                    filtered_data=data,
                    identifier_info={},
                    processing_time=0.0,
                    errors=[],
                    warnings=["DataFrame vacío - no hay datos para filtrar"]
                )
            
            if len(data) > self.max_records_for_filtering:
                warning_msg = f"El DataFrame tiene {len(data)} registros, excede el límite de {self.max_records_for_filtering}. Se procesarán solo los primeros {self.max_records_for_filtering} registros."
                self.logger.warning(warning_msg)
                data = data.head(self.max_records_for_filtering)
            
            # Obtener identificador único de la tabla
            best_identifier = self.metadata_utils.get_best_unique_identifier(schema_name, table_name)
            
            if not best_identifier:
                # No hay identificador único, no se pueden filtrar duplicados
                processing_time = (datetime.now() - start_time).total_seconds()
                return DuplicateFilterResult(
                    success=True,
                    original_count=len(data),
                    new_records_count=len(data),
                    duplicate_count=0,
                    filtered_data=data,
                    identifier_info={'warning': 'No se encontró identificador único'},
                    processing_time=processing_time,
                    errors=[],
                    warnings=[f"No se encontró identificador único para {schema_name}.{table_name}. No se pueden filtrar duplicados."]
                )
            
            # Verificar que las columnas clave existan en el DataFrame
            key_columns = best_identifier['columns']
            missing_columns = [col for col in key_columns if col not in data.columns]
            
            if missing_columns:
                error_msg = f"Columnas clave faltantes en los datos: {missing_columns}"
                self.logger.error(error_msg)
                processing_time = (datetime.now() - start_time).total_seconds()
                return DuplicateFilterResult(
                    success=False,
                    original_count=len(data),
                    new_records_count=0,
                    duplicate_count=0,
                    filtered_data=None,
                    identifier_info=best_identifier,
                    processing_time=processing_time,
                    errors=[error_msg],
                    warnings=[]
                )
            
            # Filtrar registros nuevos usando metadata_utils
            filtered_data, filter_stats = self.metadata_utils.filter_new_records(
                schema_name, table_name, data
            )
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Preparar resultado
            result = DuplicateFilterResult(
                success=True,
                original_count=filter_stats['total_records'],
                new_records_count=filter_stats['new_records'],
                duplicate_count=filter_stats['existing_records'],
                filtered_data=filtered_data,
                identifier_info=filter_stats['identifier_used'],
                processing_time=processing_time,
                errors=[],
                warnings=[]
            )
            
            # Agregar advertencias si es necesario
            if 'warning' in filter_stats:
                result.warnings.append(filter_stats['warning'])
            
            if 'error' in filter_stats:
                result.errors.append(filter_stats['error'])
                result.success = False
            
            self.logger.info(f"Filtrado completado: {result.new_records_count}/{result.original_count} registros son nuevos")
            self.logger.info(f"Tiempo de procesamiento: {processing_time:.2f} segundos")
            
            return result
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Error durante el filtrado de duplicados: {str(e)}"
            self.logger.error(error_msg)
            
            return DuplicateFilterResult(
                success=False,
                original_count=len(data) if data is not None else 0,
                new_records_count=0,
                duplicate_count=0,
                filtered_data=None,
                identifier_info={},
                processing_time=processing_time,
                errors=[error_msg],
                warnings=[]
            )
    
    def analyze_duplicates_in_data(self, data: pd.DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """
        Analiza duplicados internos en el DataFrame (no contra la base de datos).
        
        Args:
            data: DataFrame a analizar
            key_columns: Columnas que forman la clave única
            
        Returns:
            Diccionario con análisis de duplicados internos
        """
        try:
            if data.empty or not key_columns:
                return {
                    'has_internal_duplicates': False,
                    'duplicate_count': 0,
                    'unique_count': len(data),
                    'duplicate_groups': []
                }
            
            # Verificar que las columnas existan
            missing_columns = [col for col in key_columns if col not in data.columns]
            if missing_columns:
                return {
                    'error': f"Columnas faltantes: {missing_columns}",
                    'has_internal_duplicates': False,
                    'duplicate_count': 0,
                    'unique_count': len(data)
                }
            
            # Identificar duplicados internos
            duplicated_mask = data.duplicated(subset=key_columns, keep=False)
            has_duplicates = duplicated_mask.any()
            
            if not has_duplicates:
                return {
                    'has_internal_duplicates': False,
                    'duplicate_count': 0,
                    'unique_count': len(data),
                    'duplicate_groups': []
                }
            
            # Analizar grupos de duplicados
            duplicate_data = data[duplicated_mask]
            duplicate_groups = []
            
            for key_values, group in duplicate_data.groupby(key_columns):
                if len(group) > 1:
                    if isinstance(key_values, tuple):
                        key_dict = dict(zip(key_columns, key_values))
                    else:
                        key_dict = {key_columns[0]: key_values}
                    
                    duplicate_groups.append({
                        'key_values': key_dict,
                        'count': len(group),
                        'indices': group.index.tolist()
                    })
            
            return {
                'has_internal_duplicates': True,
                'duplicate_count': duplicated_mask.sum(),
                'unique_count': len(data) - duplicated_mask.sum(),
                'duplicate_groups': duplicate_groups,
                'total_groups': len(duplicate_groups)
            }
            
        except Exception as e:
            self.logger.error(f"Error analizando duplicados internos: {str(e)}")
            return {
                'error': str(e),
                'has_internal_duplicates': False,
                'duplicate_count': 0,
                'unique_count': len(data) if data is not None else 0
            }
    
    def remove_internal_duplicates(self, data: pd.DataFrame, key_columns: List[str], 
                                  keep: str = 'first') -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Remueve duplicados internos del DataFrame.
        
        Args:
            data: DataFrame original
            key_columns: Columnas que forman la clave única
            keep: Estrategia para mantener duplicados ('first', 'last', False)
            
        Returns:
            Tupla con (DataFrame sin duplicados internos, estadísticas)
        """
        try:
            if data.empty or not key_columns:
                return data, {
                    'original_count': len(data),
                    'final_count': len(data),
                    'removed_count': 0
                }
            
            # Analizar duplicados antes de remover
            duplicate_analysis = self.analyze_duplicates_in_data(data, key_columns)
            
            if not duplicate_analysis.get('has_internal_duplicates', False):
                return data, {
                    'original_count': len(data),
                    'final_count': len(data),
                    'removed_count': 0,
                    'message': 'No se encontraron duplicados internos'
                }
            
            # Remover duplicados
            deduplicated_data = data.drop_duplicates(subset=key_columns, keep=keep)
            
            stats = {
                'original_count': len(data),
                'final_count': len(deduplicated_data),
                'removed_count': len(data) - len(deduplicated_data),
                'duplicate_groups_found': duplicate_analysis.get('total_groups', 0),
                'keep_strategy': keep
            }
            
            self.logger.info(f"Duplicados internos removidos: {stats['removed_count']} registros")
            
            return deduplicated_data, stats
            
        except Exception as e:
            self.logger.error(f"Error removiendo duplicados internos: {str(e)}")
            return data, {
                'original_count': len(data) if data is not None else 0,
                'final_count': len(data) if data is not None else 0,
                'removed_count': 0,
                'error': str(e)
            }
    
    def get_duplicate_summary(self, filter_result: DuplicateFilterResult) -> str:
        """
        Genera un resumen legible del resultado del filtrado.
        
        Args:
            filter_result: Resultado del filtrado de duplicados
            
        Returns:
            String con resumen del filtrado
        """
        try:
            if not filter_result.success:
                return f"❌ Error en el filtrado: {', '.join(filter_result.errors)}"
            
            summary_lines = []
            summary_lines.append(f"📊 Resumen del Filtrado de Duplicados:")
            summary_lines.append(f"   • Registros originales: {filter_result.original_count:,}")
            summary_lines.append(f"   • Registros nuevos: {filter_result.new_records_count:,}")
            summary_lines.append(f"   • Registros duplicados: {filter_result.duplicate_count:,}")
            
            if filter_result.duplicate_count > 0:
                percentage = (filter_result.duplicate_count / filter_result.original_count) * 100
                summary_lines.append(f"   • Porcentaje de duplicados: {percentage:.1f}%")
            
            if filter_result.identifier_info:
                identifier = filter_result.identifier_info
                if 'type' in identifier and 'columns' in identifier:
                    summary_lines.append(f"   • Identificador usado: {identifier['type']} ({', '.join(identifier['columns'])})")
            
            summary_lines.append(f"   • Tiempo de procesamiento: {filter_result.processing_time:.2f} segundos")
            
            if filter_result.warnings:
                summary_lines.append(f"   ⚠️ Advertencias: {len(filter_result.warnings)}")
                for warning in filter_result.warnings:
                    summary_lines.append(f"      - {warning}")
            
            return "\n".join(summary_lines)
            
        except Exception as e:
            return f"Error generando resumen: {str(e)}"

