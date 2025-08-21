"""
Módulo de Mapeo de Tablas
Proporciona funcionalidad para mapear columnas de Excel a columnas de SQL Server usando coincidencia difusa.

Autor: Manus AI
Fecha: 2025-01-08
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from fuzzywuzzy import fuzz, process
import re
import pandas as pd
from datetime import datetime
import numpy as np


class TableMapper:
    """
    Clase para mapear columnas de Excel a columnas de SQL Server usando coincidencia difusa.
    """

    def __init__(self, db_connection):
        """
        Inicializa el mapeador de tablas.

        Args:
            db_connection: Conexión a la base de datos
        """
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)

        # Configuración de coincidencia difusa
        self.fuzzy_threshold = 70  # Umbral mínimo de coincidencia
        self.high_confidence_threshold = 85  # Umbral para alta confianza

        # Patrones comunes de nombres de columnas
        self.common_patterns = {
            'id': ['id', 'identifier', 'key', 'codigo', 'code'],
            'name': ['name', 'nombre', 'title', 'titulo', 'descripcion', 'description'],
            'date': ['date', 'fecha', 'time', 'tiempo', 'created', 'updated', 'modified'],
            'email': ['email', 'correo', 'mail'],
            'phone': ['phone', 'telefono', 'tel', 'celular', 'mobile'],
            'address': ['address', 'direccion', 'location', 'ubicacion'],
            'status': ['status', 'estado', 'active', 'activo', 'enabled'],
            'amount': ['amount', 'monto', 'price', 'precio', 'cost', 'costo', 'value', 'valor'],
            'quantity': ['quantity', 'cantidad', 'qty', 'count', 'numero']
        }

        # Mapeo de tipos de datos SQL Server a tipos Python
        self.sql_type_mapping = {
            'int': ['int', 'integer', 'bigint', 'smallint', 'tinyint'],
            'float': ['float', 'real', 'decimal', 'numeric', 'money', 'smallmoney'],
            'string': ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'],
            'datetime': ['datetime', 'datetime2', 'date', 'time', 'smalldatetime'],
            'boolean': ['bit'],
            'binary': ['binary', 'varbinary', 'image']
        }

    def suggest_column_mappings(self, excel_columns: List[str], sql_columns: List[str],
                                table_structure: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sugiere mapeos de columnas usando coincidencia difusa.

        Args:
            excel_columns: Lista de nombres de columnas de Excel
            sql_columns: Lista de nombres de columnas de SQL
            table_structure: Estructura completa de la tabla SQL

        Returns:
            Lista de mapeos sugeridos con scores de confianza
        """
        try:
            self.logger.info(
                f"Iniciando mapeo de {len(excel_columns)} columnas Excel a {len(sql_columns)} columnas SQL")

            mappings = []
            used_sql_columns = set()

            # Crear diccionario de estructura para acceso rápido
            structure_dict = {col['COLUMN_NAME']                              : col for col in table_structure}

            for excel_col in excel_columns:
                best_mapping = self._find_best_column_match(
                    excel_col, sql_columns, structure_dict, used_sql_columns
                )

                if best_mapping:
                    mappings.append(best_mapping)
                    used_sql_columns.add(best_mapping['sql_column'])
                else:
                    # No se encontró mapeo válido
                    mappings.append({
                        'excel_column': excel_col,
                        'sql_column': None,
                        'data_type': 'unknown',
                        'confidence': 0.0,
                        'match_type': 'no_match',
                        'suggestions': self._get_alternative_suggestions(excel_col, sql_columns, used_sql_columns)
                    })

            # Ordenar por confianza descendente
            mappings.sort(key=lambda x: x['confidence'], reverse=True)

            self.logger.info(
                f"Mapeo completado: {sum(1 for m in mappings if m['confidence'] > 0.7)}/{len(excel_columns)} columnas mapeadas con alta confianza")

            return mappings

        except Exception as e:
            self.logger.error(f"Error en mapeo de columnas: {str(e)}")
            raise

    def _find_best_column_match(self, excel_col: str, sql_columns: List[str],
                                structure_dict: Dict[str, Any], used_columns: set) -> Optional[Dict[str, Any]]:
        """
        Encuentra la mejor coincidencia para una columna de Excel.

        Args:
            excel_col: Nombre de la columna de Excel
            sql_columns: Lista de columnas SQL disponibles
            structure_dict: Diccionario con estructura de la tabla
            used_columns: Conjunto de columnas ya utilizadas

        Returns:
            Diccionario con el mejor mapeo o None si no hay coincidencia válida
        """
        try:
            # Filtrar columnas ya utilizadas
            available_columns = [
                col for col in sql_columns if col not in used_columns]

            if not available_columns:
                return None

            # Normalizar nombre de columna Excel
            normalized_excel = self._normalize_column_name(excel_col)

            # Calcular scores de coincidencia
            matches = []

            for sql_col in available_columns:
                normalized_sql = self._normalize_column_name(sql_col)

                # Múltiples algoritmos de coincidencia
                scores = {
                    'ratio': fuzz.ratio(normalized_excel, normalized_sql),
                    'partial_ratio': fuzz.partial_ratio(normalized_excel, normalized_sql),
                    'token_sort': fuzz.token_sort_ratio(normalized_excel, normalized_sql),
                    'token_set': fuzz.token_set_ratio(normalized_excel, normalized_sql)
                }

                # Score ponderado
                weighted_score = (
                    scores['ratio'] * 0.3 +
                    scores['partial_ratio'] * 0.2 +
                    scores['token_sort'] * 0.3 +
                    scores['token_set'] * 0.2
                )

                # Bonus por patrones comunes
                pattern_bonus = self._calculate_pattern_bonus(
                    normalized_excel, normalized_sql)
                final_score = min(100, weighted_score + pattern_bonus)

                if final_score >= self.fuzzy_threshold:
                    matches.append({
                        'sql_column': sql_col,
                        'score': final_score,
                        'scores_detail': scores,
                        'pattern_bonus': pattern_bonus
                    })

            if not matches:
                return None

            # Seleccionar la mejor coincidencia
            best_match = max(matches, key=lambda x: x['score'])

            # Obtener información de la columna SQL
            sql_info = structure_dict[best_match['sql_column']]

            # Determinar tipo de coincidencia
            match_type = self._determine_match_type(best_match['score'])

            return {
                'excel_column': excel_col,
                'sql_column': best_match['sql_column'],
                'data_type': self._get_simplified_data_type(sql_info['DATA_TYPE']),
                'confidence': best_match['score'] / 100.0,
                'match_type': match_type,
                'sql_info': {
                    'data_type': sql_info['DATA_TYPE'],
                    'is_nullable': sql_info['IS_NULLABLE'],
                    'max_length': sql_info['CHARACTER_MAXIMUM_LENGTH'],
                    'precision': sql_info['NUMERIC_PRECISION'],
                    'scale': sql_info['NUMERIC_SCALE']
                },
                'scores_detail': best_match['scores_detail'],
                'pattern_bonus': best_match['pattern_bonus']
            }

        except Exception as e:
            self.logger.error(
                f"Error encontrando coincidencia para {excel_col}: {str(e)}")
            return None

    def _normalize_column_name(self, column_name: str) -> str:
        """
        Normaliza un nombre de columna para mejorar la coincidencia.

        Args:
            column_name: Nombre original de la columna

        Returns:
            Nombre normalizado
        """
        try:
            # Convertir a minúsculas
            normalized = column_name.lower().strip()

            # Remover caracteres especiales y espacios
            normalized = re.sub(r'[^a-z0-9]', '', normalized)

            # Remover prefijos/sufijos comunes
            prefixes_to_remove = ['tbl', 'col', 'fld', 'field']
            suffixes_to_remove = ['id', 'key', 'code', 'num', 'no']

            for prefix in prefixes_to_remove:
                if normalized.startswith(prefix) and len(normalized) > len(prefix):
                    normalized = normalized[len(prefix):]
                    break

            for suffix in suffixes_to_remove:
                if normalized.endswith(suffix) and len(normalized) > len(suffix):
                    normalized = normalized[:-len(suffix)]
                    break

            return normalized

        except Exception as e:
            self.logger.warning(
                f"Error normalizando nombre de columna {column_name}: {str(e)}")
            return column_name.lower()

    def _calculate_pattern_bonus(self, excel_col: str, sql_col: str) -> float:
        """
        Calcula bonus por patrones comunes de nombres.

        Args:
            excel_col: Columna Excel normalizada
            sql_col: Columna SQL normalizada

        Returns:
            Bonus de puntuación (0-20)
        """
        try:
            bonus = 0.0

            # Verificar patrones comunes
            for pattern_type, patterns in self.common_patterns.items():
                excel_matches = any(
                    pattern in excel_col for pattern in patterns)
                sql_matches = any(pattern in sql_col for pattern in patterns)

                if excel_matches and sql_matches:
                    bonus += 15.0
                    break
                elif excel_matches or sql_matches:
                    # Bonus menor si solo una columna coincide con el patrón
                    bonus += 5.0

            # Bonus por longitud similar
            length_diff = abs(len(excel_col) - len(sql_col))
            if length_diff <= 2:
                bonus += 5.0
            elif length_diff <= 5:
                bonus += 2.0

            return min(20.0, bonus)

        except Exception as e:
            self.logger.warning(f"Error calculando bonus de patrón: {str(e)}")
            return 0.0

    def _determine_match_type(self, score: float) -> str:
        """
        Determina el tipo de coincidencia basado en el score.

        Args:
            score: Score de coincidencia (0-100)

        Returns:
            Tipo de coincidencia
        """
        if score >= self.high_confidence_threshold:
            return 'exact_match'
        elif score >= self.fuzzy_threshold:
            return 'fuzzy_match'
        else:
            return 'low_confidence'

    def _get_simplified_data_type(self, sql_data_type: str) -> str:
        """
        Convierte un tipo de dato SQL a un tipo simplificado.

        Args:
            sql_data_type: Tipo de dato SQL Server

        Returns:
            Tipo de dato simplificado
        """
        try:
            sql_type_lower = sql_data_type.lower()

            for simple_type, sql_types in self.sql_type_mapping.items():
                if any(sql_type in sql_type_lower for sql_type in sql_types):
                    return simple_type

            return 'string'  # Tipo por defecto

        except Exception as e:
            self.logger.warning(
                f"Error simplificando tipo de dato {sql_data_type}: {str(e)}")
            return 'string'

    def _get_alternative_suggestions(self, excel_col: str, sql_columns: List[str],
                                     used_columns: set, max_suggestions: int = 3) -> List[Dict[str, Any]]:
        """
        Obtiene sugerencias alternativas para una columna sin mapeo.

        Args:
            excel_col: Columna de Excel
            sql_columns: Lista de columnas SQL
            used_columns: Columnas ya utilizadas
            max_suggestions: Número máximo de sugerencias

        Returns:
            Lista de sugerencias alternativas
        """
        try:
            available_columns = [
                col for col in sql_columns if col not in used_columns]

            if not available_columns:
                return []

            # Usar fuzzywuzzy para obtener las mejores coincidencias
            matches = process.extract(
                excel_col,
                available_columns,
                scorer=fuzz.token_sort_ratio,
                limit=max_suggestions
            )

            suggestions = []
            for match, score in matches:
                if score >= 50:  # Umbral más bajo para sugerencias
                    suggestions.append({
                        'sql_column': match,
                        'confidence': score / 100.0,
                        'reason': 'fuzzy_similarity'
                    })

            return suggestions

        except Exception as e:
            self.logger.warning(
                f"Error obteniendo sugerencias para {excel_col}: {str(e)}")
            return []

    def validate_column_mapping(self, excel_data: pd.Series, sql_column_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida si los datos de Excel son compatibles con la columna SQL.

        Args:
            excel_data: Serie de pandas con datos de Excel
            sql_column_info: Información de la columna SQL

        Returns:
            Diccionario con resultado de validación
        """
        try:
            validation_result = {
                'is_valid': True,
                'warnings': [],
                'errors': [],
                'data_type_compatible': True,
                'null_compatibility': True,
                'length_compatibility': True,
                'sample_conversions': []
            }

            # Validar tipos de datos
            sql_type = sql_column_info['data_type'].lower()

            # Obtener muestra de datos no nulos
            non_null_data = excel_data.dropna()
            if len(non_null_data) == 0:
                validation_result['warnings'].append(
                    "Todos los valores son nulos")
                return validation_result

            sample_data = non_null_data.head(10)

            # Validar compatibilidad de tipos
            type_validation = self._validate_data_type_compatibility(
                sample_data, sql_type)
            validation_result.update(type_validation)

            # Validar nulos
            if excel_data.isnull().any() and sql_column_info['is_nullable'] == 'NO':
                validation_result['null_compatibility'] = False
                validation_result['errors'].append(
                    "La columna SQL no permite valores nulos pero Excel contiene valores vacíos")

            # Validar longitud para tipos de texto
            if 'varchar' in sql_type or 'char' in sql_type:
                max_length = sql_column_info.get('max_length')
                if max_length:
                    length_validation = self._validate_string_length(
                        sample_data, max_length)
                    validation_result.update(length_validation)

            # Determinar validez general
            validation_result['is_valid'] = (
                validation_result['data_type_compatible'] and
                validation_result['null_compatibility'] and
                validation_result['length_compatibility']
            )

            return validation_result

        except Exception as e:
            self.logger.error(f"Error validando mapeo de columna: {str(e)}")
            return {
                'is_valid': False,
                'errors': [f"Error en validación: {str(e)}"],
                'warnings': [],
                'data_type_compatible': False,
                'null_compatibility': False,
                'length_compatibility': False,
                'sample_conversions': []
            }

    def get_mapping_statistics(self, mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calcula estadísticas de los mapeos realizados.

        Args:
            mappings: Lista de mapeos

        Returns:
            Diccionario con estadísticas
        """
        try:
            total_columns = len(mappings)
            mapped_columns = sum(1 for m in mappings if m['confidence'] > 0)
            high_confidence = sum(
                1 for m in mappings if m['confidence'] >= 0.85)
            medium_confidence = sum(
                1 for m in mappings if 0.7 <= m['confidence'] < 0.85)
            low_confidence = sum(1 for m in mappings if 0 <
                                 m['confidence'] < 0.7)
            unmapped = total_columns - mapped_columns

            return {
                'total_columns': total_columns,
                'mapped_columns': mapped_columns,
                'unmapped_columns': unmapped,
                'high_confidence': high_confidence,
                'medium_confidence': medium_confidence,
                'low_confidence': low_confidence,
                'mapping_rate': mapped_columns / total_columns if total_columns > 0 else 0,
                'high_confidence_rate': high_confidence / total_columns if total_columns > 0 else 0,
                'average_confidence': sum(m['confidence'] for m in mappings) / total_columns if total_columns > 0 else 0
            }

        except Exception as e:
            self.logger.error(f"Error calculando estadísticas: {str(e)}")
            return {
                'total_columns': 0,
                'mapped_columns': 0,
                'unmapped_columns': 0,
                'high_confidence': 0,
                'medium_confidence': 0,
                'low_confidence': 0,
                'mapping_rate': 0,
                'high_confidence_rate': 0,
                'average_confidence': 0
            }
