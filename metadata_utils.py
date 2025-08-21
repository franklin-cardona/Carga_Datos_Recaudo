"""
Utilidades de Metadatos de Base de Datos
Proporciona funcionalidades para consultar metadatos de SQL Server, incluyendo claves primarias,
índices únicos, y estructura de tablas.

Autor: Manus AI
Fecha: 2025-08-20
"""

import logging
from typing import Dict, List, Any, Optional, Tuple, Set
import pandas as pd


class DatabaseMetadataUtils:
    """
    Clase para consultar metadatos de la base de datos SQL Server.
    """
    
    def __init__(self, db_connection):
        """
        Inicializa las utilidades de metadatos.
        
        Args:
            db_connection: Conexión a la base de datos
        """
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)
    
    def get_table_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """
        Obtiene las columnas que forman la clave primaria de una tabla.
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            
        Returns:
            Lista de nombres de columnas que forman la clave primaria
        """
        try:
            query = """
                SELECT 
                    kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.TABLE_SCHEMA = ?
                    AND tc.TABLE_NAME = ?
                    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY kcu.ORDINAL_POSITION
            """
            
            results = self.db_connection.execute_query(query, (schema_name, table_name))
            primary_keys = [row['COLUMN_NAME'] for row in results]
            
            self.logger.info(f"Claves primarias encontradas para {schema_name}.{table_name}: {primary_keys}")
            return primary_keys
            
        except Exception as e:
            self.logger.error(f"Error obteniendo claves primarias: {str(e)}")
            return []
    
    def get_table_unique_constraints(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtiene todas las restricciones únicas de una tabla.
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            
        Returns:
            Lista de diccionarios con información de restricciones únicas
        """
        try:
            query = """
                SELECT 
                    tc.CONSTRAINT_NAME,
                    kcu.COLUMN_NAME,
                    kcu.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.TABLE_SCHEMA = ?
                    AND tc.TABLE_NAME = ?
                    AND tc.CONSTRAINT_TYPE = 'UNIQUE'
                ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
            """
            
            results = self.db_connection.execute_query(query, (schema_name, table_name))
            
            # Agrupar por constraint_name
            unique_constraints = {}
            for row in results:
                constraint_name = row['CONSTRAINT_NAME']
                if constraint_name not in unique_constraints:
                    unique_constraints[constraint_name] = {
                        'constraint_name': constraint_name,
                        'columns': []
                    }
                unique_constraints[constraint_name]['columns'].append(row['COLUMN_NAME'])
            
            unique_list = list(unique_constraints.values())
            self.logger.info(f"Restricciones únicas encontradas para {schema_name}.{table_name}: {len(unique_list)}")
            return unique_list
            
        except Exception as e:
            self.logger.error(f"Error obteniendo restricciones únicas: {str(e)}")
            return []
    
    def get_table_unique_indexes(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtiene los índices únicos de una tabla.
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            
        Returns:
            Lista de diccionarios con información de índices únicos
        """
        try:
            query = """
                SELECT 
                    i.name AS index_name,
                    c.name AS column_name,
                    ic.key_ordinal
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ?
                    AND t.name = ?
                    AND i.is_unique = 1
                    AND i.is_primary_key = 0  -- Excluir claves primarias (ya las obtenemos por separado)
                ORDER BY i.name, ic.key_ordinal
            """
            
            results = self.db_connection.execute_query(query, (schema_name, table_name))
            
            # Agrupar por index_name
            unique_indexes = {}
            for row in results:
                index_name = row['index_name']
                if index_name not in unique_indexes:
                    unique_indexes[index_name] = {
                        'index_name': index_name,
                        'columns': []
                    }
                unique_indexes[index_name]['columns'].append(row['column_name'])
            
            unique_list = list(unique_indexes.values())
            self.logger.info(f"Índices únicos encontrados para {schema_name}.{table_name}: {len(unique_list)}")
            return unique_list
            
        except Exception as e:
            self.logger.error(f"Error obteniendo índices únicos: {str(e)}")
            return []
    
    def get_all_unique_identifiers(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtiene todos los identificadores únicos de una tabla (claves primarias, restricciones únicas, índices únicos).
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            
        Returns:
            Lista de diccionarios con todos los identificadores únicos
        """
        try:
            unique_identifiers = []
            
            # 1. Clave primaria
            primary_keys = self.get_table_primary_keys(schema_name, table_name)
            if primary_keys:
                unique_identifiers.append({
                    'type': 'PRIMARY_KEY',
                    'name': 'PRIMARY_KEY',
                    'columns': primary_keys,
                    'priority': 1  # Máxima prioridad
                })
            
            # 2. Restricciones únicas
            unique_constraints = self.get_table_unique_constraints(schema_name, table_name)
            for constraint in unique_constraints:
                unique_identifiers.append({
                    'type': 'UNIQUE_CONSTRAINT',
                    'name': constraint['constraint_name'],
                    'columns': constraint['columns'],
                    'priority': 2
                })
            
            # 3. Índices únicos
            unique_indexes = self.get_table_unique_indexes(schema_name, table_name)
            for index in unique_indexes:
                unique_identifiers.append({
                    'type': 'UNIQUE_INDEX',
                    'name': index['index_name'],
                    'columns': index['columns'],
                    'priority': 3
                })
            
            # Ordenar por prioridad
            unique_identifiers.sort(key=lambda x: x['priority'])
            
            self.logger.info(f"Total de identificadores únicos para {schema_name}.{table_name}: {len(unique_identifiers)}")
            return unique_identifiers
            
        except Exception as e:
            self.logger.error(f"Error obteniendo identificadores únicos: {str(e)}")
            return []
    
    def get_best_unique_identifier(self, schema_name: str, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene el mejor identificador único para una tabla (preferencia: PK > UNIQUE CONSTRAINT > UNIQUE INDEX).
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            
        Returns:
            Diccionario con el mejor identificador único o None si no existe
        """
        try:
            unique_identifiers = self.get_all_unique_identifiers(schema_name, table_name)
            
            if not unique_identifiers:
                self.logger.warning(f"No se encontraron identificadores únicos para {schema_name}.{table_name}")
                return None
            
            # El primer elemento ya está ordenado por prioridad
            best_identifier = unique_identifiers[0]
            
            self.logger.info(f"Mejor identificador único para {schema_name}.{table_name}: {best_identifier['type']} - {best_identifier['columns']}")
            return best_identifier
            
        except Exception as e:
            self.logger.error(f"Error obteniendo mejor identificador único: {str(e)}")
            return None
    
    def check_records_exist(self, schema_name: str, table_name: str, 
                           key_columns: List[str], records_data: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica qué registros ya existen en la tabla basándose en las columnas clave.
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            key_columns: Lista de columnas que forman la clave única
            records_data: DataFrame con los datos a verificar
            
        Returns:
            DataFrame con una columna adicional 'exists_in_db' indicando si el registro existe
        """
        try:
            if not key_columns:
                self.logger.warning("No se proporcionaron columnas clave para verificación")
                records_data['exists_in_db'] = False
                return records_data
            
            # Verificar que todas las columnas clave existan en el DataFrame
            missing_columns = [col for col in key_columns if col not in records_data.columns]
            if missing_columns:
                self.logger.error(f"Columnas clave faltantes en los datos: {missing_columns}")
                records_data['exists_in_db'] = False
                return records_data
            
            # Crear una copia del DataFrame para no modificar el original
            result_df = records_data.copy()
            result_df['exists_in_db'] = False
            
            # Procesar en lotes para evitar consultas muy grandes
            batch_size = 100
            total_records = len(result_df)
            
            for start_idx in range(0, total_records, batch_size):
                end_idx = min(start_idx + batch_size, total_records)
                batch_df = result_df.iloc[start_idx:end_idx]
                
                # Construir consulta para el lote actual
                exists_flags = self._check_batch_existence(
                    schema_name, table_name, key_columns, batch_df
                )
                
                # Actualizar flags de existencia
                result_df.iloc[start_idx:end_idx, result_df.columns.get_loc('exists_in_db')] = exists_flags
            
            existing_count = result_df['exists_in_db'].sum()
            self.logger.info(f"Verificación completada: {existing_count}/{total_records} registros ya existen en la base de datos")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error verificando existencia de registros: {str(e)}")
            records_data['exists_in_db'] = False
            return records_data
    
    def _check_batch_existence(self, schema_name: str, table_name: str, 
                              key_columns: List[str], batch_df: pd.DataFrame) -> List[bool]:
        """
        Verifica la existencia de un lote de registros.
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            key_columns: Lista de columnas clave
            batch_df: DataFrame con el lote de datos
            
        Returns:
            Lista de booleanos indicando existencia
        """
        try:
            exists_flags = []
            
            for _, row in batch_df.iterrows():
                # Construir condiciones WHERE para las columnas clave
                where_conditions = []
                params = []
                
                for col in key_columns:
                    value = row[col]
                    if pd.isna(value):
                        where_conditions.append(f"[{col}] IS NULL")
                    else:
                        where_conditions.append(f"[{col}] = ?")
                        params.append(value)
                
                where_clause = " AND ".join(where_conditions)
                
                # Consulta de existencia
                query = f"""
                    SELECT COUNT(*) as record_count
                    FROM [{schema_name}].[{table_name}]
                    WHERE {where_clause}
                """
                
                try:
                    results = self.db_connection.execute_query(query, params)
                    record_exists = results[0]['record_count'] > 0 if results else False
                    exists_flags.append(record_exists)
                    
                except Exception as query_error:
                    self.logger.warning(f"Error en consulta individual: {str(query_error)}")
                    exists_flags.append(False)  # Asumir que no existe en caso de error
            
            return exists_flags
            
        except Exception as e:
            self.logger.error(f"Error verificando lote: {str(e)}")
            return [False] * len(batch_df)
    
    def filter_new_records(self, schema_name: str, table_name: str, 
                          records_data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Filtra los registros que no existen en la base de datos.
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            records_data: DataFrame con los datos a filtrar
            
        Returns:
            Tupla con (DataFrame de registros nuevos, estadísticas del filtrado)
        """
        try:
            # Obtener el mejor identificador único para la tabla
            best_identifier = self.get_best_unique_identifier(schema_name, table_name)
            
            if not best_identifier:
                self.logger.warning(f"No se encontró identificador único para {schema_name}.{table_name}. Se insertarán todos los registros.")
                return records_data, {
                    'total_records': len(records_data),
                    'new_records': len(records_data),
                    'existing_records': 0,
                    'identifier_used': None,
                    'warning': 'No se encontró identificador único - no se verificaron duplicados'
                }
            
            key_columns = best_identifier['columns']
            
            # Verificar existencia de registros
            records_with_existence = self.check_records_exist(
                schema_name, table_name, key_columns, records_data
            )
            
            # Filtrar solo registros nuevos
            new_records = records_with_existence[~records_with_existence['exists_in_db']].copy()
            
            # Remover la columna auxiliar
            if 'exists_in_db' in new_records.columns:
                new_records = new_records.drop('exists_in_db', axis=1)
            
            # Estadísticas
            total_records = len(records_data)
            new_count = len(new_records)
            existing_count = total_records - new_count
            
            stats = {
                'total_records': total_records,
                'new_records': new_count,
                'existing_records': existing_count,
                'identifier_used': {
                    'type': best_identifier['type'],
                    'name': best_identifier['name'],
                    'columns': best_identifier['columns']
                },
                'filter_rate': new_count / total_records if total_records > 0 else 0
            }
            
            self.logger.info(f"Filtrado completado: {new_count}/{total_records} registros son nuevos")
            
            return new_records, stats
            
        except Exception as e:
            self.logger.error(f"Error filtrando registros nuevos: {str(e)}")
            return records_data, {
                'total_records': len(records_data),
                'new_records': len(records_data),
                'existing_records': 0,
                'identifier_used': None,
                'error': str(e)
            }
    
    def get_table_column_info(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtiene información detallada de las columnas de una tabla.
        
        Args:
            schema_name: Nombre del esquema
            table_name: Nombre de la tabla
            
        Returns:
            Lista de diccionarios con información de columnas
        """
        try:
            query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """
            
            results = self.db_connection.execute_query(query, (schema_name, table_name))
            
            self.logger.info(f"Información de columnas obtenida para {schema_name}.{table_name}: {len(results)} columnas")
            return results
            
        except Exception as e:
            self.logger.error(f"Error obteniendo información de columnas: {str(e)}")
            return []

