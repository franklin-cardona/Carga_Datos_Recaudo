"""
Mapeador de Tablas
Proporciona funcionalidad para mapear automáticamente estructuras de Excel a tablas de base de datos.

Autor: FRANKLIN ANDRES CARDONA YARA
Fecha: 2025-01-08
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from fuzzywuzzy import fuzz
import re

from connection import DatabaseConnection
from excel_processor import DataType, ColumnMapping, WorksheetMapping


@dataclass
class TableSchema:
    """Representa el esquema de una tabla de base de datos."""
    table_name: str
    schema_name: str
    columns: List[Dict[str, Any]] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, str]] = field(default_factory=list)
    unique_constraints: List[str] = field(default_factory=list)
    not_null_columns: List[str] = field(default_factory=list)

    def get_column_names(self) -> List[str]:
        """Retorna lista de nombres de columnas."""
        return [col['column_name'] for col in self.columns]

    def get_column_info(self, column_name: str) -> Optional[Dict[str, Any]]:
        """Obtiene información de una columna específica."""
        for col in self.columns:
            if col['column_name'].lower() == column_name.lower():
                return col
        return None


class TableMapper:
    """
    Mapeador automático de tablas de base de datos.
    """

    def __init__(self, db_connection: DatabaseConnection):
        """
        Inicializa el mapeador de tablas.

        Args:
            db_connection: Conexión a la base de datos
        """
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)
        self.table_schemas = {}
        self.data_type_mapping = self._setup_data_type_mapping()

        # Cargar esquemas de tablas
        self._load_table_schemas()

    def _setup_data_type_mapping(self) -> Dict[str, DataType]:
        """Configura el mapeo de tipos de datos SQL a tipos internos."""
        return {
            # Tipos de string
            'varchar': DataType.STRING,
            'nvarchar': DataType.STRING,
            'char': DataType.STRING,
            'nchar': DataType.STRING,
            'text': DataType.STRING,
            'ntext': DataType.STRING,

            # Tipos numéricos enteros
            'int': DataType.INTEGER,
            'integer': DataType.INTEGER,
            'bigint': DataType.INTEGER,
            'smallint': DataType.INTEGER,
            'tinyint': DataType.INTEGER,

            # Tipos numéricos decimales
            'decimal': DataType.DECIMAL,
            'numeric': DataType.DECIMAL,
            'float': DataType.DECIMAL,
            'real': DataType.DECIMAL,
            'money': DataType.DECIMAL,
            'smallmoney': DataType.DECIMAL,

            # Tipos booleanos
            'bit': DataType.BOOLEAN,
            'boolean': DataType.BOOLEAN,

            # Tipos de fecha/hora
            'date': DataType.DATE,
            'datetime': DataType.DATETIME,
            'datetime2': DataType.DATETIME,
            'smalldatetime': DataType.DATETIME,
            'time': DataType.DATETIME,
            'timestamp': DataType.DATETIME,
        }

    def _load_table_schemas(self):
        """Carga los esquemas de todas las tablas disponibles."""
        try:
            # Consulta para obtener información de tablas y columnas
            query = """
            SELECT 
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.TABLES t
            INNER JOIN INFORMATION_SCHEMA.COLUMNS c 
                ON t.TABLE_SCHEMA = c.TABLE_SCHEMA 
                AND t.TABLE_NAME = c.TABLE_NAME
            WHERE t.TABLE_TYPE = 'BASE TABLE'
                AND t.TABLE_SCHEMA IN ('Data', 'dbo')
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
            """

            results = self.db_connection.execute_query(query)

            # Agrupar por tabla
            current_table = None
            current_schema = None

            for row in results:
                table_key = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"

                if table_key not in self.table_schemas:
                    self.table_schemas[table_key] = TableSchema(
                        table_name=row['TABLE_NAME'],
                        schema_name=row['TABLE_SCHEMA']
                    )

                # Mapear tipo de datos
                sql_type = row['DATA_TYPE'].lower()
                mapped_type = self.data_type_mapping.get(
                    sql_type, DataType.STRING)

                # Información de la columna
                column_info = {
                    'column_name': row['COLUMN_NAME'],
                    'data_type': mapped_type,
                    'sql_data_type': row['DATA_TYPE'],
                    'max_length': row['CHARACTER_MAXIMUM_LENGTH'],
                    'precision': row['NUMERIC_PRECISION'],
                    'scale': row['NUMERIC_SCALE'],
                    'is_nullable': row['IS_NULLABLE'] == 'YES',
                    'default_value': row['COLUMN_DEFAULT'],
                    'ordinal_position': row['ORDINAL_POSITION']
                }

                self.table_schemas[table_key].columns.append(column_info)

                # Agregar a lista de columnas no nulas si es requerida
                if not column_info['is_nullable']:
                    self.table_schemas[table_key].not_null_columns.append(
                        row['COLUMN_NAME'])

            # Cargar información de claves primarias y restricciones
            self._load_table_constraints()

            self.logger.info(
                f"Cargados esquemas de {len(self.table_schemas)} tablas")

        except Exception as e:
            self.logger.error(f"Error cargando esquemas de tablas: {str(e)}")
            raise

    def _load_table_constraints(self):
        """Carga información de restricciones de las tablas."""
        try:
            # Consulta para claves primarias
            pk_query = """
            SELECT 
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME,
                kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA IN ('Data', 'dbo')
            """

            pk_results = self.db_connection.execute_query(pk_query)

            for row in pk_results:
                table_key = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
                if table_key in self.table_schemas:
                    self.table_schemas[table_key].primary_keys.append(
                        row['COLUMN_NAME'])

            # Consulta para restricciones UNIQUE
            unique_query = """
            SELECT 
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME,
                kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'UNIQUE'
                AND tc.TABLE_SCHEMA IN ('Data', 'dbo')
            """

            unique_results = self.db_connection.execute_query(unique_query)

            for row in unique_results:
                table_key = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
                if table_key in self.table_schemas:
                    self.table_schemas[table_key].unique_constraints.append(
                        row['COLUMN_NAME'])

            # Consulta para claves foráneas
            fk_query = """
            SELECT 
                kcu.TABLE_SCHEMA,
                kcu.TABLE_NAME,
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_SCHEMA,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            WHERE kcu.REFERENCED_TABLE_NAME IS NOT NULL
                AND kcu.TABLE_SCHEMA IN ('Data', 'dbo')
            """

            fk_results = self.db_connection.execute_query(fk_query)

            for row in fk_results:
                table_key = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
                if table_key in self.table_schemas:
                    fk_info = {
                        'column': row['COLUMN_NAME'],
                        'referenced_table': row['REFERENCED_TABLE_NAME'],
                        'referenced_column': row['REFERENCED_COLUMN_NAME'],
                        'referenced_schema': row['REFERENCED_TABLE_SCHEMA']
                    }
                    self.table_schemas[table_key].foreign_keys.append(fk_info)

        except Exception as e:
            self.logger.warning(
                f"Error cargando restricciones de tablas: {str(e)}")

    def get_available_tables(self) -> Dict[str, List[str]]:
        """
        Obtiene lista de tablas disponibles y sus columnas.

        Returns:
            Diccionario {nombre_tabla: [columnas]}
        """
        tables = {}
        for table_key, schema in self.table_schemas.items():
            tables[schema.table_name] = schema.get_column_names()
        return tables

    def get_table_schema(self, table_name: str, schema_name: str = None) -> Optional[TableSchema]:
        """
        Obtiene el esquema de una tabla específica.

        Args:
            table_name: Nombre de la tabla
            schema_name: Nombre del esquema (opcional)

        Returns:
            Esquema de la tabla o None si no se encuentra
        """
        # Buscar por nombre completo si se proporciona esquema
        if schema_name:
            table_key = f"{schema_name}.{table_name}"
            return self.table_schemas.get(table_key)

        # Buscar por nombre de tabla solamente
        for table_key, schema in self.table_schemas.items():
            if schema.table_name.lower() == table_name.lower():
                return schema

        return None

    def suggest_table_mapping(self, worksheet_name: str,
                              excel_columns: List[str]) -> List[Tuple[str, float, TableSchema]]:
        """
        Sugiere mapeos de tabla basado en nombre de hoja y columnas.

        Args:
            worksheet_name: Nombre de la hoja de Excel
            excel_columns: Lista de columnas de Excel

        Returns:
            Lista de tuplas (nombre_tabla, score_confianza, esquema)
        """
        suggestions = []

        for table_key, schema in self.table_schemas.items():
            # Calcular similitud de nombre
            name_similarity = fuzz.ratio(
                worksheet_name.lower(), schema.table_name.lower()) / 100

            # Calcular similitud de columnas
            column_similarity = self._calculate_column_similarity(
                excel_columns, schema.get_column_names())

            # Score combinado (60% columnas, 40% nombre)
            combined_score = (column_similarity * 0.6) + \
                (name_similarity * 0.4)

            suggestions.append((schema.table_name, combined_score, schema))

        # Ordenar por score descendente
        suggestions.sort(key=lambda x: x[1], reverse=True)

        return suggestions[:5]  # Retornar top 5

    def _calculate_column_similarity(self, excel_columns: List[str],
                                     db_columns: List[str]) -> float:
        """
        Calcula la similitud entre listas de columnas.

        Args:
            excel_columns: Columnas de Excel
            db_columns: Columnas de base de datos

        Returns:
            Score de similitud (0.0 - 1.0)
        """
        if not excel_columns or not db_columns:
            return 0.0

        total_score = 0
        matched_columns = 0

        for excel_col in excel_columns:
            best_match_score = 0

            for db_col in db_columns:
                # Calcular diferentes tipos de similitud
                ratio_score = fuzz.ratio(excel_col.lower(), db_col.lower())
                partial_score = fuzz.partial_ratio(
                    excel_col.lower(), db_col.lower())
                token_sort_score = fuzz.token_sort_ratio(
                    excel_col.lower(), db_col.lower())

                # Promedio ponderado
                combined_score = (ratio_score * 0.5 +
                                  partial_score * 0.3 + token_sort_score * 0.2)

                if combined_score > best_match_score:
                    best_match_score = combined_score

            total_score += best_match_score
            if best_match_score > 70:  # Umbral para considerar como match
                matched_columns += 1

        # Score basado en promedio y porcentaje de columnas mapeadas
        avg_score = total_score / len(excel_columns) / 100
        match_ratio = matched_columns / len(excel_columns)

        return (avg_score * 0.7) + (match_ratio * 0.3)

    def create_column_mappings(self, excel_columns: List[str],
                               table_schema: TableSchema,
                               fuzzy_threshold: float = 0.8) -> List[ColumnMapping]:
        """
        Crea mapeos de columnas entre Excel y tabla de base de datos.

        Args:
            excel_columns: Lista de columnas de Excel
            table_schema: Esquema de la tabla de destino
            fuzzy_threshold: Umbral para coincidencia difusa

        Returns:
            Lista de mapeos de columnas
        """
        mappings = []
        db_columns = table_schema.get_column_names()
        used_db_columns = set()

        for i, excel_col in enumerate(excel_columns):
            best_match = None
            best_score = 0
            best_column_info = None

            # Buscar la mejor coincidencia
            for db_col in db_columns:
                if db_col in used_db_columns:
                    continue

                # Calcular score de similitud
                ratio_score = fuzz.ratio(excel_col.lower(), db_col.lower())
                partial_score = fuzz.partial_ratio(
                    excel_col.lower(), db_col.lower())
                token_sort_score = fuzz.token_sort_ratio(
                    excel_col.lower(), db_col.lower())
                token_set_score = fuzz.token_set_ratio(
                    excel_col.lower(), db_col.lower())

                # Promedio ponderado
                combined_score = (ratio_score * 0.4 +
                                  partial_score * 0.2 +
                                  token_sort_score * 0.2 +
                                  token_set_score * 0.2)

                if combined_score > best_score:
                    best_score = combined_score
                    best_match = db_col
                    best_column_info = table_schema.get_column_info(db_col)

            # Crear mapeo
            if best_match and best_score >= (fuzzy_threshold * 100):
                mapping = ColumnMapping(
                    excel_column=excel_col,
                    db_column=best_match,
                    excel_index=i,
                    data_type=best_column_info['data_type'] if best_column_info else DataType.STRING,
                    max_length=best_column_info['max_length'] if best_column_info else None,
                    is_nullable=best_column_info['is_nullable'] if best_column_info else True,
                    confidence_score=best_score / 100
                )
                used_db_columns.add(best_match)
            else:
                # Mapeo con baja confianza
                mapping = ColumnMapping(
                    excel_column=excel_col,
                    db_column=excel_col,  # Usar nombre original
                    excel_index=i,
                    data_type=DataType.STRING,  # Tipo por defecto
                    confidence_score=0.0
                )
                mapping.validation_errors.append(
                    f"No se encontró coincidencia para '{excel_col}' (mejor match: {best_score:.1f}%)"
                )

            mappings.append(mapping)

        return mappings

    def validate_mapping(self, worksheet_mapping: WorksheetMapping) -> Dict[str, Any]:
        """
        Valida un mapeo de hoja de trabajo.

        Args:
            worksheet_mapping: Mapeo a validar

        Returns:
            Resultado de validación
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'suggestions': []
        }

        # Obtener esquema de tabla
        table_schema = self.get_table_schema(
            worksheet_mapping.table_name,
            worksheet_mapping.schema_name
        )

        if not table_schema:
            validation_result['is_valid'] = False
            validation_result['errors'].append(
                f"Tabla '{worksheet_mapping.schema_name}.{worksheet_mapping.table_name}' no encontrada"
            )
            return validation_result

        # Validar mapeos de columnas
        mapped_db_columns = {
            m.db_column for m in worksheet_mapping.column_mappings}
        required_columns = set(table_schema.not_null_columns)

        # Verificar columnas requeridas faltantes
        missing_required = required_columns - mapped_db_columns
        for col in missing_required:
            # Verificar si tiene valor por defecto
            col_info = table_schema.get_column_info(col)
            if not col_info or not col_info.get('default_value'):
                validation_result['errors'].append(
                    f"Columna requerida '{col}' no está mapeada y no tiene valor por defecto"
                )
                validation_result['is_valid'] = False

        # Verificar mapeos con baja confianza
        low_confidence_mappings = [
            m for m in worksheet_mapping.column_mappings
            if m.confidence_score < 0.7
        ]

        for mapping in low_confidence_mappings:
            validation_result['warnings'].append(
                f"Mapeo de baja confianza: '{mapping.excel_column}' -> '{mapping.db_column}' "
                f"({mapping.confidence_score:.1%})"
            )

        # Sugerir mejoras
        if low_confidence_mappings:
            validation_result['suggestions'].append(
                "Revisar manualmente los mapeos de baja confianza"
            )

        # Verificar tipos de datos incompatibles
        for mapping in worksheet_mapping.column_mappings:
            db_col_info = table_schema.get_column_info(mapping.db_column)
            if db_col_info and db_col_info['data_type'] != mapping.data_type:
                validation_result['warnings'].append(
                    f"Posible incompatibilidad de tipos: '{mapping.excel_column}' "
                    f"({mapping.data_type.value}) -> '{mapping.db_column}' "
                    f"({db_col_info['data_type'].value})"
                )

        return validation_result

    def get_table_constraints(self, table_name: str, schema_name: str = None) -> Dict[str, Any]:
        """
        Obtiene las restricciones de una tabla para validación.

        Args:
            table_name: Nombre de la tabla
            schema_name: Nombre del esquema

        Returns:
            Diccionario con restricciones de la tabla
        """
        table_schema = self.get_table_schema(table_name, schema_name)

        if not table_schema:
            return {}

        return {
            'not_null_columns': table_schema.not_null_columns,
            'unique_columns': table_schema.unique_constraints,
            'primary_keys': table_schema.primary_keys,
            'foreign_keys': table_schema.foreign_keys
        }

    def refresh_schemas(self):
        """Recarga los esquemas de tablas desde la base de datos."""
        self.table_schemas.clear()
        self._load_table_schemas()
        self.logger.info("Esquemas de tablas actualizados")
