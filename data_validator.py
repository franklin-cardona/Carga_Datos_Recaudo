"""
Validador de Datos
Proporciona funcionalidad avanzada para validar datos de Excel antes de la inserción en base de datos.

Autor: Manus AI
Fecha: 2025-01-08
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Callable, Union
import logging
import re
from datetime import datetime, date
from dataclasses import dataclass
from enum import Enum
from .excel_processor import DataType, ColumnMapping, ValidationResult


class ValidationSeverity(Enum):
    """Severidad de los errores de validación."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ValidationRule:
    """Representa una regla de validación."""
    name: str
    description: str
    validator_func: Callable[[Any], Tuple[bool, Optional[str]]]
    severity: ValidationSeverity = ValidationSeverity.ERROR
    applies_to_types: List[DataType] = None
    
    def __post_init__(self):
        if self.applies_to_types is None:
            self.applies_to_types = list(DataType)


@dataclass
class ValidationIssue:
    """Representa un problema de validación encontrado."""
    row_number: int
    column_name: str
    value: Any
    rule_name: str
    message: str
    severity: ValidationSeverity
    suggested_fix: Optional[str] = None


class BusinessRuleValidator:
    """
    Validador de reglas de negocio personalizables.
    """
    
    def __init__(self):
        """Inicializa el validador de reglas de negocio."""
        self.logger = logging.getLogger(__name__)
        self.rules = {}
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """Configura las reglas de validación por defecto."""
        
        # Regla: Valores requeridos
        self.add_rule(ValidationRule(
            name="required_value",
            description="Verifica que los campos requeridos no estén vacíos",
            validator_func=self._validate_required,
            severity=ValidationSeverity.ERROR
        ))
        
        # Regla: Longitud de string
        self.add_rule(ValidationRule(
            name="string_length",
            description="Verifica que los strings no excedan la longitud máxima",
            validator_func=self._validate_string_length,
            severity=ValidationSeverity.ERROR,
            applies_to_types=[DataType.STRING]
        ))
        
        # Regla: Formato de email
        self.add_rule(ValidationRule(
            name="email_format",
            description="Verifica formato válido de email",
            validator_func=self._validate_email_format,
            severity=ValidationSeverity.WARNING,
            applies_to_types=[DataType.STRING]
        ))
        
        # Regla: Números positivos
        self.add_rule(ValidationRule(
            name="positive_number",
            description="Verifica que los números sean positivos",
            validator_func=self._validate_positive_number,
            severity=ValidationSeverity.WARNING,
            applies_to_types=[DataType.INTEGER, DataType.DECIMAL]
        ))
        
        # Regla: Rango de fechas
        self.add_rule(ValidationRule(
            name="date_range",
            description="Verifica que las fechas estén en un rango válido",
            validator_func=self._validate_date_range,
            severity=ValidationSeverity.WARNING,
            applies_to_types=[DataType.DATE, DataType.DATETIME]
        ))
        
        # Regla: Caracteres especiales
        self.add_rule(ValidationRule(
            name="special_characters",
            description="Detecta caracteres especiales que pueden causar problemas",
            validator_func=self._validate_special_characters,
            severity=ValidationSeverity.INFO,
            applies_to_types=[DataType.STRING]
        ))
        
        # Regla: Duplicados
        self.add_rule(ValidationRule(
            name="duplicates",
            description="Detecta valores duplicados en columnas que deberían ser únicas",
            validator_func=self._validate_duplicates,
            severity=ValidationSeverity.ERROR
        ))
    
    def add_rule(self, rule: ValidationRule):
        """
        Agrega una nueva regla de validación.
        
        Args:
            rule: Regla de validación a agregar
        """
        self.rules[rule.name] = rule
        self.logger.debug(f"Regla de validación agregada: {rule.name}")
    
    def remove_rule(self, rule_name: str):
        """
        Remueve una regla de validación.
        
        Args:
            rule_name: Nombre de la regla a remover
        """
        if rule_name in self.rules:
            del self.rules[rule_name]
            self.logger.debug(f"Regla de validación removida: {rule_name}")
    
    def _validate_required(self, value: Any, **kwargs) -> Tuple[bool, Optional[str]]:
        """Valida que un valor requerido no esté vacío."""
        is_required = kwargs.get('is_required', False)
        
        if not is_required:
            return True, None
        
        if pd.isna(value) or value is None or str(value).strip() == '':
            return False, "Campo requerido no puede estar vacío"
        
        return True, None
    
    def _validate_string_length(self, value: Any, **kwargs) -> Tuple[bool, Optional[str]]:
        """Valida la longitud de un string."""
        if pd.isna(value):
            return True, None
        
        max_length = kwargs.get('max_length')
        if max_length is None:
            return True, None
        
        str_value = str(value)
        if len(str_value) > max_length:
            return False, f"Longitud excede el máximo permitido ({max_length} caracteres)"
        
        return True, None
    
    def _validate_email_format(self, value: Any, **kwargs) -> Tuple[bool, Optional[str]]:
        """Valida formato de email."""
        if pd.isna(value):
            return True, None
        
        column_name = kwargs.get('column_name', '').lower()
        if 'email' not in column_name and 'mail' not in column_name:
            return True, None  # Solo validar si la columna parece ser email
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, str(value)):
            return False, "Formato de email inválido"
        
        return True, None
    
    def _validate_positive_number(self, value: Any, **kwargs) -> Tuple[bool, Optional[str]]:
        """Valida que un número sea positivo."""
        if pd.isna(value):
            return True, None
        
        column_name = kwargs.get('column_name', '').lower()
        # Solo aplicar a columnas que sugieren valores positivos
        positive_indicators = ['price', 'amount', 'quantity', 'count', 'total', 'precio', 'cantidad']
        
        if not any(indicator in column_name for indicator in positive_indicators):
            return True, None
        
        try:
            num_value = float(value)
            if num_value < 0:
                return False, "El valor debería ser positivo"
        except (ValueError, TypeError):
            pass  # Error de tipo se maneja en otra validación
        
        return True, None
    
    def _validate_date_range(self, value: Any, **kwargs) -> Tuple[bool, Optional[str]]:
        """Valida que una fecha esté en un rango razonable."""
        if pd.isna(value):
            return True, None
        
        try:
            if isinstance(value, str):
                date_value = pd.to_datetime(value)
            elif isinstance(value, (datetime, date)):
                date_value = pd.to_datetime(value)
            else:
                return True, None
            
            # Verificar rango razonable (1900 - 2100)
            min_date = pd.to_datetime('1900-01-01')
            max_date = pd.to_datetime('2100-12-31')
            
            if date_value < min_date or date_value > max_date:
                return False, f"Fecha fuera del rango válido (1900-2100)"
            
            # Verificar fechas futuras sospechosas
            if date_value > pd.to_datetime('now') + pd.DateOffset(years=10):
                return False, "Fecha muy lejana en el futuro"
            
        except:
            pass  # Error de conversión se maneja en otra validación
        
        return True, None
    
    def _validate_special_characters(self, value: Any, **kwargs) -> Tuple[bool, Optional[str]]:
        """Detecta caracteres especiales problemáticos."""
        if pd.isna(value):
            return True, None
        
        str_value = str(value)
        
        # Caracteres problemáticos para SQL
        problematic_chars = ["'", '"', ';', '--', '/*', '*/', 'xp_', 'sp_']
        
        for char in problematic_chars:
            if char in str_value:
                return False, f"Contiene caracteres potencialmente problemáticos: {char}"
        
        return True, None
    
    def _validate_duplicates(self, value: Any, **kwargs) -> Tuple[bool, Optional[str]]:
        """Valida duplicados (se maneja a nivel de DataFrame)."""
        # Esta validación se implementa en validate_dataframe
        return True, None


class DataValidator:
    """
    Validador principal de datos con capacidades avanzadas.
    """
    
    def __init__(self):
        """Inicializa el validador de datos."""
        self.logger = logging.getLogger(__name__)
        self.business_validator = BusinessRuleValidator()
        
        # Configuraciones de validación
        self.config = {
            'max_error_rate': 0.1,  # 10% máximo de errores
            'sample_validation_size': 1000,  # Validar muestra para archivos grandes
            'enable_data_profiling': True,
            'enable_business_rules': True
        }
    
    def validate_dataframe(self, df: pd.DataFrame, 
                         column_mappings: List[ColumnMapping],
                         table_constraints: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """
        Valida un DataFrame completo según los mapeos y restricciones definidos.
        
        Args:
            df: DataFrame a validar
            column_mappings: Lista de mapeos de columnas
            table_constraints: Restricciones de la tabla de destino
            
        Returns:
            Resultado de validación detallado
        """
        try:
            issues = []
            processed_rows = len(df)
            
            # Validar estructura básica
            structure_issues = self._validate_structure(df, column_mappings)
            issues.extend(structure_issues)
            
            # Validar tipos de datos
            type_issues = self._validate_data_types(df, column_mappings)
            issues.extend(type_issues)
            
            # Validar restricciones de tabla
            if table_constraints:
                constraint_issues = self._validate_table_constraints(df, column_mappings, table_constraints)
                issues.extend(constraint_issues)
            
            # Validar reglas de negocio
            if self.config['enable_business_rules']:
                business_issues = self._validate_business_rules(df, column_mappings)
                issues.extend(business_issues)
            
            # Validar duplicados
            duplicate_issues = self._validate_duplicates(df, column_mappings)
            issues.extend(duplicate_issues)
            
            # Calcular estadísticas
            error_count = sum(1 for issue in issues if issue.severity == ValidationSeverity.ERROR)
            warning_count = sum(1 for issue in issues if issue.severity == ValidationSeverity.WARNING)
            
            # Determinar si la validación es exitosa
            error_rate = error_count / processed_rows if processed_rows > 0 else 0
            is_valid = error_rate <= self.config['max_error_rate']
            
            # Crear resultado
            result = ValidationResult(
                is_valid=is_valid,
                processed_rows=processed_rows,
                valid_rows=processed_rows - error_count
            )
            
            # Convertir issues a formato de resultado
            for issue in issues:
                if issue.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]:
                    result.errors.append({
                        'row': issue.row_number,
                        'column': issue.column_name,
                        'value': str(issue.value),
                        'error': issue.message,
                        'rule': issue.rule_name,
                        'severity': issue.severity.value,
                        'suggested_fix': issue.suggested_fix
                    })
                else:
                    result.warnings.append({
                        'row': issue.row_number,
                        'column': issue.column_name,
                        'value': str(issue.value),
                        'warning': issue.message,
                        'rule': issue.rule_name,
                        'severity': issue.severity.value,
                        'suggested_fix': issue.suggested_fix
                    })
            
            # Agregar estadísticas de validación
            if error_rate > self.config['max_error_rate']:
                result.warnings.append({
                    'row': 0,
                    'column': 'GENERAL',
                    'value': '',
                    'warning': f'Alta tasa de errores: {error_rate:.1%}',
                    'rule': 'error_rate_check',
                    'severity': ValidationSeverity.WARNING.value,
                    'suggested_fix': 'Revisar mapeo de columnas y calidad de datos'
                })
            
            self.logger.info(f"Validación completada: {processed_rows} filas, "
                           f"{error_count} errores, {warning_count} advertencias")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error en validación: {str(e)}")
            return ValidationResult(
                is_valid=False,
                errors=[{
                    'row': 0,
                    'column': 'GENERAL',
                    'value': '',
                    'error': f'Error de validación: {str(e)}',
                    'rule': 'validation_error',
                    'severity': ValidationSeverity.CRITICAL.value
                }]
            )
    
    def _validate_structure(self, df: pd.DataFrame, 
                          column_mappings: List[ColumnMapping]) -> List[ValidationIssue]:
        """Valida la estructura básica del DataFrame."""
        issues = []
        
        # Verificar columnas faltantes
        expected_columns = {mapping.excel_column for mapping in column_mappings}
        actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        for col in missing_columns:
            issues.append(ValidationIssue(
                row_number=0,
                column_name=col,
                value='',
                rule_name='missing_column',
                message=f"Columna esperada '{col}' no encontrada",
                severity=ValidationSeverity.ERROR,
                suggested_fix="Verificar nombres de columnas en Excel"
            ))
        
        # Verificar columnas extra
        extra_columns = actual_columns - expected_columns
        for col in extra_columns:
            issues.append(ValidationIssue(
                row_number=0,
                column_name=col,
                value='',
                rule_name='extra_column',
                message=f"Columna inesperada '{col}' encontrada",
                severity=ValidationSeverity.INFO,
                suggested_fix="Considerar si debe incluirse en el mapeo"
            ))
        
        return issues
    
    def _validate_data_types(self, df: pd.DataFrame, 
                           column_mappings: List[ColumnMapping]) -> List[ValidationIssue]:
        """Valida los tipos de datos de las columnas."""
        issues = []
        
        for mapping in column_mappings:
            if mapping.excel_column not in df.columns:
                continue
            
            series = df[mapping.excel_column]
            
            for idx, value in series.items():
                if pd.isna(value):
                    continue
                
                error_msg = self._validate_cell_type(value, mapping.data_type)
                if error_msg:
                    issues.append(ValidationIssue(
                        row_number=idx + 2,  # +2 para Excel (1-based + header)
                        column_name=mapping.excel_column,
                        value=value,
                        rule_name='data_type_validation',
                        message=error_msg,
                        severity=ValidationSeverity.ERROR,
                        suggested_fix=f"Convertir a tipo {mapping.data_type.value}"
                    ))
        
        return issues
    
    def _validate_table_constraints(self, df: pd.DataFrame, 
                                  column_mappings: List[ColumnMapping],
                                  constraints: Dict[str, Any]) -> List[ValidationIssue]:
        """Valida restricciones de la tabla de destino."""
        issues = []
        
        # Validar restricciones NOT NULL
        not_null_columns = constraints.get('not_null_columns', [])
        for mapping in column_mappings:
            if mapping.db_column in not_null_columns and mapping.excel_column in df.columns:
                null_rows = df[df[mapping.excel_column].isna()].index
                for idx in null_rows:
                    issues.append(ValidationIssue(
                        row_number=idx + 2,
                        column_name=mapping.excel_column,
                        value='NULL',
                        rule_name='not_null_constraint',
                        message=f"Columna '{mapping.db_column}' no puede ser nula",
                        severity=ValidationSeverity.ERROR,
                        suggested_fix="Proporcionar un valor válido"
                    ))
        
        # Validar restricciones UNIQUE
        unique_columns = constraints.get('unique_columns', [])
        for mapping in column_mappings:
            if mapping.db_column in unique_columns and mapping.excel_column in df.columns:
                duplicates = df[df[mapping.excel_column].duplicated(keep=False)]
                for idx, row in duplicates.iterrows():
                    issues.append(ValidationIssue(
                        row_number=idx + 2,
                        column_name=mapping.excel_column,
                        value=row[mapping.excel_column],
                        rule_name='unique_constraint',
                        message=f"Valor duplicado en columna única '{mapping.db_column}'",
                        severity=ValidationSeverity.ERROR,
                        suggested_fix="Asegurar valores únicos"
                    ))
        
        return issues
    
    def _validate_business_rules(self, df: pd.DataFrame, 
                               column_mappings: List[ColumnMapping]) -> List[ValidationIssue]:
        """Valida reglas de negocio personalizadas."""
        issues = []
        
        for mapping in column_mappings:
            if mapping.excel_column not in df.columns:
                continue
            
            # Aplicar reglas relevantes para el tipo de datos
            applicable_rules = [
                rule for rule in self.business_validator.rules.values()
                if mapping.data_type in rule.applies_to_types
            ]
            
            for idx, value in df[mapping.excel_column].items():
                for rule in applicable_rules:
                    try:
                        is_valid, error_msg = rule.validator_func(
                            value,
                            column_name=mapping.excel_column,
                            max_length=mapping.max_length,
                            is_required=not mapping.is_nullable
                        )
                        
                        if not is_valid and error_msg:
                            issues.append(ValidationIssue(
                                row_number=idx + 2,
                                column_name=mapping.excel_column,
                                value=value,
                                rule_name=rule.name,
                                message=error_msg,
                                severity=rule.severity
                            ))
                    except Exception as e:
                        self.logger.warning(f"Error aplicando regla '{rule.name}': {str(e)}")
        
        return issues
    
    def _validate_duplicates(self, df: pd.DataFrame, 
                           column_mappings: List[ColumnMapping]) -> List[ValidationIssue]:
        """Valida duplicados en columnas que deberían ser únicas."""
        issues = []
        
        # Identificar columnas que probablemente deberían ser únicas
        unique_indicators = ['id', 'code', 'number', 'codigo', 'numero']
        
        for mapping in column_mappings:
            if mapping.excel_column not in df.columns:
                continue
            
            column_lower = mapping.excel_column.lower()
            if any(indicator in column_lower for indicator in unique_indicators):
                duplicates = df[df[mapping.excel_column].duplicated(keep=False)]
                for idx, row in duplicates.iterrows():
                    issues.append(ValidationIssue(
                        row_number=idx + 2,
                        column_name=mapping.excel_column,
                        value=row[mapping.excel_column],
                        rule_name='potential_duplicate',
                        message=f"Posible valor duplicado en columna '{mapping.excel_column}'",
                        severity=ValidationSeverity.WARNING,
                        suggested_fix="Verificar si los duplicados son intencionales"
                    ))
        
        return issues
    
    def _validate_cell_type(self, value: Any, expected_type: DataType) -> Optional[str]:
        """Valida el tipo de una celda individual."""
        if pd.isna(value):
            return None
        
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
                    return "No es un número entero válido"
            
            elif expected_type == DataType.DECIMAL:
                if isinstance(value, (int, float, np.number)):
                    return None
                try:
                    float(str(value).replace(',', ''))
                    return None
                except ValueError:
                    return "No es un número decimal válido"
            
            elif expected_type == DataType.BOOLEAN:
                if isinstance(value, bool):
                    return None
                str_val = str(value).lower().strip()
                boolean_values = {'true', 'false', 'yes', 'no', 'y', 'n', '1', '0'}
                if str_val in boolean_values:
                    return None
                return "No es un valor booleano válido"
            
            elif expected_type in [DataType.DATE, DataType.DATETIME]:
                if isinstance(value, (datetime, date)):
                    return None
                try:
                    pd.to_datetime(value)
                    return None
                except:
                    return "No es una fecha/hora válida"
            
            return None
            
        except Exception as e:
            return f"Error de validación de tipo: {str(e)}"
    
    def generate_validation_report(self, validation_result: ValidationResult) -> str:
        """
        Genera un reporte detallado de validación.
        
        Args:
            validation_result: Resultado de validación
            
        Returns:
            Reporte en formato texto
        """
        report = []
        report.append("=" * 60)
        report.append("REPORTE DE VALIDACIÓN DE DATOS")
        report.append("=" * 60)
        report.append("")
        
        # Resumen
        report.append("RESUMEN:")
        report.append(f"  Filas procesadas: {validation_result.processed_rows}")
        report.append(f"  Filas válidas: {validation_result.valid_rows}")
        report.append(f"  Errores encontrados: {len(validation_result.errors)}")
        report.append(f"  Advertencias: {len(validation_result.warnings)}")
        report.append(f"  Estado: {'VÁLIDO' if validation_result.is_valid else 'INVÁLIDO'}")
        report.append("")
        
        # Errores
        if validation_result.errors:
            report.append("ERRORES:")
            report.append("-" * 40)
            for i, error in enumerate(validation_result.errors[:10], 1):  # Mostrar solo primeros 10
                report.append(f"{i}. Fila {error['row']}, Columna '{error['column']}':")
                report.append(f"   Valor: {error['value']}")
                report.append(f"   Error: {error['error']}")
                if error.get('suggested_fix'):
                    report.append(f"   Sugerencia: {error['suggested_fix']}")
                report.append("")
            
            if len(validation_result.errors) > 10:
                report.append(f"... y {len(validation_result.errors) - 10} errores más")
                report.append("")
        
        # Advertencias
        if validation_result.warnings:
            report.append("ADVERTENCIAS:")
            report.append("-" * 40)
            for i, warning in enumerate(validation_result.warnings[:5], 1):  # Mostrar solo primeras 5
                report.append(f"{i}. Fila {warning['row']}, Columna '{warning['column']}':")
                report.append(f"   Valor: {warning['value']}")
                report.append(f"   Advertencia: {warning['warning']}")
                if warning.get('suggested_fix'):
                    report.append(f"   Sugerencia: {warning['suggested_fix']}")
                report.append("")
            
            if len(validation_result.warnings) > 5:
                report.append(f"... y {len(validation_result.warnings) - 5} advertencias más")
                report.append("")
        
        report.append("=" * 60)
        
        return "\n".join(report)

