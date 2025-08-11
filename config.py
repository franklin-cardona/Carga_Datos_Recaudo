"""
Configuración de la Aplicación
Contiene todas las configuraciones y constantes de la aplicación.

Autor: Manus AI
Fecha: 2025-01-08
"""

import os
import logging
from typing import Dict, Any


class Config:
    """
    Clase de configuración principal de la aplicación.
    """
    
    # Configuración de base de datos
    DATABASE_CONFIG = {
        'server': os.getenv('SQL_SERVER', 'localhost'),
        'database': os.getenv('SQL_DATABASE', 'ExcelSQLIntegration'),
        'username': os.getenv('SQL_USERNAME', None),
        'password': os.getenv('SQL_PASSWORD', None),
        'trusted_connection': os.getenv('SQL_TRUSTED_CONNECTION', 'True').lower() == 'true',
        'driver': os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server'),
        'connection_timeout': int(os.getenv('SQL_CONNECTION_TIMEOUT', '30')),
        'command_timeout': int(os.getenv('SQL_COMMAND_TIMEOUT', '300'))
    }
    
    # Configuración de logging
    LOGGING_CONFIG = {
        'level': getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper()),
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'file_path': os.getenv('LOG_FILE_PATH', 'logs/application.log'),
        'max_file_size': int(os.getenv('LOG_MAX_FILE_SIZE', '10485760')),  # 10MB
        'backup_count': int(os.getenv('LOG_BACKUP_COUNT', '5'))
    }
    
    # Configuración de seguridad
    SECURITY_CONFIG = {
        'max_login_attempts': int(os.getenv('MAX_LOGIN_ATTEMPTS', '3')),
        'lockout_duration_minutes': int(os.getenv('LOCKOUT_DURATION_MINUTES', '15')),
        'session_timeout_minutes': int(os.getenv('SESSION_TIMEOUT_MINUTES', '60')),
        'password_min_length': int(os.getenv('PASSWORD_MIN_LENGTH', '8')),
        'require_password_complexity': os.getenv('REQUIRE_PASSWORD_COMPLEXITY', 'True').lower() == 'true'
    }
    
    # Configuración de Excel
    EXCEL_CONFIG = {
        'max_file_size_mb': int(os.getenv('EXCEL_MAX_FILE_SIZE_MB', '50')),
        'supported_extensions': ['.xlsx', '.xls', '.xlsm'],
        'max_rows_per_batch': int(os.getenv('EXCEL_MAX_ROWS_PER_BATCH', '1000')),
        'fuzzy_match_threshold': float(os.getenv('FUZZY_MATCH_THRESHOLD', '0.8')),
        'auto_detect_headers': os.getenv('EXCEL_AUTO_DETECT_HEADERS', 'True').lower() == 'true'
    }
    
    # Configuración de UI
    UI_CONFIG = {
        'theme': os.getenv('UI_THEME', 'clam'),
        'window_width': int(os.getenv('UI_WINDOW_WIDTH', '1024')),
        'window_height': int(os.getenv('UI_WINDOW_HEIGHT', '768')),
        'font_family': os.getenv('UI_FONT_FAMILY', 'Arial'),
        'font_size': int(os.getenv('UI_FONT_SIZE', '10'))
    }
    
    # Configuración de auditoría
    AUDIT_CONFIG = {
        'enable_detailed_logging': os.getenv('AUDIT_DETAILED_LOGGING', 'True').lower() == 'true',
        'log_data_changes': os.getenv('AUDIT_LOG_DATA_CHANGES', 'True').lower() == 'true',
        'retention_days': int(os.getenv('AUDIT_RETENTION_DAYS', '365')),
        'compress_old_logs': os.getenv('AUDIT_COMPRESS_OLD_LOGS', 'True').lower() == 'true'
    }
    
    # Configuración de rendimiento
    PERFORMANCE_CONFIG = {
        'enable_performance_monitoring': os.getenv('PERF_MONITORING', 'True').lower() == 'true',
        'batch_size': int(os.getenv('PERF_BATCH_SIZE', '1000')),
        'max_memory_usage_mb': int(os.getenv('PERF_MAX_MEMORY_MB', '512')),
        'enable_parallel_processing': os.getenv('PERF_PARALLEL_PROCESSING', 'True').lower() == 'true'
    }
    
    @classmethod
    def get_database_connection_string(cls) -> str:
        """
        Construye la cadena de conexión de base de datos.
        
        Returns:
            Cadena de conexión ODBC
        """
        config = cls.DATABASE_CONFIG
        
        if config['trusted_connection']:
            return (
                f"DRIVER={{{config['driver']}}};"
                f"SERVER={config['server']};"
                f"DATABASE={config['database']};"
                f"Trusted_Connection=yes;"
                f"Connection Timeout={config['connection_timeout']};"
                f"Command Timeout={config['command_timeout']};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"
            )
        else:
            return (
                f"DRIVER={{{config['driver']}}};"
                f"SERVER={config['server']};"
                f"DATABASE={config['database']};"
                f"UID={config['username']};"
                f"PWD={config['password']};"
                f"Connection Timeout={config['connection_timeout']};"
                f"Command Timeout={config['command_timeout']};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"
            )
    
    @classmethod
    def validate_config(cls) -> Dict[str, Any]:
        """
        Valida la configuración actual.
        
        Returns:
            Diccionario con resultado de validación
        """
        errors = []
        warnings = []
        
        # Validar configuración de base de datos
        db_config = cls.DATABASE_CONFIG
        if not db_config['server']:
            errors.append("Servidor de base de datos no configurado")
        
        if not db_config['database']:
            errors.append("Nombre de base de datos no configurado")
        
        if not db_config['trusted_connection']:
            if not db_config['username']:
                errors.append("Usuario de base de datos no configurado")
            if not db_config['password']:
                warnings.append("Contraseña de base de datos no configurada")
        
        # Validar configuración de Excel
        excel_config = cls.EXCEL_CONFIG
        if excel_config['max_file_size_mb'] > 100:
            warnings.append("Tamaño máximo de archivo Excel muy grande (>100MB)")
        
        if excel_config['fuzzy_match_threshold'] < 0.5 or excel_config['fuzzy_match_threshold'] > 1.0:
            errors.append("Umbral de coincidencia difusa debe estar entre 0.5 y 1.0")
        
        # Validar configuración de seguridad
        security_config = cls.SECURITY_CONFIG
        if security_config['max_login_attempts'] < 1:
            errors.append("Máximo de intentos de login debe ser mayor a 0")
        
        if security_config['password_min_length'] < 6:
            warnings.append("Longitud mínima de contraseña muy baja (<6 caracteres)")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    @classmethod
    def setup_logging(cls):
        """Configura el sistema de logging."""
        config = cls.LOGGING_CONFIG
        
        # Crear directorio de logs si no existe
        log_dir = os.path.dirname(config['file_path'])
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Configurar logging
        logging.basicConfig(
            level=config['level'],
            format=config['format'],
            handlers=[
                logging.FileHandler(config['file_path'], encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        # Configurar rotación de archivos de log
        if config['max_file_size'] > 0:
            from logging.handlers import RotatingFileHandler
            
            # Remover handler de archivo existente
            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                if isinstance(handler, logging.FileHandler):
                    root_logger.removeHandler(handler)
            
            # Agregar handler con rotación
            rotating_handler = RotatingFileHandler(
                config['file_path'],
                maxBytes=config['max_file_size'],
                backupCount=config['backup_count'],
                encoding='utf-8'
            )
            rotating_handler.setFormatter(logging.Formatter(config['format']))
            root_logger.addHandler(rotating_handler)


class DevelopmentConfig(Config):
    """Configuración para entorno de desarrollo."""
    
    DATABASE_CONFIG = Config.DATABASE_CONFIG.copy()
    DATABASE_CONFIG.update({
        'server': 'localhost',
        'database': 'ExcelSQLIntegration_Dev'
    })
    
    LOGGING_CONFIG = Config.LOGGING_CONFIG.copy()
    LOGGING_CONFIG.update({
        'level': logging.DEBUG,
        'file_path': 'logs/development.log'
    })


class ProductionConfig(Config):
    """Configuración para entorno de producción."""
    
    LOGGING_CONFIG = Config.LOGGING_CONFIG.copy()
    LOGGING_CONFIG.update({
        'level': logging.WARNING,
        'file_path': 'logs/production.log'
    })
    
    SECURITY_CONFIG = Config.SECURITY_CONFIG.copy()
    SECURITY_CONFIG.update({
        'max_login_attempts': 5,
        'lockout_duration_minutes': 30,
        'require_password_complexity': True
    })


class TestConfig(Config):
    """Configuración para entorno de pruebas."""
    
    DATABASE_CONFIG = Config.DATABASE_CONFIG.copy()
    DATABASE_CONFIG.update({
        'database': 'ExcelSQLIntegration_Test'
    })
    
    LOGGING_CONFIG = Config.LOGGING_CONFIG.copy()
    LOGGING_CONFIG.update({
        'level': logging.DEBUG,
        'file_path': 'logs/test.log'
    })


# Configuración activa basada en variable de entorno
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development').lower()

if ENVIRONMENT == 'production':
    active_config = ProductionConfig
elif ENVIRONMENT == 'test':
    active_config = TestConfig
else:
    active_config = DevelopmentConfig

# Constantes de la aplicación
APP_NAME = "Excel-SQL Integration"
APP_VERSION = "1.0.0"
APP_AUTHOR = "Manus AI"
APP_DESCRIPTION = "Sistema de integración de datos entre Excel y SQL Server"

# Mensajes de la aplicación
MESSAGES = {
    'login_success': 'Autenticación exitosa',
    'login_failed': 'Credenciales inválidas',
    'connection_error': 'Error de conexión a la base de datos',
    'file_not_found': 'Archivo no encontrado',
    'invalid_file_format': 'Formato de archivo no válido',
    'processing_complete': 'Procesamiento completado exitosamente',
    'processing_error': 'Error durante el procesamiento',
    'validation_error': 'Error de validación de datos',
    'permission_denied': 'Permisos insuficientes',
    'session_expired': 'Sesión expirada'
}

