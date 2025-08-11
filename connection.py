"""
Módulo de Conexión a Base de Datos
Proporciona funcionalidad para conectarse a SQL Server de forma segura.

Autor: Manus AI
Fecha: 2025-01-08
"""

import pyodbc
import logging
from typing import Optional, Dict, Any, Tuple
from contextlib import contextmanager
import hashlib
import secrets
from datetime import datetime


class DatabaseConnection:
    """
    Clase para manejar conexiones seguras a SQL Server.
    """
    
    def __init__(self, server: str, database: str, username: str = None, password: str = None, 
                 trusted_connection: bool = False, driver: str = "ODBC Driver 17 for SQL Server"):
        """
        Inicializa la conexión a la base de datos.
        
        Args:
            server: Nombre del servidor SQL Server
            database: Nombre de la base de datos
            username: Usuario de SQL Server (opcional si se usa autenticación Windows)
            password: Contraseña (opcional si se usa autenticación Windows)
            trusted_connection: True para usar autenticación Windows
            driver: Driver ODBC a utilizar
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.driver = driver
        self._connection_string = self._build_connection_string()
        
        # Configurar logging
        self.logger = logging.getLogger(__name__)
        
    def _build_connection_string(self) -> str:
        """
        Construye la cadena de conexión basada en los parámetros proporcionados.
        
        Returns:
            Cadena de conexión ODBC
        """
        try:
            # Componentes básicos de la conexión
            connection_parts = [
                f"DRIVER={{{self.driver}}}",
                f"SERVER={self.server}",
                f"DATABASE={self.database}",
                "Encrypt=yes",
                "TrustServerCertificate=yes",
                "Connection Timeout=30",
                "Command Timeout=30"
            ]
            
            # Configurar autenticación
            if self.trusted_connection:
                connection_parts.append("Trusted_Connection=yes")
                self.logger.debug("Configurando autenticación de Windows")
            else:
                if not self.username or not self.password:
                    raise ValueError("Usuario y contraseña son requeridos para autenticación SQL")
                connection_parts.extend([
                    f"UID={self.username}",
                    f"PWD={self.password}"
                ])
                self.logger.debug("Configurando autenticación SQL Server")
            
            connection_string = ";".join(connection_parts)
            self.logger.debug(f"Cadena de conexión construida para servidor: {self.server}")
            
            return connection_string
            
        except Exception as e:
            self.logger.error(f"Error construyendo cadena de conexión: {str(e)}")
            raise
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Prueba la conexión a la base de datos.
        
        Returns:
            Tupla (éxito, mensaje_error)
        """
        try:
            with pyodbc.connect(self._connection_string, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                self.logger.info("Conexión a base de datos exitosa")
                return True, None
        except Exception as e:
            error_msg = f"Error de conexión: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    @contextmanager
    def get_connection(self):
        """
        Context manager para obtener una conexión a la base de datos.
        
        Yields:
            Conexión pyodbc
        """
        conn = None
        try:
            conn = pyodbc.connect(self._connection_string)
            yield conn
        except Exception as e:
            self.logger.error(f"Error al obtener conexión: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = None) -> list:
        """
        Ejecuta una consulta SELECT y retorna los resultados.
        
        Args:
            query: Consulta SQL
            params: Parámetros para la consulta
            
        Returns:
            Lista de resultados
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                columns = [column[0] for column in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                return results
        except Exception as e:
            self.logger.error(f"Error ejecutando consulta: {str(e)}")
            raise
    
    def execute_non_query(self, query: str, params: tuple = None) -> int:
        """
        Ejecuta una consulta que no retorna resultados (INSERT, UPDATE, DELETE).
        
        Args:
            query: Consulta SQL
            params: Parámetros para la consulta
            
        Returns:
            Número de filas afectadas
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                rows_affected = cursor.rowcount
                conn.commit()
                return rows_affected
        except Exception as e:
            self.logger.error(f"Error ejecutando comando: {str(e)}")
            raise
    
    def execute_stored_procedure(self, proc_name: str, params: dict = None) -> Dict[str, Any]:
        """
        Ejecuta un procedimiento almacenado.
        
        Args:
            proc_name: Nombre del procedimiento almacenado
            params: Diccionario con parámetros de entrada y salida
            
        Returns:
            Diccionario con parámetros de salida y resultados
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Construir la llamada al procedimiento
                if params:
                    param_placeholders = ', '.join(['?' for _ in params.values()])
                    call = f"EXEC {proc_name} {param_placeholders}"
                    cursor.execute(call, list(params.values()))
                else:
                    cursor.execute(f"EXEC {proc_name}")
                
                # Obtener resultados si los hay
                results = []
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                
                conn.commit()
                return {'results': results, 'rowcount': cursor.rowcount}
                
        except Exception as e:
            self.logger.error(f"Error ejecutando procedimiento {proc_name}: {str(e)}")
            raise


class PasswordManager:
    """
    Clase para manejar el hash y validación de contraseñas de forma segura.
    """
    
    @staticmethod
    def generate_salt() -> str:
        """
        Genera un salt aleatorio para el hash de contraseñas.
        
        Returns:
            Salt en formato hexadecimal
        """
        return secrets.token_hex(32)
    
    @staticmethod
    def hash_password(password: str, salt: str) -> str:
        """
        Genera el hash de una contraseña usando SHA-256 y salt.
        
        Args:
            password: Contraseña en texto plano
            salt: Salt para el hash
            
        Returns:
            Hash de la contraseña
        """
        # En producción, usar bcrypt o argon2 en lugar de SHA-256
        password_bytes = password.encode('utf-8')
        salt_bytes = salt.encode('utf-8')
        hash_obj = hashlib.sha256(password_bytes + salt_bytes)
        return hash_obj.hexdigest()
    
    @staticmethod
    def verify_password(password: str, salt: str, stored_hash: str) -> bool:
        """
        Verifica si una contraseña coincide con el hash almacenado.
        
        Args:
            password: Contraseña en texto plano
            salt: Salt usado para el hash
            stored_hash: Hash almacenado en la base de datos
            
        Returns:
            True si la contraseña es correcta
        """
        computed_hash = PasswordManager.hash_password(password, salt)
        return computed_hash == stored_hash


class AuthenticationManager:
    """
    Clase para manejar la autenticación de usuarios.
    """
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Inicializa el gestor de autenticación.
        
        Args:
            db_connection: Instancia de DatabaseConnection
        """
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
    
    def authenticate_user(self, username: str, password: str, ip_address: str = None, 
                         user_agent: str = None) -> Dict[str, Any]:
        """
        Autentica un usuario contra la base de datos.
        
        Args:
            username: Nombre de usuario
            password: Contraseña
            ip_address: Dirección IP del cliente
            user_agent: User agent del cliente
            
        Returns:
            Diccionario con resultado de autenticación
        """
        try:
            # Obtener información del usuario
            user_query = """
                SELECT UserID, Username, PasswordHash, Salt, IsActive, IsLocked, 
                       FailedLoginAttempts, LockoutTime
                FROM Security.Users 
                WHERE Username = ?
            """
            
            users = self.db.execute_query(user_query, (username,))
            
            if not users:
                return {
                    'success': False,
                    'message': 'Usuario no encontrado',
                    'user_id': None,
                    'session_id': None
                }
            
            user = users[0]
            
            # Verificar si el usuario está activo
            if not user['IsActive']:
                return {
                    'success': False,
                    'message': 'Usuario inactivo',
                    'user_id': user['UserID'],
                    'session_id': None
                }
            
            # Verificar si el usuario está bloqueado
            if user['IsLocked'] and user['LockoutTime'] and user['LockoutTime'] > datetime.now():
                return {
                    'success': False,
                    'message': 'Usuario bloqueado temporalmente',
                    'user_id': user['UserID'],
                    'session_id': None
                }
            
            # Verificar contraseña
            if PasswordManager.verify_password(password, user['Salt'], user['PasswordHash']):
                # Autenticación exitosa - llamar al procedimiento almacenado
                try:
                    password_hash = PasswordManager.hash_password(password, user['Salt'])
                    
                    # Usar el procedimiento almacenado para manejar la autenticación
                    with self.db.get_connection() as conn:
                        cursor = conn.cursor()
                        
                        # Declarar variables de salida
                        cursor.execute("""
                            DECLARE @AuthResult INT, @UserID INT, @SessionID UNIQUEIDENTIFIER, @ErrorMessage NVARCHAR(255);
                            EXEC Security.sp_AuthenticateUser 
                                @Username = ?, 
                                @PasswordHash = ?, 
                                @IPAddress = ?, 
                                @UserAgent = ?,
                                @AuthResult = @AuthResult OUTPUT,
                                @UserID = @UserID OUTPUT,
                                @SessionID = @SessionID OUTPUT,
                                @ErrorMessage = @ErrorMessage OUTPUT;
                            SELECT @AuthResult as AuthResult, @UserID as UserID, 
                                   @SessionID as SessionID, @ErrorMessage as ErrorMessage;
                        """, (username, password_hash, ip_address, user_agent))
                        
                        result = cursor.fetchone()
                        conn.commit()
                        
                        if result.AuthResult == 1:  # Éxito
                            return {
                                'success': True,
                                'message': 'Autenticación exitosa',
                                'user_id': result.UserID,
                                'session_id': str(result.SessionID),
                                'username': username
                            }
                        elif result.AuthResult == 2:  # Usuario bloqueado
                            return {
                                'success': False,
                                'message': 'Usuario bloqueado por múltiples intentos fallidos',
                                'user_id': result.UserID,
                                'session_id': None
                            }
                        else:  # Fallo general
                            return {
                                'success': False,
                                'message': result.ErrorMessage or 'Error de autenticación',
                                'user_id': result.UserID,
                                'session_id': None
                            }
                            
                except Exception as e:
                    self.logger.error(f"Error en procedimiento de autenticación: {str(e)}")
                    return {
                        'success': False,
                        'message': 'Error interno de autenticación',
                        'user_id': user['UserID'],
                        'session_id': None
                    }
            else:
                # Contraseña incorrecta - incrementar intentos fallidos
                self._handle_failed_login(user['UserID'])
                return {
                    'success': False,
                    'message': 'Credenciales inválidas',
                    'user_id': user['UserID'],
                    'session_id': None
                }
                
        except Exception as e:
            self.logger.error(f"Error en autenticación: {str(e)}")
            return {
                'success': False,
                'message': 'Error interno del sistema',
                'user_id': None,
                'session_id': None
            }
    
    def _handle_failed_login(self, user_id: int):
        """
        Maneja los intentos de login fallidos.
        
        Args:
            user_id: ID del usuario
        """
        try:
            # Incrementar intentos fallidos
            update_query = """
                UPDATE Security.Users 
                SET FailedLoginAttempts = FailedLoginAttempts + 1,
                    LastFailedLogin = GETDATE()
                WHERE UserID = ?
            """
            self.db.execute_non_query(update_query, (user_id,))
            
            # Verificar si debe bloquearse el usuario
            user_query = "SELECT FailedLoginAttempts FROM Security.Users WHERE UserID = ?"
            users = self.db.execute_query(user_query, (user_id,))
            
            if users and users[0]['FailedLoginAttempts'] >= 3:
                # Bloquear usuario por 15 minutos
                lockout_query = """
                    UPDATE Security.Users 
                    SET IsLocked = 1, LockoutTime = DATEADD(MINUTE, 15, GETDATE())
                    WHERE UserID = ?
                """
                self.db.execute_non_query(lockout_query, (user_id,))
                self.logger.warning(f"Usuario {user_id} bloqueado por múltiples intentos fallidos")
                
        except Exception as e:
            self.logger.error(f"Error manejando login fallido: {str(e)}")
    
    def logout_user(self, session_id: str) -> bool:
        """
        Cierra la sesión de un usuario.
        
        Args:
            session_id: ID de la sesión
            
        Returns:
            True si el logout fue exitoso
        """
        try:
            # Marcar sesión como terminada
            update_query = """
                UPDATE Security.UserSessions 
                SET EndTime = GETDATE(), IsActive = 0
                WHERE SessionID = ?
            """
            rows_affected = self.db.execute_non_query(update_query, (session_id,))
            
            if rows_affected > 0:
                self.logger.info(f"Sesión {session_id} cerrada exitosamente")
                return True
            else:
                self.logger.warning(f"No se encontró sesión activa: {session_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error en logout: {str(e)}")
            return False
    
    def validate_session(self, session_id: str) -> Dict[str, Any]:
        """
        Valida si una sesión está activa.
        
        Args:
            session_id: ID de la sesión
            
        Returns:
            Diccionario con información de la sesión
        """
        try:
            session_query = """
                SELECT s.SessionID, s.UserID, u.Username, s.StartTime, s.LastActivity
                FROM Security.UserSessions s
                INNER JOIN Security.Users u ON s.UserID = u.UserID
                WHERE s.SessionID = ? AND s.IsActive = 1
                AND s.StartTime > DATEADD(HOUR, -24, GETDATE())
            """
            
            sessions = self.db.execute_query(session_query, (session_id,))
            
            if sessions:
                session = sessions[0]
                
                # Actualizar última actividad
                update_query = """
                    UPDATE Security.UserSessions 
                    SET LastActivity = GETDATE()
                    WHERE SessionID = ?
                """
                self.db.execute_non_query(update_query, (session_id,))
                
                return {
                    'valid': True,
                    'user_id': session['UserID'],
                    'username': session['Username'],
                    'session_start': session['StartTime'],
                    'last_activity': session['LastActivity']
                }
            else:
                return {'valid': False, 'message': 'Sesión inválida o expirada'}
                
        except Exception as e:
            self.logger.error(f"Error validando sesión: {str(e)}")
            return {'valid': False, 'message': 'Error interno del sistema'}

