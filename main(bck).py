"""
Aplicación Principal - Excel-SQL Integration
Punto de entrada principal para la aplicación de integración de datos con configuración dinámica de conexión.

Autor: FRANKLIN ANDRES CARDONA YARA
Fecha: 2025-01-08
"""

from connection_dialog import ConnectionDialog, ConnectionConfigManager
from login_ui import LoginWindow
from connection import DatabaseConnection, AuthenticationManager
from config import active_config, APP_NAME, APP_VERSION, MESSAGES
import sys
import os
import logging
import tkinter as tk
from tkinter import messagebox
from typing import Dict, Any, Optional

# Agregar el directorio src al path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class ExcelSQLIntegrationApp:
    """
    Aplicación principal de integración Excel-SQL Server con configuración dinámica.
    """

    def __init__(self):
        """Inicializa la aplicación."""
        self.db_connection = None
        self.auth_manager = None
        self.current_user = None
        self.current_session = None
        self.logger = None
        self.connection_config = None
        self.config_manager = ConnectionConfigManager()

        # Configurar logging
        self._setup_logging()

        # self.logger.info(f"Iniciando {APP_NAME} v{APP_VERSION}")

    def _setup_logging(self):
        """Configura el sistema de logging."""
        try:
            active_config.setup_logging()
            self.logger = logging.getLogger(__name__)
        except Exception as e:
            print(f"Error configurando logging: {e}")
            sys.exit(1)

    def _show_connection_dialog(self) -> bool:
        """
        Muestra el diálogo de configuración de conexión.

        Returns:
            True si se configuró la conexión exitosamente
        """
        try:
            self.logger.info("Mostrando diálogo de configuración de conexión")

            # Crear diálogo de conexión
            dialog = ConnectionDialog(
                on_connection_success=self._on_connection_configured)

            # Mostrar diálogo
            config = dialog.show()

            if config:
                self.connection_config = config
                return True
            else:
                self.logger.info(
                    "Usuario canceló la configuración de conexión")
                return False

        except Exception as e:
            self.logger.error(f"Error en diálogo de conexión: {str(e)}")
            messagebox.showerror(
                "Error", f"Error en configuración de conexión:\n{str(e)}")
            return False

    def _on_connection_configured(self, config: Dict[str, Any]):
        """
        Callback cuando se configura la conexión exitosamente.

        Args:
            config: Configuración de conexión
        """
        self.connection_config = config
        self.logger.info(
            f"Conexión configurada para servidor: {config['server']}")

    def _initialize_database_connection(self) -> bool:
        """
        Inicializa la conexión a la base de datos con la configuración proporcionada.

        Returns:
            True si la inicialización fue exitosa
        """
        try:
            if not self.connection_config:
                self.logger.error(
                    "No hay configuración de conexión disponible")
                return False

            # Crear conexión a base de datos
            self.db_connection = DatabaseConnection(**self.connection_config)

            # Probar conexión
            success, error_msg = self.db_connection.test_connection()
            if not success:
                self.logger.error(
                    f"Error de conexión a base de datos: {error_msg}")
                messagebox.showerror("Error de Conexión",
                                     f"No se pudo conectar a la base de datos:\n{error_msg}")
                return False

            # Inicializar gestor de autenticación
            self.auth_manager = AuthenticationManager(self.db_connection)

            self.logger.info(
                "Conexión a base de datos inicializada exitosamente")
            return True

        except Exception as e:
            self.logger.error(f"Error inicializando conexión: {str(e)}")
            messagebox.showerror("Error de Inicialización",
                                 f"Error inicializando la conexión:\n{str(e)}")
            return False

    def _check_database_schema(self) -> bool:
        """
        Verifica que el esquema de base de datos esté configurado correctamente.

        Returns:
            True si el esquema está correcto
        """
        try:
            # Verificar que existan las tablas principales
            required_tables = [
                'Security.Users',
                'Security.Roles',
                'Audit.OperationLog',
                'Data.Customers'  # Tabla de ejemplo
            ]

            for table in required_tables:
                schema, table_name = table.split('.')
                query = """
                    SELECT COUNT(*) as table_count
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """

                result = self.db_connection.execute_query(
                    query, (schema, table_name))

                if not result or result[0]['table_count'] == 0:
                    self.logger.warning(f"Tabla {table} no encontrada")

                    # Preguntar al usuario si desea continuar
                    response = messagebox.askyesno(
                        "Esquema Incompleto",
                        f"La tabla {table} no existe en la base de datos.\n\n"
                        f"Esto puede indicar que el esquema no está completamente configurado.\n"
                        f"¿Desea continuar de todos modos?\n\n"
                        f"Nota: Algunas funcionalidades pueden no estar disponibles."
                    )

                    if not response:
                        return False

            self.logger.info("Verificación de esquema completada")
            return True

        except Exception as e:
            self.logger.error(f"Error verificando esquema: {str(e)}")

            # Preguntar si continuar a pesar del error
            response = messagebox.askyesno(
                "Error de Verificación",
                f"No se pudo verificar el esquema de la base de datos:\n{str(e)}\n\n"
                f"¿Desea continuar de todos modos?"
            )

            return response

    def _on_login_success(self, auth_result: Dict[str, Any]):
        """
        Maneja el login exitoso.

        Args:
            auth_result: Resultado de la autenticación
        """
        try:
            self.logger.info(f" usuario:{auth_result['user_id']}")
            self.current_user = {
                'user_id': auth_result['user_id'],
                'username': auth_result['username'],
                'session_id': auth_result['session_id']
            }

            self.logger.info(
                f"Usuario autenticado: {self.current_user['username']}")

            # Establecer contexto de usuario en la base de datos
            with self.db_connection.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("EXEC Security.sp_SetUserContext ?, ?",
                               (self.current_user['username'], self.current_user['session_id']))
                conn.commit()

            # Guardar configuración de conexión si fue exitosa
            self.config_manager.save_config(self.connection_config)

            # Iniciar aplicación principal
            self._start_main_application()

        except Exception as e:
            self.logger.error(f"Error en login exitoso: {str(e)}")
            messagebox.showerror(
                "Error", f"Error iniciando aplicación: {str(e)}")

    def _on_login_failed(self, error_message: str):
        """
        Maneja el login fallido.

        Args:
            error_message: Mensaje de error
        """
        self.logger.warning(f"Login fallido: {error_message}")

    def _start_main_application(self):
        """Inicia la aplicación principal después del login exitoso."""
        try:
            # Por ahora, mostrar información de conexión y usuario
            connection_info = (
                f"Servidor: {self.connection_config['server']}\n"
                f"Base de Datos: {self.connection_config['database']}\n"
                f"Autenticación: {'Windows' if self.connection_config['trusted_connection'] else 'SQL Server'}\n"
                f"Usuario: {self.current_user['username']}"
            )

            messagebox.showinfo("Conexión Exitosa",
                                f"¡Bienvenido {self.current_user['username']}!\n\n"
                                f"Conexión establecida exitosamente:\n\n{connection_info}\n\n"
                                f"La aplicación principal se implementará en las siguientes fases.\n"
                                f"Por ahora, la autenticación y conexión están funcionando correctamente.")

            self.logger.info("Aplicación principal iniciada")

            # Cerrar sesión al salir
            self._logout()

        except Exception as e:
            self.logger.error(
                f"Error iniciando aplicación principal: {str(e)}")
            messagebox.showerror(
                "Error", f"Error en aplicación principal: {str(e)}")

    def _logout(self):
        """Cierra la sesión del usuario actual."""
        try:
            if self.current_user and self.current_user['session_id']:
                self.auth_manager.logout_user(self.current_user['session_id'])
                self.logger.info(
                    f"Sesión cerrada para usuario: {self.current_user['username']}")

            # Limpiar contexto de usuario
            if self.db_connection:
                with self.db_connection.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("EXEC Security.sp_ClearUserContext")
                    conn.commit()

            self.current_user = None

        except Exception as e:
            self.logger.error(f"Error en logout: {str(e)}")

    def _show_login_window(self) -> bool:
        """
        Muestra la ventana de login.

        Returns:
            True si el login fue exitoso
        """
        try:
            self.logger.info("Iniciando interfaz de login")

            # Crear y mostrar ventana de login
            login_window = LoginWindow(
                on_login_success=self._on_login_success,
                on_login_failed=self._on_login_failed
            )

            self.logger.info("prueba voy aqu")

            # Configurar gestor de autenticación
            login_window.set_auth_manager(self.auth_manager)

            # Mostrar ventana de login
            login_window.show()

            # Verificar si el login fue exitoso
            return self.current_user is not None

        except Exception as e:
            self.logger.error(f"Error en ventana de login: {str(e)}")
            messagebox.showerror("Error", f"Error en login:\n{str(e)}")
            return False

    def run(self):
        """Ejecuta la aplicación."""
        try:
            self.logger.info("Iniciando aplicación")

            # Paso 1: Configurar conexión a base de datos
            if not self._show_connection_dialog():
                self.logger.info("Aplicación cancelada por el usuario")
                return

            # Paso 2: Inicializar conexión a base de datos
            if not self._initialize_database_connection():
                self.logger.error(
                    "No se pudo inicializar la conexión a base de datos")
                return

            # Paso 3: Verificar esquema de base de datos
            # if not self._check_database_schema():
            #     self.logger.info(
            #         "Verificación de esquema cancelada por el usuario")
            #     return

            # Paso 4: Mostrar ventana de login
            if not self._show_login_window():
                self.logger.info("Login cancelado por el usuario")
                return

            self.logger.info("Aplicación finalizada exitosamente")

        except KeyboardInterrupt:
            self.logger.info("Aplicación interrumpida por el usuario")
        except Exception as e:
            self.logger.error(f"Error ejecutando aplicación: {str(e)}")
            messagebox.showerror(
                "Error Fatal", f"Error ejecutando aplicación:\n{str(e)}")
        finally:
            # Limpiar recursos
            self._cleanup()

    def _cleanup(self):
        """Limpia los recursos de la aplicación."""
        try:
            if self.current_user:
                self._logout()

            self.logger.info("Recursos limpiados exitosamente")

        except Exception as e:
            self.logger.error(f"Error limpiando recursos: {str(e)}")


def show_welcome_message():
    """Muestra mensaje de bienvenida."""
    try:
        # Crear ventana temporal para el mensaje
        root = tk.Tk()
        root.withdraw()  # Ocultar ventana principal

        welcome_msg = (
            f"Bienvenido a {APP_NAME} v{APP_VERSION}\n\n"
            f"Esta aplicación le permitirá integrar datos de Excel con SQL Server de forma segura.\n\n"
            f"Características principales:\n"
            f"• Conexión configurable a SQL Server\n"
            f"• Autenticación segura (Windows o SQL Server)\n"
            f"• Mapeo automático de columnas con coincidencia difusa\n"
            f"• Validación completa de datos\n"
            f"• Sistema de auditoría integrado\n\n"
            f"A continuación se le solicitará configurar la conexión a la base de datos."
        )

        messagebox.showinfo("Bienvenido", welcome_msg)
        root.destroy()

    except Exception as e:
        print(f"Error mostrando mensaje de bienvenida: {e}")


def main():
    """Función principal de la aplicación."""
    try:
        # Mostrar mensaje de bienvenida
        show_welcome_message()

        # Crear y ejecutar aplicación
        app = ExcelSQLIntegrationApp()
        app.run()

    except KeyboardInterrupt:
        print("\nAplicación interrumpida por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"Error fatal: {e}")
        logging.error(f"Error fatal: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
