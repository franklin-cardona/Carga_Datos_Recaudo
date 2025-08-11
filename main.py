"""
Aplicación Principal - Excel-SQL Integration
Punto de entrada principal para la aplicación de integración de datos.

Autor: Manus AI
Fecha: 2025-01-08
"""

from login_ui import LoginWindow
from connection import DatabaseConnection, AuthenticationManager
from config import active_config, APP_NAME, APP_VERSION, MESSAGES
import sys
import os
import logging
import tkinter as tk
from tkinter import messagebox
from typing import Dict, Any

# Agregar el directorio src al path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class ExcelSQLIntegrationApp:
    """
    Aplicación principal de integración Excel-SQL Server.
    """

    def __init__(self):
        """Inicializa la aplicación."""
        self.db_connection = None
        self.auth_manager = None
        self.current_user = None
        self.current_session = None
        self.logger = None

        # Configurar logging
        self._setup_logging()

        # Validar configuración
        self._validate_configuration()

        # Inicializar componentes
        self._initialize_components()

    def _setup_logging(self):
        """Configura el sistema de logging."""
        try:
            active_config.setup_logging()
            self.logger = logging.getLogger(__name__)
            self.logger.info(f"Iniciando {APP_NAME} v{APP_VERSION}")
        except Exception as e:
            print(f"Error configurando logging: {e}")
            sys.exit(1)

    def _validate_configuration(self):
        """Valida la configuración de la aplicación."""
        try:
            validation_result = active_config.validate_config()

            if not validation_result['valid']:
                error_msg = "Errores de configuración:\n" + \
                    "\n".join(validation_result['errors'])
                self.logger.error(error_msg)
                messagebox.showerror("Error de Configuración", error_msg)
                sys.exit(1)

            if validation_result['warnings']:
                warning_msg = "Advertencias de configuración:\n" + \
                    "\n".join(validation_result['warnings'])
                self.logger.warning(warning_msg)
                messagebox.showwarning(
                    "Advertencias de Configuración", warning_msg)

            self.logger.info("Configuración validada exitosamente")

        except Exception as e:
            self.logger.error(f"Error validando configuración: {e}")
            messagebox.showerror(
                "Error", f"Error validando configuración: {e}")
            sys.exit(1)

    def _initialize_components(self):
        """Inicializa los componentes principales de la aplicación."""
        try:
            # Inicializar conexión a base de datos
            db_config = active_config.DATABASE_CONFIG
            self.db_connection = DatabaseConnection(
                server=db_config['P18PPAD20\\SQLEXPRESS'],
                database=db_config['RECAUDO_DRFS'],
                username=db_config['fraklin.cardona'],
                password=db_config['97606+84939+Daniel*'],
                trusted_connection=db_config['trusted_connection'],
                driver=db_config['driver']
            )

            # Probar conexión
            success, error_msg = self.db_connection.test_connection()
            if not success:
                self.logger.error(
                    f"Error de conexión a base de datos: {error_msg}")
                messagebox.showerror("Error de Conexión",
                                     f"No se pudo conectar a la base de datos:\n{error_msg}")
                sys.exit(1)

            # Inicializar gestor de autenticación
            self.auth_manager = AuthenticationManager(self.db_connection)

            self.logger.info("Componentes inicializados exitosamente")

        except Exception as e:
            self.logger.error(f"Error inicializando componentes: {e}")
            messagebox.showerror("Error de Inicialización",
                                 f"Error inicializando la aplicación:\n{e}")
            sys.exit(1)

    def _on_login_success(self, auth_result: Dict[str, Any]):
        """
        Maneja el login exitoso.

        Args:
            auth_result: Resultado de la autenticación
        """
        try:
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

            # Iniciar aplicación principal
            self._start_main_application()

        except Exception as e:
            self.logger.error(f"Error en login exitoso: {e}")
            messagebox.showerror("Error", f"Error iniciando aplicación: {e}")

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
            # Por ahora, mostrar un mensaje de éxito
            # En las siguientes fases se implementará la interfaz principal
            messagebox.showinfo("Éxito",
                                f"Bienvenido {self.current_user['username']}!\n\n"
                                f"La aplicación principal se implementará en las siguientes fases.\n"
                                f"Por ahora, la autenticación está funcionando correctamente.")

            self.logger.info("Aplicación principal iniciada")

            # Cerrar sesión al salir
            self._logout()

        except Exception as e:
            self.logger.error(f"Error iniciando aplicación principal: {e}")
            messagebox.showerror(
                "Error", f"Error en aplicación principal: {e}")

    def _logout(self):
        """Cierra la sesión del usuario actual."""
        try:
            if self.current_user and self.current_user['session_id']:
                self.auth_manager.logout_user(self.current_user['session_id'])
                self.logger.info(
                    f"Sesión cerrada para usuario: {self.current_user['username']}")

            # Limpiar contexto de usuario
            with self.db_connection.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("EXEC Security.sp_ClearUserContext")
                conn.commit()

            self.current_user = None

        except Exception as e:
            self.logger.error(f"Error en logout: {e}")

    def run(self):
        """Ejecuta la aplicación."""
        try:
            self.logger.info("Iniciando interfaz de login")

            # Crear y mostrar ventana de login
            login_window = LoginWindow(
                on_login_success=self._on_login_success,
                on_login_failed=self._on_login_failed
            )

            # Configurar gestor de autenticación
            login_window.set_auth_manager(self.auth_manager)

            # Mostrar ventana de login
            login_window.show()

            self.logger.info("Aplicación finalizada")

        except Exception as e:
            self.logger.error(f"Error ejecutando aplicación: {e}")
            messagebox.showerror(
                "Error Fatal", f"Error ejecutando aplicación:\n{e}")
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
            self.logger.error(f"Error limpiando recursos: {e}")


def main():
    """Función principal de la aplicación."""
    try:
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
