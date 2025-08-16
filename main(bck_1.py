"""
Aplicación Principal - Excel-SQL Integration
Punto de entrada principal para la aplicación de integración de datos con configuración dinámica de conexión.

Autor: FRANKLIN ANDRES CARDONA YARA
Fecha: 2025-01-08
"""

import sys
import os
import logging
import tkinter as tk
from tkinter import messagebox
from tkinter import filedialog
from typing import Dict, Any, Optional
import getpass

# Agregar el directorio src al path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Importaciones absolutas para evitar problemas de importación relativa
try:
    from config import active_config, APP_NAME, APP_VERSION, MESSAGES
    from connection import DatabaseConnection
    from connection_dialog import ConnectionDialog, ConnectionConfigManager
    from excel_processor import ExcelProcessor
    from table_mapper import TableMapper

except ImportError as e:
    print(f"Error de importación: {e}")
    print("Asegúrate de ejecutar la aplicación desde el directorio correcto")
    sys.exit(1)


class ExcelSQLIntegrationApp:
    """
    Aplicación principal de integración Excel-SQL Server con configuración dinámica.
    """

    def __init__(self):
        """Inicializa la aplicación."""
        self.db_connection = None
        self.current_user = None
        self.logger = None
        self.connection_config = None
        self.config_manager = ConnectionConfigManager()

        # Configurar logging
        self._setup_logging()

        self.logger.info(f"Iniciando {APP_NAME} v{APP_VERSION}")

    def _setup_logging(self):
        """Configura el sistema de logging."""
        try:
            # Configuración básica de logging si no existe configuración activa
            logging.basicConfig(
                level=logging.INFO,
                # format="""% (asctime)s - %(name)s - %(levelname)s - %(message)s""",
                handlers=[
                    logging.FileHandler('excel_sql_integration.log'),
                    logging.StreamHandler()
                ]
            )
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

            self.logger.info(
                "Conexión a base de datos inicializada exitosamente")
            return True

        except Exception as e:
            self.logger.error(f"Error inicializando conexión: {str(e)}")
            messagebox.showerror("Error de Inicialización",
                                 f"Error inicializando la conexión:\n{str(e)}")
            return False

    def _get_current_user_info(self) -> Dict[str, Any]:
        """
        Obtiene información del usuario actual basado en el tipo de autenticación.

        Returns:
            Diccionario con información del usuario
        """
        try:
            if self.connection_config['trusted_connection']:
                # Autenticación de Windows - obtener usuario del sistema
                windows_user = getpass.getuser()
                computer_name = os.environ.get('COMPUTERNAME', 'UNKNOWN')

                return {'username': f"{computer_name}\\\\{windows_user}",
                        'auth_type': 'Windows',
                        'display_name': windows_user,
                        'is_windows_auth': True
                        }
            else:
                # Autenticación de SQL Server - usar usuario de la configuración
                sql_user = self.connection_config.get('username', 'UNKNOWN')

                return {'username': sql_user,
                        'auth_type': 'SQL Server',
                        'display_name': sql_user,
                        'is_windows_auth': False
                        }

        except Exception as e:
            self.logger.warning(
                f"Error obteniendo información de usuario: {str(e)}")
            return {'username': 'UNKNOWN',
                    'auth_type': 'Unknown',
                    'display_name': 'Usuario Desconocido',
                    'is_windows_auth': False
                    }

    def _check_database_schema(self) -> bool:
        """
        Verifica que el esquema de base de datos esté configurado correctamente.

        Returns:
            True si el esquema está correcto o el usuario decide continuar
        """
        try:
            # Lista de tablas opcionales para verificar
            optional_tables = ['Security.Users',
                               'Security.Roles',
                               'Audit.OperationLog',
                               'Data.Customers'
                               ]

            missing_tables = []

            for table in optional_tables:
                try:
                    schema, table_name = table.split('.')
                    query = """
                        SELECT COUNT(*) as table_count
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    """

                    result = self.db_connection.execute_query(
                        query, (schema, table_name))

                    if not result or result[0]['table_count'] == 0:
                        missing_tables.append(table)

                except Exception as table_error:
                    self.logger.warning(
                        f"Error verificando tabla {table}: {str(table_error)}")
                    missing_tables.append(table)

            if missing_tables:
                self.logger.warning(
                    f"Tablas faltantes: {', '.join(missing_tables)}")

                # Informar al usuario pero permitir continuar
                response = messagebox.askyesno(
                    "Esquema Incompleto",
                    f"Algunas tablas del esquema no se encontraron:\n\n"
                    f"• {chr(10).join(missing_tables)}\n\n"
                    f"Esto puede indicar que el esquema no está completamente configurado.\n"
                    f"La aplicación funcionará con las funcionalidades básicas disponibles.\n\n"
                    f"¿Desea continuar de todos modos?"
                )

                return response
            else:
                self.logger.info(
                    "Verificación de esquema completada - todas las tablas encontradas")
                return True

        except Exception as e:
            self.logger.error(f"Error verificando esquema: {str(e)}")

            # Preguntar si continuar a pesar del error
            response = messagebox.askyesno(
                "Error de Verificación",
                f"No se pudo verificar completamente el esquema de la base de datos:\n{str(e)}\n\n"
                f"¿Desea continuar de todos modos?"
            )

            return response

    def _start_main_application(self):
        """Inicia la aplicación principal después de la configuración exitosa."""
        try:
            # Obtener información del usuario actual
            user_info = self._get_current_user_info()
            self.current_user = user_info

            # Información de conexión para mostrar
            connection_info = (
                f"Servidor: {self.connection_config['server']}\n"
                f"Base de Datos: {self.connection_config['database']}\n"
                f"Autenticación: {user_info['auth_type']}\n"
                f"Usuario: {user_info['display_name']}"
            )

            # Guardar configuración si fue exitosa
            self.config_manager.save_config(self.connection_config)

            # Mostrar información de conexión exitosa
            messagebox.showinfo(
                "Conexión Exitosa",
                f"¡Bienvenido {user_info['display_name']}!\n\n"
                f"Conexión establecida exitosamente:\n\n{connection_info}\n\n"
                f"La aplicación está lista para procesar archivos Excel.\n"
                f"Las funcionalidades principales se implementarán en las siguientes fases."
            )

            self.logger.info(
                f"Aplicación iniciada para usuario: {user_info['username']}")

            # Aquí se iniciará la interfaz principal de la aplicación
            # Por ahora, mostrar mensaje informativo
            self._show_main_interfaz_Principal()

        except Exception as e:
            self.logger.error(
                f"Error iniciando aplicación principal: {str(e)}")
            messagebox.showerror(
                "Error", f"Error en aplicación principal: {str(e)}")

    def _show_main_interfaz_Principal(self):
        """
        Muestra una interfaz principal.
        """
        try:
            # Crear ventana principal temporal
            root = tk.Tk()
            root.title(
                f"{APP_NAME} - Usuario: {self.current_user['display_name']}")
            root.geometry("800x800")
            root.resizable(True, True)

            # Frame principal
            main_frame = tk.Frame(root, padx=20, pady=20)
            main_frame.pack(fill=tk.BOTH, expand=True)

            # Título
            title_label = tk.Label(
                main_frame,
                text=f"{APP_NAME} v{APP_VERSION}",
                font=('Arial', 16, 'bold')
            )
            title_label.pack(pady=(0, 20))

            # Información de conexión
            info_text = (
                f"Conexión Activa:\\n"
                f"Servidor: {self.connection_config['server']}\n"
                f"Base de Datos: {self.connection_config['database']}\n"
                f"Usuario: {self.current_user['display_name']}\n"
                f"Tipo de Autenticación: {self.current_user['auth_type']}\n\n"
                f"Estado: ✓ Conectado y listo para procesar archivos Excel"
            )

            info_label = tk.Label(
                main_frame,
                text=info_text,
                font=('Arial', 10),
                justify=tk.LEFT,
                bg='lightgreen',
                padx=10,
                pady=10
            )
            info_label.pack(fill=tk.X, pady=(0, 20))

            # Mensaje de desarrollo
            dev_message = (
                "🚧 Interfaz de selección de archivos 🚧\n\n"
                # "La conexión a la base de datos está funcionando correctamente.\n"
                # "Las siguientes funcionalidades se implementarán en las próximas fases:\n\n"
                # "• Interfaz de selección de archivos Excel\n"
                # "• Mapeo automático de columnas\n"
                # "• Validación de datos\n"
                # "• Procesamiento por lotes\n"
                # "• Sistema de auditoría\n"
                # "• Reportes de resultados"
            )

            dev_label = tk.Label(
                main_frame,
                text=dev_message,
                font=('Arial', 9),
                justify=tk.LEFT,
                bg='lightyellow',
                padx=10,
                pady=10
            )
            dev_label.pack(fill=tk.BOTH, expand=True, pady=(0, 20))

            # crear objeto para abrir ventana de seleccion de archivos de excel
            # y procesarlos
            # from excel_file_selector import ExcelFileSelector
            self.file_selector = ExcelProcessor()
            archivo = filedialog.askopenfilename(
                filetypes=[("Excel files", "*.xlsx *.xls")])
            if self.file_selector.validate_file(archivo)[0]:
                base = TableMapper(self.db_connection)
                self.logger.info(
                    # f"prueba de lectura: {self.file_selector.process_excel_file(archivo,)}")
                    f"prueba de lectura: {base}")

            # Botón de cerrar
            close_button = tk.Button(
                main_frame,
                text="Cerrar Aplicación",
                command=root.destroy,
                font=('Arial', 10),
                bg='lightcoral'
            )
            close_button.pack()

            # Centrar ventana
            root.update_idletasks()
            x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
            y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
            root.geometry(f"+{x}+{y}")

            # Mostrar ventana
            root.mainloop()

        except Exception as e:
            self.logger.error(
                f"Error mostrando interfaz Principal: {str(e)}")

    # def _show_main_interface_placeholder(self):
    #     """
    #     Muestra una interfaz placeholder hasta que se implemente la interfaz principal.
    #     """
    #     try:
    #         # Crear ventana principal temporal
    #         root = tk.Tk()
    #         root.title(
    #             f"{APP_NAME} - Usuario: {self.current_user['display_name']}")
    #         root.geometry("800x800")
    #         root.resizable(True, True)

    #         # Frame principal
    #         main_frame = tk.Frame(root, padx=20, pady=20)
    #         main_frame.pack(fill=tk.BOTH, expand=True)

    #         # Título
    #         title_label = tk.Label(
    #             main_frame,
    #             text=f"{APP_NAME} v{APP_VERSION}",
    #             font=('Arial', 16, 'bold')
    #         )
    #         title_label.pack(pady=(0, 20))

    #         # Información de conexión
    #         info_text = (
    #             f"Conexión Activa:\\n"
    #             f"Servidor: {self.connection_config['server']}\n"
    #             f"Base de Datos: {self.connection_config['database']}\n"
    #             f"Usuario: {self.current_user['display_name']}\n"
    #             f"Tipo de Autenticación: {self.current_user['auth_type']}\n\n"
    #             f"Estado: ✓ Conectado y listo para procesar archivos Excel"
    #         )

    #         info_label = tk.Label(
    #             main_frame,
    #             text=info_text,
    #             font=('Arial', 10),
    #             justify=tk.LEFT,
    #             bg='lightgreen',
    #             padx=10,
    #             pady=10
    #         )
    #         info_label.pack(fill=tk.X, pady=(0, 20))

    #         # Mensaje de desarrollo
    #         dev_message = (
    #             "🚧 APLICACIÓN EN DESARROLLO 🚧\n\n"
    #             "La conexión a la base de datos está funcionando correctamente.\n"
    #             "Las siguientes funcionalidades se implementarán en las próximas fases:\n\n"
    #             "• Interfaz de selección de archivos Excel\n"
    #             "• Mapeo automático de columnas\n"
    #             "• Validación de datos\n"
    #             "• Procesamiento por lotes\n"
    #             "• Sistema de auditoría\n"
    #             "• Reportes de resultados"
    #         )

    #         dev_label = tk.Label(
    #             main_frame,
    #             text=dev_message,
    #             font=('Arial', 9),
    #             justify=tk.LEFT,
    #             bg='lightyellow',
    #             padx=10,
    #             pady=10
    #         )
    #         dev_label.pack(fill=tk.BOTH, expand=True, pady=(0, 20))

    #         # Botón de cerrar
    #         close_button = tk.Button(
    #             main_frame,
    #             text="Cerrar Aplicación",
    #             command=root.destroy,
    #             font=('Arial', 10),
    #             bg='lightcoral'
    #         )
    #         close_button.pack()

    #         # Centrar ventana
    #         root.update_idletasks()
    #         x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    #         y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    #         root.geometry(f"+{x}+{y}")

    #         # Mostrar ventana
    #         root.mainloop()

    #     except Exception as e:
    #         self.logger.error(
    #             f"Error mostrando interfaz placeholder: {str(e)}")

    def run(self):
        """
        Ejecuta la aplicación.
        """
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

            # # Paso 3: Verificar esquema de base de datos (opcional)
            # if not self._check_database_schema():
            #     self.logger.info(
            #         "Verificación de esquema cancelada por el usuario")
            #     return

            # Paso 4: Iniciar aplicación principal
            self._start_main_application()

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
        """
        Limpia los recursos de la aplicación.
        """
        try:
            if self.db_connection:
                # Cerrar conexión si existe
                self.logger.info("Cerrando conexión a base de datos")

            self.logger.info("Recursos limpiados exitosamente")

        except Exception as e:
            self.logger.error(f"Error limpiando recursos: {str(e)}")


def show_welcome_message():
    """
    Muestra mensaje de bienvenida.
    """
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
            f"A continuación se le solicitará configurar la conexión a la base de datos.\n"
            f"Una vez establecida la conexión, la aplicación se iniciará automáticamente."
        )

        messagebox.showinfo("Bienvenido", welcome_msg)
        root.destroy()

    except Exception as e:
        print(f"Error mostrando mensaje de bienvenida: {e}")


def main():
    """
    Función principal de la aplicación.
    """
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
