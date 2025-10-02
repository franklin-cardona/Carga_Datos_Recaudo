"""
Diálogo de Configuración de Conexión
Proporciona una interfaz para configurar la conexión a SQL Server.

Autor: FRANKLIN ANDRES CARDONA YARA
Fecha: 2025-01-08
"""

import tkinter as tk
from tkinter import ttk, messagebox
import threading
from typing import Optional, Dict, Any, Callable
import logging
import json
import os
from datetime import datetime


class ConnectionDialog:
    """
    Diálogo para configurar la conexión a SQL Server.
    """

    def __init__(self, parent=None, on_connection_success: Callable[[Dict[str, Any]], None] = None):
        """
        Inicializa el diálogo de conexión.

        Args:
            parent: Ventana padre (opcional)
            on_connection_success: Callback para conexión exitosa
        """
        self.parent = parent
        self.on_connection_success = on_connection_success
        self.result = None
        self.logger = logging.getLogger(__name__)

        # Crear ventana
        if parent:
            self.dialog = tk.Toplevel(parent)
            self.dialog.transient(parent)
            self.dialog.grab_set()
        else:
            self.dialog = tk.Tk()

        self.dialog.title("Configuración de Conexión a SQL Server")
        self.dialog.geometry("800x800")
        self.dialog.resizable(False, False)

        # Variables de interfaz
        self.server_var = tk.StringVar()
        self.database_var = tk.StringVar(value="HISTORICO")
        self.auth_type_var = tk.StringVar(value="windows")
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.save_config_var = tk.BooleanVar(value=True)
        self.environment_var = tk.StringVar(value="production")

        # Estado de la interfaz
        self.testing_connection = False

        # Configurar interfaz
        self._create_widgets()
        self._load_saved_config()
        self._center_dialog()

        # Configurar eventos
        self.dialog.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.dialog.bind('<Return>', lambda e: self._test_connection())
        self.dialog.bind('<Escape>', lambda e: self._on_cancel())

    def _create_widgets(self):
        """Crea todos los widgets del diálogo."""
        # Frame principal
        main_frame = ttk.Frame(self.dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Título
        title_label = ttk.Label(main_frame, text="Configuración de Conexión a SQL Server",
                                font=('Arial', 12, 'bold'))
        title_label.pack(pady=(0, 20))

        # Frame de configuración
        config_frame = ttk.LabelFrame(
            main_frame, text="Configuración de Conexión", padding="15")
        config_frame.pack(fill=tk.X, pady=(0, 15))

        # Entorno
        ttk.Label(config_frame, text="Entorno:").grid(
            row=0, column=0, sticky=tk.W, pady=(0, 5))
        env_frame = ttk.Frame(config_frame)
        env_frame.grid(row=1, column=0, columnspan=2,
                       sticky=(tk.W, tk.E), pady=(0, 15))

        ttk.Radiobutton(env_frame, text="Pruebas (P18PPAD29\\SQLEXPRESS)",
                        variable=self.environment_var, value="test",
                        command=self._on_environment_change).pack(anchor=tk.W)
        ttk.Radiobutton(env_frame, text="Producción (BDPBIA01)",
                        variable=self.environment_var, value="production",
                        command=self._on_environment_change).pack(anchor=tk.W)
        ttk.Radiobutton(env_frame, text="Personalizado",
                        variable=self.environment_var, value="custom",
                        command=self._on_environment_change).pack(anchor=tk.W)

        # Servidor
        ttk.Label(config_frame, text="Servidor:").grid(
            row=2, column=0, sticky=tk.W, pady=(0, 5))
        self.server_entry = ttk.Entry(
            config_frame, textvariable=self.server_var, width=40)
        self.server_entry.grid(row=3, column=0, columnspan=2,
                               sticky=(tk.W, tk.E), pady=(0, 15))

        # Base de datos
        ttk.Label(config_frame, text="Base de Datos:").grid(
            row=4, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(config_frame, textvariable=self.database_var, width=40).grid(
            row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 15))

        # Tipo de autenticación
        auth_frame = ttk.LabelFrame(
            config_frame, text="Autenticación", padding="10")
        auth_frame.grid(row=6, column=0, columnspan=2,
                        sticky=(tk.W, tk.E), pady=(0, 15))

        ttk.Radiobutton(auth_frame, text="Autenticación de Windows",
                        variable=self.auth_type_var, value="windows",
                        command=self._on_auth_type_change).pack(anchor=tk.W)
        ttk.Radiobutton(auth_frame, text="Autenticación de SQL Server",
                        variable=self.auth_type_var, value="sql",
                        command=self._on_auth_type_change).pack(anchor=tk.W, pady=(5, 0))

        # Credenciales SQL Server
        self.credentials_frame = ttk.Frame(auth_frame)
        self.credentials_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Label(self.credentials_frame, text="Usuario:").grid(
            row=0, column=0, sticky=tk.W, pady=(0, 5))
        self.username_entry = ttk.Entry(
            self.credentials_frame, textvariable=self.username_var, width=25)
        self.username_entry.grid(
            row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        ttk.Label(self.credentials_frame, text="Contraseña:").grid(
            row=2, column=0, sticky=tk.W, pady=(0, 5))
        self.password_entry = ttk.Entry(self.credentials_frame, textvariable=self.password_var,
                                        show="*", width=25)
        self.password_entry.grid(
            row=3, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        self.credentials_frame.columnconfigure(0, weight=1)

        # Configurar grid
        config_frame.columnconfigure(1, weight=1)

        # Opciones adicionales
        options_frame = ttk.Frame(main_frame)
        options_frame.pack(fill=tk.X, pady=(0, 15))

        ttk.Checkbutton(options_frame, text="Guardar configuración",
                        variable=self.save_config_var).pack(anchor=tk.W)

        # Frame de estado
        self.status_frame = ttk.Frame(main_frame)
        self.status_frame.pack(fill=tk.X, pady=(0, 15))

        self.status_label = ttk.Label(
            self.status_frame, text="", foreground="blue")
        self.status_label.pack(anchor=tk.W)

        self.progress_bar = ttk.Progressbar(
            self.status_frame, mode='indeterminate')
        self.progress_bar.pack(fill=tk.X, pady=(5, 0))
        self.progress_bar.pack_forget()  # Ocultar inicialmente

        # Botones
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        ttk.Button(button_frame, text="Cancelar",
                   command=self._on_cancel).pack(side=tk.RIGHT, padx=(10, 0))
        ttk.Button(button_frame, text="Conectar",
                   command=self._test_connection).pack(side=tk.RIGHT)
        self.test_button = ttk.Button(button_frame, text="Probar Conexión",
                                      command=self._test_connection_only)
        self.test_button.pack(side=tk.RIGHT, padx=(0, 10))

        # Configurar estado inicial
        self._on_environment_change()
        self._on_auth_type_change()

    def _center_dialog(self):
        """Centra el diálogo en la pantalla o sobre la ventana padre."""
        self.dialog.update_idletasks()

        width = self.dialog.winfo_width()
        height = self.dialog.winfo_height()

        if self.parent:
            # Centrar sobre la ventana padre
            parent_x = self.parent.winfo_rootx()
            parent_y = self.parent.winfo_rooty()
            parent_width = self.parent.winfo_width()
            parent_height = self.parent.winfo_height()

            x = parent_x + (parent_width // 2) - (width // 2)
            y = parent_y + (parent_height // 2) - (height // 2)
        else:
            # Centrar en la pantalla
            x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
            y = (self.dialog.winfo_screenheight() // 2) - (height // 2)

        self.dialog.geometry(f"{width}x{height}+{x}+{y}")

    def _on_environment_change(self):
        """Maneja el cambio de entorno."""
        env = self.environment_var.get()

        if env == "test":
            self.server_var.set("P18PPAD29\\SQLEXPRESS")
            self.server_entry.config(state='disabled')
        elif env == "production":
            self.server_var.set("BDPBIA01")
            self.server_entry.config(state='disabled')
        else:  # custom
            self.server_entry.config(state='normal')
            if self.server_var.get() in ["P18PPAD29\\SQLEXPRESS", "BDPBIA01"]:
                self.server_var.set("")

    def _on_auth_type_change(self):
        """Maneja el cambio de tipo de autenticación."""
        if self.auth_type_var.get() == "windows":
            # Deshabilitar campos de credenciales
            self.username_entry.config(state='disabled')
            self.password_entry.config(state='disabled')
        else:
            # Habilitar campos de credenciales
            self.username_entry.config(state='normal')
            self.password_entry.config(state='normal')

    def _load_saved_config(self):
        """Carga la configuración guardada si existe."""
        try:
            config_file = os.path.join(os.path.dirname(
                __file__), '..', '..', 'config', 'connection.json')

            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)

                self.server_var.set(config.get(
                    'server', 'P18PPAD29\\SQLEXPRESS'))
                self.database_var.set(config.get('database', 'HISTORICO'))
                self.auth_type_var.set(config.get('auth_type', 'windows'))
                self.username_var.set(config.get('username', ''))
                # No cargar contraseña por seguridad

                # Determinar entorno basado en servidor
                server = config.get('server', '')
                if server == "P18PPAD29\\SQLEXPRESS":
                    self.environment_var.set("test")
                elif server == "BDPBIA01":
                    self.environment_var.set("production")
                else:
                    self.environment_var.set("custom")

                self.logger.info("Configuración cargada desde archivo")

        except Exception as e:
            self.logger.warning(
                f"No se pudo cargar configuración guardada: {str(e)}")

    def _save_config(self):
        """Guarda la configuración actual."""
        if not self.save_config_var.get():
            return

        try:
            config_dir = os.path.join(
                os.path.dirname(__file__), '..', '..', 'config')
            os.makedirs(config_dir, exist_ok=True)

            config = {
                'server': self.server_var.get(),
                'database': self.database_var.get(),
                'auth_type': self.auth_type_var.get(),
                'username': self.username_var.get() if self.auth_type_var.get() == 'sql' else '',
                'saved_at': datetime.now().isoformat()
            }

            config_file = os.path.join(config_dir, 'connection.json')
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)

            self.logger.info("Configuración guardada exitosamente")

        except Exception as e:
            self.logger.error(f"Error guardando configuración: {str(e)}")

    def _validate_inputs(self) -> tuple[bool, str]:
        """
        Valida las entradas del usuario.

        Returns:
            Tupla (es_válido, mensaje_error)
        """
        server = self.server_var.get().strip()
        database = self.database_var.get().strip()

        if not server:
            return False, "El servidor es requerido"

        if not database:
            return False, "La base de datos es requerida"

        if self.auth_type_var.get() == "sql":
            username = self.username_var.get().strip()
            password = self.password_var.get()

            if not username:
                return False, "El usuario es requerido para autenticación SQL"

            if not password:
                return False, "La contraseña es requerida para autenticación SQL"

        return True, ""

    def _get_connection_config(self) -> Dict[str, Any]:
        """
        Obtiene la configuración de conexión actual.

        Returns:
            Diccionario con configuración de conexión
        """
        config = {
            'server': self.server_var.get().strip(),
            'database': self.database_var.get().strip(),
            'trusted_connection': self.auth_type_var.get() == 'windows',
            'driver': 'ODBC Driver 17 for SQL Server'
        }

        if self.auth_type_var.get() == 'sql':
            config['username'] = self.username_var.get().strip()
            config['password'] = self.password_var.get()

        return config

    def _set_status(self, message: str, color: str = "blue", show_progress: bool = False):
        """
        Establece el mensaje de estado.

        Args:
            message: Mensaje a mostrar
            color: Color del texto
            show_progress: Si mostrar la barra de progreso
        """
        self.status_label.config(text=message, foreground=color)

        if show_progress:
            self.progress_bar.pack(fill=tk.X, pady=(5, 0))
            self.progress_bar.start(10)
        else:
            self.progress_bar.stop()
            self.progress_bar.pack_forget()

    def _test_connection_only(self):
        """Prueba la conexión sin cerrar el diálogo."""
        self._perform_connection_test(connect_and_close=False)

    def _test_connection(self):
        """Prueba la conexión y cierra el diálogo si es exitosa."""
        self._perform_connection_test(connect_and_close=True)

    def _perform_connection_test(self, connect_and_close: bool = True):
        """
        Realiza la prueba de conexión.

        Args:
            connect_and_close: Si cerrar el diálogo después de conexión exitosa
        """

        if self.testing_connection:
            return

        # Validar entradas
        is_valid, error_msg = self._validate_inputs()
        if not is_valid:
            messagebox.showerror("Error de Validación", error_msg)
            return

        # Deshabilitar botones durante la prueba
        self.testing_connection = True
        self.test_button.config(state='disabled')

        # Mostrar estado de prueba
        self._set_status("Probando conexión...", "blue", True)

        # Ejecutar prueba en hilo separado
        config = self._get_connection_config()

        thread = threading.Thread(
            target=self._test_connection_thread,
            args=(config, connect_and_close),
            daemon=True
        )

        thread.start()

    def _test_connection_thread(self, config: Dict[str, Any], connect_and_close: bool):
        """
        Ejecuta la prueba de conexión en un hilo separado.

        Args:
            config: Configuración de conexión
            connect_and_close: Si cerrar el diálogo después de conexión exitosa
        """
        try:
            # Importar aquí para evitar dependencias circulares
            from connection import DatabaseConnection

            # Crear conexión de prueba
            db_connection = DatabaseConnection(
                server=config.get("server"), database=config.get('database'),
                trusted_connection=config.get("trusted_connection", False),
                driver=config.get('driver', 'ODBC Driver 17 for SQL Server'),
                username=config.get('username', None),
                password=config.get('password', None))

            # Probar conexión
            success, error_msg = db_connection.test_connection()

            # Actualizar UI en el hilo principal
            self.dialog.after(0, lambda: self._handle_connection_result(
                success, error_msg, config, connect_and_close
            ))

        except Exception as e:
            error_msg = f"Error de conexión: {str(e)}"
            self.dialog.after(0, lambda: self._handle_connection_result(
                False, error_msg, config, connect_and_close
            ))

    def _handle_connection_result(self, success: bool, error_msg: Optional[str],
                                  config: Dict[str, Any], connect_and_close: bool):
        """
        Maneja el resultado de la prueba de conexión.

        Args:
            success: Si la conexión fue exitosa
            error_msg: Mensaje de error (si aplica)
            config: Configuración de conexión
            connect_and_close: Si cerrar el diálogo después de conexión exitosa
        """
        # Rehabilitar botones
        self.testing_connection = False
        self.test_button.config(state='normal')

        if success:
            self._set_status("✓ Conexión exitosa", "green", False)

            if connect_and_close:
                # Guardar configuración
                self._save_config()

                # Establecer resultado
                self.result = config

                # Llamar callback si existe
                if self.on_connection_success:
                    self.on_connection_success(config)

                # Cerrar diálogo
                self.dialog.destroy()
            else:
                # Solo mostrar mensaje de éxito
                messagebox.showinfo("Conexión Exitosa",
                                    "La conexión a la base de datos fue establecida correctamente.")
        else:
            self._set_status("✗ Error de conexión", "red", False)
            messagebox.showerror("Error de Conexión",
                                 f"No se pudo conectar a la base de datos:\n\n{error_msg}")

    def _on_cancel(self):
        """Maneja la cancelación del diálogo."""
        self.result = None
        self.dialog.destroy()

    def show(self) -> Optional[Dict[str, Any]]:
        """
        Muestra el diálogo y retorna la configuración de conexión.

        Returns:
            Configuración de conexión o None si se canceló
        """
        if self.parent:
            self.dialog.wait_window()
        else:
            self.dialog.mainloop()

        return self.result


class ConnectionConfigManager:
    """
    Gestor de configuración de conexión.
    """

    def __init__(self):
        """Inicializa el gestor de configuración."""
        self.logger = logging.getLogger(__name__)
        self.config_dir = os.path.join(
            os.path.dirname(__file__), '..', '..', 'config')
        self.config_file = os.path.join(self.config_dir, 'connection.json')

    def get_saved_config(self) -> Optional[Dict[str, Any]]:
        """
        Obtiene la configuración guardada.

        Returns:
            Configuración guardada o None si no existe
        """
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.warning(f"Error cargando configuración: {str(e)}")

        return None

    def save_config(self, config: Dict[str, Any]):
        """
        Guarda la configuración.

        Args:
            config: Configuración a guardar
        """
        try:
            os.makedirs(self.config_dir, exist_ok=True)

            # No guardar contraseña por seguridad
            safe_config = config.copy()
            if 'password' in safe_config:
                del safe_config['password']

            safe_config['saved_at'] = datetime.now().isoformat()

            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(safe_config, f, indent=2, ensure_ascii=False)

            self.logger.info("Configuración guardada exitosamente")

        except Exception as e:
            self.logger.error(f"Error guardando configuración: {str(e)}")

    def clear_config(self):
        """Elimina la configuración guardada."""
        try:
            if os.path.exists(self.config_file):
                os.remove(self.config_file)
                self.logger.info("Configuración eliminada")
        except Exception as e:
            self.logger.error(f"Error eliminando configuración: {str(e)}")
